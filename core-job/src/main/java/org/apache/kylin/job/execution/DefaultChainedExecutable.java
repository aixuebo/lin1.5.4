/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.job.execution;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.manager.ExecutableManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 表示可以执行一组任务
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    //存储job集合
    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    //管理job的对象
    protected final ExecutableManager jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());

    public DefaultChainedExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            ExecutableState state = subTask.getStatus();
            if (state == ExecutableState.RUNNING) {
                // there is already running subtask, no need to start a new subtask
                break;
            } else if (state == ExecutableState.ERROR) {
                throw new IllegalStateException("invalid subtask state, subtask:" + subTask.getName() + ", state:" + subTask.getStatus());
            }
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED, null);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        Map<String, String> info = Maps.newHashMap();
        info.put(START_TIME, Long.toString(System.currentTimeMillis()));
        final long startTime = getStartTime();
        if (startTime > 0) {
            jobService.updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
        } else {
            jobService.updateJobOutput(getId(), ExecutableState.RUNNING, info, null);
        }
    }

    @Override
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        super.onExecuteError(exception, executableContext);
        notifyUserStatusChange(executableContext, ExecutableState.ERROR);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        if (isDiscarded()) {
            setEndTime(System.currentTimeMillis());
            notifyUserStatusChange(executableContext, ExecutableState.DISCARDED);
        } else if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;//表示是否所有的任务都成功了
            boolean hasError = false;//表示是否有任务失败
            boolean hasRunning = false;//表示是否有任务还在进行中
            for (Executable task : jobs) {
                final ExecutableState status = task.getStatus();
                if (status == ExecutableState.ERROR) {
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
                if (status == ExecutableState.RUNNING) {
                    hasRunning = true;
                }
            }
            if (allSucceed) {
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.SUCCEED, null, null);
                notifyUserStatusChange(executableContext, ExecutableState.SUCCEED);
            } else if (hasError) {
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
                notifyUserStatusChange(executableContext, ExecutableState.ERROR);
            } else if (hasRunning) {
                jobService.updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
            } else {
                jobService.updateJobOutput(getId(), ExecutableState.READY, null, null);
            }
        } else {
            setEndTime(System.currentTimeMillis());
            jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, result.output());
            notifyUserStatusChange(executableContext, ExecutableState.ERROR);
        }
    }

    //返回全部的job
    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    @Override
    protected boolean needRetry() {
        return false;
    }

    //用name找到一个job
    public final AbstractExecutable getTaskByName(String name) {
        for (AbstractExecutable task : subTasks) {
            if (task.getName() != null && task.getName().equalsIgnoreCase(name)) {
                return task;
            }
        }
        return null;
    }

    //添加一个job
    @Override
    public void addTask(AbstractExecutable executable) {
        executable.setId(getId() + "-" + String.format("%02d", subTasks.size()));//用ID表示第几个job
        this.subTasks.add(executable);
    }
}
