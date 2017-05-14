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

    //每次调度会产生一个job去运行
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            ExecutableState state = subTask.getStatus();
            if (state == ExecutableState.RUNNING) {
                // there is already running subtask, no need to start a new subtask 已经运行了,不需要重新开启,因此其实是一种错误,退出即可
                break;
            } else if (state == ExecutableState.ERROR) {
                throw new IllegalStateException("invalid subtask state, subtask:" + subTask.getName() + ", state:" + subTask.getStatus());
            }
            if (subTask.isRunnable()) {//对ready状态的任务进行执行
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
        } else if (result.succeed()) {//说明此时一个小任务完成了
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;//表示是否所有的任务都成功了
            boolean hasError = false;//表示是否有任务失败
            boolean hasRunning = false;//表示是否有任务还在进行中
            for (Executable task : jobs) {
                final ExecutableState status = task.getStatus();//循环所有的子任务
                if (status == ExecutableState.ERROR) {//说明任务有异常的子任务
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {//说明不是所有的都是成功的
                    allSucceed = false;
                }
                if (status == ExecutableState.RUNNING) {//说明还有正在运行中的
                    hasRunning = true;
                }
            }
            if (allSucceed) {//说明全部完成了
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.SUCCEED, null, null);//因此本任务,即父任务设置成成功完成
                notifyUserStatusChange(executableContext, ExecutableState.SUCCEED);
            } else if (hasError) {//说明已经有异常了,则最终设置为该父任务为error
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
                notifyUserStatusChange(executableContext, ExecutableState.ERROR);
            } else if (hasRunning) {//说明此时该任务还有子任务在运行中,因此父任务的状态也是运行中
                //注意:目前没想明白说明情况下,子任务还在运行中,父任务却已经通知完成了,按道理不应该出现这种情况
                jobService.updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
            } else {//说明还有任务在ready中,因此继续把该任务设置为ready,下一次系统调度的时候就会继续调度该任务,依次启动新的任务
                jobService.updateJobOutput(getId(), ExecutableState.READY, null, null);
            }
        } else {//说明任务已经失败了,则直接返回error
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
