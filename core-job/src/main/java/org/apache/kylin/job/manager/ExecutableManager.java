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

package org.apache.kylin.job.manager;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);
    private static final ConcurrentHashMap<KylinConfig, ExecutableManager> CACHE = new ConcurrentHashMap<KylinConfig, ExecutableManager>();
    @SuppressWarnings("unused")
    private final KylinConfig config;

    private ExecutableDao executableDao;

    public static ExecutableManager getInstance(KylinConfig config) {
        ExecutableManager r = CACHE.get(config);
        if (r == null) {
            synchronized (ExecutableManager.class) {
                r = CACHE.get(config);
                if (r == null) {
                    r = new ExecutableManager(config);
                    CACHE.put(config, r);
                    if (CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return r;
    }

    private ExecutableManager(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.config = config;
        this.executableDao = ExecutableDao.getInstance(config);
    }

    //添加一个任务
    public void addJob(AbstractExecutable executable) {
        try {
            executableDao.addJob(parse(executable));
            addJobOutput(executable);
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    //为一个任务添加输出
    private void addJobOutput(AbstractExecutable executable) throws PersistentException {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setUuid(executable.getId());
        executableDao.addJobOutput(executableOutputPO);
        if (executable instanceof DefaultChainedExecutable) {//可能由于多个任务组成,因此要为每一个子任务添加输出
            for (AbstractExecutable subTask : ((DefaultChainedExecutable) executable).getTasks()) {
                addJobOutput(subTask);
            }
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        try {
            executableDao.deleteJob(jobId);
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + jobId, e);
            throw new RuntimeException(e);
        }
    }

    //获取一个任务对象
    public AbstractExecutable getJob(String uuid) {
        try {
            return parseTo(executableDao.getJob(uuid));
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public Output getOutput(String uuid) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(uuid);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + uuid);
            return parseOutput(jobOutput);
        } catch (PersistentException e) {
            logger.error("fail to get job output:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        return result;
    }

    public Map<String, Output> getAllOutputs() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            HashMap<String, Output> result = Maps.newHashMap();
            for (ExecutableOutputPO jobOutput : jobOutputs) {
                result.put(jobOutput.getId(), parseOutput(jobOutput));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Output> getAllOutputs(long timeStartInMillis, long timeEndInMillis) {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(timeStartInMillis, timeEndInMillis);
            HashMap<String, Output> result = Maps.newHashMap();
            for (ExecutableOutputPO jobOutput : jobOutputs) {
                result.put(jobOutput.getId(), parseOutput(jobOutput));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables() {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs()) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    //获取所有任务的uuid集合
    public List<String> getAllJobIds() {
        try {
            return executableDao.getJobIds();
        } catch (PersistentException e) {
            logger.error("error get All Job Ids", e);
            throw new RuntimeException(e);
        }
    }

    //将所有running状态的任务设置为error状态
    public void updateAllRunningJobsToError() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                    executableOutputPO.setStatus(ExecutableState.ERROR.toString());
                    executableDao.updateJobOutput(executableOutputPO);
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to ERROR", e);
            throw new RuntimeException(e);
        }
    }

    //将所有runing状态的任务设置成READY状态
    public void resumeAllRunningJobs() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                    executableOutputPO.setStatus(ExecutableState.READY.toString());
                    executableDao.updateJobOutput(executableOutputPO);
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to READY", e);
            throw new RuntimeException(e);
        }
    }

    //为job设置成READY状态
    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.ERROR) {
                    updateJobOutput(task.getId(), ExecutableState.READY, null, null);
                    break;
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.READY, null, null);
    }

    //为job设置成DISCARDED丢弃状态
    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (!task.getStatus().isFinalState()) {
                    updateJobOutput(task.getId(), ExecutableState.DISCARDED, null, null);
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.DISCARDED, null, null);
    }

    //更新一个任务的所有信息
    public void updateJobOutput(String jobId, ExecutableState newStatus, Map<String, String> info, String output) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(jobId);//获取该任务
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + jobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());//以前的状态
            if (newStatus != null && oldStatus != newStatus) {//如果状态不同,要进行校验事件流是否正常流转
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    throw new IllegalStateTranferException("there is no valid state transfer from:" + oldStatus + " to:" + newStatus);
                }
                jobOutput.setStatus(newStatus.toString());//设置新的状态
            }
            if (info != null) {//设置新的额外信息
                jobOutput.setInfo(info);
            }
            if (output != null) {//设置新的输出
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput);//重新存储
            logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
        } catch (PersistentException e) {
            logger.error("error change job:" + jobId + " to " + newStatus.toString());
            throw new RuntimeException(e);
        }
    }

    //for migration only
    //TODO delete when migration finished
    //重新为该job设置任务和输出内容
    public void resetJobOutput(String jobId, ExecutableState state, String output) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(jobId);
            jobOutput.setStatus(state.toString());
            if (output != null) {
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput);
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 为一个任务添加额外信息
     * @param id 任务的uuid
     * @param info 要追加的信息
     */
    public void addJobInfo(String id, Map<String, String> info) {
        if (info == null) {
            return;
        }
        try {
            //获取该任务的输出内容
            ExecutableOutputPO output = executableDao.getJobOutput(id);
            Preconditions.checkArgument(output != null, "there is no related output for job id:" + id);
            output.getInfo().putAll(info);//追加信息
            executableDao.updateJobOutput(output);//重新将内容写入到磁盘
        } catch (PersistentException e) {
            logger.error("error update job info, id:" + id + "  info:" + info.toString());
            throw new RuntimeException(e);
        }
    }

    /**
     * 为任务uuid 追加key-value信息
     * @param id 任务的uuid
     * @param key 要追加的key
     * @param value 要追加的value
     */
    public void addJobInfo(String id, String key, String value) {
        Map<String, String> info = Maps.newHashMap();
        info.put(key, value);
        addJobInfo(id, info);
    }

    //解析一个任务,转换成简单的对象
    private static ExecutablePO parse(AbstractExecutable executable) {
        ExecutablePO result = new ExecutablePO();
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        if (executable instanceof ChainedExecutable) {
            List<ExecutablePO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((ChainedExecutable) executable).getTasks()) {
                tasks.add(parse(task));
            }
            result.setTasks(tasks);
        }
        return result;
    }

    //是上一个方法parse的逆方法
    private static AbstractExecutable parseTo(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();//获取执行器的class
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);//反射构建该对象
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            result.setParams(executablePO.getParams());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    ((ChainedExecutable) result).addTask(parseTo(subTask));
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot parse this job:" + executablePO.getId(), e);
        }
    }

}
