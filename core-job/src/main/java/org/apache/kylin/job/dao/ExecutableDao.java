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

package org.apache.kylin.job.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 用于将任务的执行job内容和输出内容存储到磁盘上
 */
public class ExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<ExecutablePO>(ExecutablePO.class);
    private static final Serializer<ExecutableOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<ExecutableOutputPO>(ExecutableOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(ExecutableDao.class);
    private static final ConcurrentHashMap<KylinConfig, ExecutableDao> CACHE = new ConcurrentHashMap<KylinConfig, ExecutableDao>();

    private ResourceStore store;

    public static ExecutableDao getInstance(KylinConfig config) {
        ExecutableDao r = CACHE.get(config);
        if (r == null) {
            synchronized (ExecutableDao.class) {
                r = CACHE.get(config);
                if (r == null) {
                    r = new ExecutableDao(config);
                    CACHE.put(config, r);
                    if (CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return r;
    }

    private ExecutableDao(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.store = MetadataManager.getInstance(config).getStore();
    }

    //获取该任务uuid对应的信息存储路径
    private String pathOfJob(ExecutablePO job) {
        return pathOfJob(job.getUuid());
    }
    public static String pathOfJob(String uuid) {
        return ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid; //execute
    }

    ///存放每一个job任务的执行的输出信息
    public static String pathOfJobOutput(String uuid) {
        return ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;//execute_output
    }

    //将path对应的内容,按照json的解析方式,反序列化成ExecutablePO对象
    private ExecutablePO readJobResource(String path) throws IOException {
        return store.getResource(path, ExecutablePO.class, JOB_SERIALIZER);
    }

    //将job对象使用json方式序列化到path路径下
    private void writeJobResource(String path, ExecutablePO job) throws IOException {
        store.putResource(path, job, JOB_SERIALIZER);
    }

    //读取job的输出对象内容
    private ExecutableOutputPO readJobOutputResource(String path) throws IOException {
        return store.getResource(path, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
    }

    //写入job的输出内容
    private long writeJobOutputResource(String path, ExecutableOutputPO output) throws IOException {
        return store.putResource(path, output, JOB_OUTPUT_SERIALIZER);
    }

    //获取所有的job输出集合
    public List<ExecutableOutputPO> getJobOutputs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //获取所有的job输出集合-------有过滤条件
    public List<ExecutableOutputPO> getJobOutputs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, timeStart, timeEndExclusive, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //获取所有任务
    public List<ExecutablePO> getJobs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, ExecutablePO.class, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //获取所有任务---有过滤条件
    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, timeStart, timeEndExclusive, ExecutablePO.class, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //获取所有job的uuid集合
    public List<String> getJobIds() throws PersistentException {
        try {
            NavigableSet<String> resources = store.listResources(ResourceStore.EXECUTE_RESOURCE_ROOT);
            if (resources == null) {
                return Collections.emptyList();
            }
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(resources.size());
            for (String path : resources) {
                result.add(path.substring(path.lastIndexOf("/") + 1));
            }
            return result;
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //获取某一个任务对象
    public ExecutablePO getJob(String uuid) throws PersistentException {
        try {
            return readJobResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error get job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    //添加一个任务
    public ExecutablePO addJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) != null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
            }
            writeJobResource(pathOfJob(job), job);
            return job;
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    //删除一个任务内容
    public void deleteJob(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    //删除一个任务输出
    public void deleteJobOutput(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJobOutput(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    //获取一个输出对象
    public ExecutableOutputPO getJobOutput(String uuid) throws PersistentException {
        try {
            ExecutableOutputPO result = readJobOutputResource(pathOfJobOutput(uuid));
            if (result == null) {
                result = new ExecutableOutputPO();
                result.setUuid(uuid);//设置输出id不再是以前的输出id,要设置为任务job的id,表示该jobid对应的输出
                return result;
            }
            return result;
        } catch (IOException e) {
            logger.error("error get job output id:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    //每一个任务有一个输出
    public void addJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            output.setLastModified(0);
            writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    //重新输出内容
    public void updateJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            final long ts = writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
            output.setLastModified(ts);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

}
