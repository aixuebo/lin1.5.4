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

package org.apache.kylin.job;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.lock.JobLock;

/**
 * 任务的调度器
 * 实现类org.apache.kylin.job.impl.threadpool.DefaultScheduler
 */
public interface Scheduler<T extends Executable> {

    //初始化该调度
    void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException;

    //关闭该调度
    void shutdown() throws SchedulerException;

    //调度上停止一个任务
    boolean stop(T executable) throws SchedulerException;

    //任务是否开启成功
    boolean hasStarted();

}
