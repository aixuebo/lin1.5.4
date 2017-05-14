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

package org.apache.kylin.job.impl.threadpool;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableContext;

/**
 */
public class DefaultContext implements ExecutableContext {

    //表示真正运行中的job集合,key是Executable的id,value是Executable对象
    private final ConcurrentMap<String, Executable> runningJobs;
    private final KylinConfig kylinConfig;

    public DefaultContext(ConcurrentMap<String, Executable> runningJobs, KylinConfig kylinConfig) {
        this.runningJobs = runningJobs;
        this.kylinConfig = kylinConfig;
    }

    @Override
    public Object getSchedulerContext() {
        return null;
    }

    @Override
    public KylinConfig getConfig() {
        return kylinConfig;
    }

    //添加一个job
    void addRunningJob(Executable executable) {
        runningJobs.put(executable.getId(), executable);
    }

    //移除一个job
    void removeRunningJob(Executable executable) {
        runningJobs.remove(executable.getId());
    }

    //返回运行中的job集合,此时是copy的过程
    public Map<String, Executable> getRunningJobs() {
        return Collections.unmodifiableMap(runningJobs);
    }
}
