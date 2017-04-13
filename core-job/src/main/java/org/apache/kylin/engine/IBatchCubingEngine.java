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

package org.apache.kylin.engine;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

/**
 实现类
 r.put(0, "org.apache.kylin.engine.mr.MRBatchCubingEngine");
 r.put(2, "org.apache.kylin.engine.mr.MRBatchCubingEngine2");
批处理引擎接口
 */
public interface IBatchCubingEngine {
    
    /** Mark deprecated to indicate for test purpose only
     * 为一个cube的segment产生一个hive的临时中间表,用于存储该build需要的数据内容
     **/
    @Deprecated
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc);//cube运行的时候需要哪些列
    
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment);//cube运行的时候需要哪些列

    /** Build a new cube segment, typically its time range appends to the end of current cube.
     * 进行cube的build工厂类
     **/
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter);

    /** Merge multiple small segments into a big one.
     * 进行cube的merge工厂类
     **/
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter);

    public Class<?> getSourceInterface();

    public Class<?> getStorageInterface();
}
