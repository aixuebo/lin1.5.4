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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.IBatchCubingEngine;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

public class MRBatchCubingEngine implements IBatchCubingEngine {

    /**
     * 创建一个临时hive表,存储cube需要的字段,即在原有的表的基础上进行了一部分字段过滤
     * 剩余的字段只是包含rowkey需要的列+扩展列+字典列+度量需要的列
     */
    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc) {
        return new CubeJoinedFlatTableDesc(cubeDesc);
    }

    /**
     * 创建一个临时hive表,存储cube需要的字段,即在原有的表的基础上进行了一部分字段过滤
     * 剩余的字段只是包含rowkey需要的列+扩展列+字典列+度量需要的列
     */
    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment) {
        return new CubeJoinedFlatTableDesc(newSegment);
    }

    /**
     * 进行cube的build工厂类
     */
    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return new BatchCubingJobBuilder(newSegment, submitter).build();
    }

    /**
     * 进行cube的merge工厂类
     */
    @Override
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return new BatchMergeJobBuilder(mergeSegment, submitter).build();
    }

    @Override
    public Class<?> getSourceInterface() {
        return IMRInput.class;
    }

    @Override
    public Class<?> getStorageInterface() {
        return IMROutput.class;
    }

}
