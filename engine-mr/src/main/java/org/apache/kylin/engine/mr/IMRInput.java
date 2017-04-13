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

import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * Any ITableSource that wishes to serve as input of MapReduce build engine must adapt to this interface.
 * 参见org.apache.kylin.source.hive.HiveMRInput实现类
 */
public interface IMRInput {

    /** Return a helper to participate in batch cubing job flow. */
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc);

    /** Return an InputFormat that reads from specified table.
     * 如何读取hive表,返回读取hive表的一个InputFormat
     **/
    public IMRTableInputFormat getTableInputFormat(TableDesc table);

    /**
     * Utility that configures mapper to read from a table.
     * 如何读取hive的table
     */
    public interface IMRTableInputFormat {

        /** Configure the InputFormat of given job.
         * 配置hive的信息,让其可以读取hive的input以及inputFormat
         **/
        public void configureJob(Job job);

        /** Parse a mapper input object into column values.
         * hive的每一行数据 返回列组成的字符串数组
         **/
        public String[] parseMapperInput(Object mapperInput);
    }

    /**
     * Participate the batch cubing flow as the input side. Responsible for creating
     * intermediate flat table (Phase 1) and clean up any leftover (Phase 4).
     * 用于对一个cube的segment创建一个临时的hive表,存储该cube需要的列集合,日后处理只需要处理该临时表即可
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary (with FlatTableInputFormat)
     * - Phase 3: Build Cube (with FlatTableInputFormat)
     * - Phase 4: Update Metadata & Cleanup
     */
    public interface IMRBatchCubingInputSide {

        /** Return an InputFormat that reads from the intermediate flat table */
        public IMRTableInputFormat getFlatTableInputFormat();//如何读取hive表中临时数据表中数据

        /** Add step that creates an intermediate flat table as defined by CubeJoinedFlatTableDesc
         * 根据cube的segment内容,将cube的内容创建到一个临时表中,并且按照一个字段进行分片处理,让每一个都很均匀分布
         **/
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow);

        /** Add step that does necessary clean up, like delete the intermediate flat table
         * 最终处理数据,让临时表清空
         **/
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);
    }

}
