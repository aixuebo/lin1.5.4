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

package org.apache.kylin.cube.cli;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理cube的一个segment中不重复的字段对应的值
 * 主要处理字典信息以及对lookup表设置快照
 */
public class DictionaryGeneratorCLI {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGeneratorCLI.class);

    /**
     * @param cubeName 要处理哪一个cubue
     * @param segmentID 要处理哪一个segment
     * @param factTableValueProvider 如何读取某一个列中的数据内容,该内容存储在输出的目录中,存储的内容是该字段所有不重复的数据内容集合
     */
    public static void processSegment(KylinConfig config, String cubeName, String segmentID, DistinctColumnValuesProvider factTableValueProvider) throws IOException {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);

        processSegment(config, segment, factTableValueProvider);//对cube的segment去创建字典
    }

    /**
     *
     * @param cubeSeg 要处理哪一个segment
     * @param factTableValueProvider 如何读取某一个列中的数据内容,该内容存储在输出的目录中,存储的内容是该字段所有不重复的数据内容集合
     * @throws IOException
     */
    private static void processSegment(KylinConfig config, CubeSegment cubeSeg, DistinctColumnValuesProvider factTableValueProvider) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(config);

        // dictionary 处理rowkey中需要的字典
        for (TblColRef col : cubeSeg.getCubeDesc().getAllColumnsNeedDictionaryBuilt()) {//获取需要构建字典的列的集合,并且循环
            logger.info("Building dictionary for " + col);
            cubeMgr.buildDictionary(cubeSeg, col, factTableValueProvider);//构建该列的字典
        }

        //处理度量中需要的维度表
        for (DimensionDesc dim : cubeSeg.getCubeDesc().getDimensions()) {
            // build snapshot
            if (dim.getTable() != null && !dim.getTable().equalsIgnoreCase(cubeSeg.getCubeDesc().getFactTable())) {//不是fact表即可
                // CubeSegment seg = cube.getTheOnlySegment();
                logger.info("Building snapshot of " + dim.getTable());
                cubeMgr.buildSnapshotTable(cubeSeg, dim.getTable());//保存lookup表的快照
                logger.info("Checking snapshot of " + dim.getTable());
                cubeMgr.getLookupTable(cubeSeg, dim); // load the table for sanity check
            }
        }
    }

}
