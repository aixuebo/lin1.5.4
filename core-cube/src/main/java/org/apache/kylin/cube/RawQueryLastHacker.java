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

package org.apache.kylin.cube;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hack黑客
 */
public class RawQueryLastHacker {

    private static final Logger logger = LoggerFactory.getLogger(RawQueryLastHacker.class);

    public static void hackNoAggregations(SQLDigest sqlDigest, CubeDesc cubeDesc) {
        if (!sqlDigest.isRawQuery) {//false 说明有聚合函数或者group by,因此不需要初始化
            return;
        }

        //代码执行到这,说明没有group by 和聚合函数出现.仅仅是简单的select from where查询
        // If no group by and metric found, then it's simple query like select ... from ... where ...,
        // But we have no raw data stored, in order to return better results, we hack to output sum of metric column
        logger.info("No group by and aggregation found in this query, will hack some result for better look of output...");

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.
        boolean isSelectAll = sqlDigest.allColumns.isEmpty() || sqlDigest.allColumns.equals(sqlDigest.filterColumns);

        for (TblColRef col : cubeDesc.listAllColumns()) {//循环  该cube支持的所有列,包括derived列
            if (cubeDesc.listDimensionColumnsIncludingDerived().contains(col) || isSelectAll) {
                if (col.getTable().equals(sqlDigest.factTable))
                    sqlDigest.allColumns.add(col);
            }
        }

        for (TblColRef col : sqlDigest.allColumns) {
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col)) {
                // For dimension columns, take them as group by columns.
                sqlDigest.groupbyColumns.add(col);
            } else {
                // For measure columns, take them as metric columns with aggregation function SUM().
                //对列进行sum函数处理
                ParameterDesc colParameter = new ParameterDesc();
                colParameter.setType("column");
                colParameter.setValue(col.getName());
                FunctionDesc sumFunc = new FunctionDesc();
                sumFunc.setExpression("SUM");
                sumFunc.setParameter(colParameter);

                boolean measureHasSum = false;//true表示找到了该属性对应的sum度量方式
                for (MeasureDesc colMeasureDesc : cubeDesc.getMeasures()) {//循环每一个度量
                    if (colMeasureDesc.getFunction().equals(sumFunc)) {//找到sum(column)的度量
                        measureHasSum = true;
                        break;
                    }
                }
                if (measureHasSum) {
                    sqlDigest.aggregations.add(sumFunc);//添加该度量
                } else {
                    logger.warn("SUM is not defined for measure column " + col + ", output will be meaningless.");
                }

                sqlDigest.metricColumns.add(col);
            }
        }
    }
}