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

package org.apache.kylin.metadata.model;

import java.util.List;

import org.apache.kylin.common.util.StringUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 描述factTable和LookupDesc中的哪些列用于做维度,进行group by操作
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ModelDimensionDesc {
    @JsonProperty("table")
    private String table;//表名
    @JsonProperty("columns")
    private String[] columns;//使用该表要做维度的列集合

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    //静态方法,去大写字母
    //即标准化的过程
    public static void capicalizeStrings(List<ModelDimensionDesc> dimensions) {
        if (dimensions != null) {
            for (ModelDimensionDesc modelDimensionDesc : dimensions) {//循环每一个表选择的维度指标对象
                modelDimensionDesc.setTable(modelDimensionDesc.getTable().toUpperCase());//table的name进行大写字母处理
                if (modelDimensionDesc.getColumns() != null) {
                    StringUtil.toUpperCaseArray(modelDimensionDesc.getColumns(), modelDimensionDesc.getColumns());//列名字进行大写字母处理
                }
            }
        }
    }

    //静态方法,计算总共多少个维度
    public static int getColumnCount(List<ModelDimensionDesc> modelDimensionDescs) {
        int count = 0;
        for (ModelDimensionDesc modelDimensionDesc : modelDimensionDescs) {//循环每一个表选择的维度指标对象
            if (modelDimensionDesc.getColumns() != null) {
                count += modelDimensionDesc.getColumns().length;//计算该表有多少列
            }
        }
        return count;
    }

}
