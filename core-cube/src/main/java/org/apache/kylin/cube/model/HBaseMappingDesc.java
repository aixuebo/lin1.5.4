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

package org.apache.kylin.cube.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 用于存储度量属性
 "hbase_mapping": {
     "column_family": [
         {
             "name": "F1",//表示标准的度量
             "columns": [
                 {
                     "qualifier": "M",
                     "measure_refs": [
                     "_COUNT_",
                     "AMOUNT",
                     "AA",
                     "BB",
                     "EE"
                     ]
                 }
             ]
         },
         {
             "name": "F2",//表示count distinct度量
             "columns": [
                 {
                     "qualifier": "M",
                     "measure_refs": [
                     "CC",
                     "GG"
                     ]
                 }
             ]
         }
     ]
 },
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class HBaseMappingDesc {

    @JsonProperty("column_family")
    private HBaseColumnFamilyDesc[] columnFamily;

    // point to the cube instance which contain this HBaseMappingDesc instance.
    private CubeDesc cubeRef;//所属cube

    //返回定义该参数函数对应的列族
    public Collection<HBaseColumnDesc> findHBaseColumnByFunction(FunctionDesc function) {
        Collection<HBaseColumnDesc> result = new LinkedList<HBaseColumnDesc>();
        HBaseMappingDesc hbaseMapping = cubeRef.getHbaseMapping();
        if (hbaseMapping == null || hbaseMapping.getColumnFamily() == null) {
            return result;
        }
        for (HBaseColumnFamilyDesc cf : hbaseMapping.getColumnFamily()) {//循环每一个列族
            for (HBaseColumnDesc c : cf.getColumns()) {//该列族下所有列
                for (MeasureDesc m : c.getMeasures()) {//每一个列对应的度量
                    if (m.getFunction().equals(function)) {//找到对应的度量
                        result.add(c);
                    }
                }
            }
        }
        return result;
    }

    public CubeDesc getCubeRef() {
        return cubeRef;
    }

    public void setCubeRef(CubeDesc cubeRef) {
        this.cubeRef = cubeRef;
    }

    public HBaseColumnFamilyDesc[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(HBaseColumnFamilyDesc[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public void init(CubeDesc cubeDesc) {
        cubeRef = cubeDesc;

        for (HBaseColumnFamilyDesc cf : columnFamily) {//循环所有的列族
            cf.setName(cf.getName().toUpperCase());//设置列族名字大写

            for (HBaseColumnDesc c : cf.getColumns()) {//循环每一个列
                c.setQualifier(c.getQualifier().toUpperCase());//列名字大写
                StringUtil.toUpperCaseArray(c.getMeasureRefs(), c.getMeasureRefs());//列里面保存的度量名字大写
            }
        }
    }

    @Override
    public String toString() {
        return "HBaseMappingDesc [columnFamily=" + Arrays.toString(columnFamily) + "]";
    }

}
