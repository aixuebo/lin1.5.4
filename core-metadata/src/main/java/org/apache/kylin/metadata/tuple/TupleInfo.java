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

package org.apache.kylin.metadata.tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * 
 * @author xjiang
 * 查询结果--表示一行记录---属于一行数据属性的schema映射
 */
public class TupleInfo {

    private final Map<String, Integer> fieldMap;//属性与下标映射
    private final Map<TblColRef, Integer> columnMap;//属性对象与下标映射

    private final List<String> fields;//该行记录对应的属性集合
    private final List<TblColRef> columns;//数据对象集合

    private final List<String> dataTypeNames;//属性的返回值

    public TupleInfo() {
        fieldMap = new HashMap<String, Integer>();
        columnMap = new HashMap<TblColRef, Integer>();
        fields = new ArrayList<String>();
        columns = new ArrayList<TblColRef>();
        dataTypeNames = new ArrayList<String>();
    }

    public TblColRef getColumn(String fieldName) {
        int idx = getFieldIndex(fieldName);
        return columns.get(idx);
    }

    public int getColumnIndex(TblColRef col) {
        return columnMap.get(col);
    }

    public String getDataTypeName(int index) {
        return dataTypeNames.get(index);
    }

    public int getFieldIndex(String fieldName) {
        return fieldMap.get(fieldName);
    }

    //是否有该属性
    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }

    public String getFieldName(TblColRef col) {
        int idx = columnMap.get(col);
        return fields.get(idx);
    }

    public boolean hasColumn(TblColRef col) {
        return columnMap.containsKey(col);
    }

    //设置属性name、属性对象、下标
    public void setField(String fieldName, TblColRef col, int index) {
        fieldMap.put(fieldName, index);

        if (col != null)
            columnMap.put(col, index);

        if (fields.size() > index)
            fields.set(index, fieldName);
        else
            fields.add(index, fieldName);

        if (columns.size() > index)
            columns.set(index, col);
        else
            columns.add(index, col);

        if (dataTypeNames.size() > index)
            dataTypeNames.set(index, col.getType().getName());
        else
            dataTypeNames.add(index, col.getType().getName());
    }

    public List<String> getAllFields() {
        return fields;
    }

    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public int size() {
        return fields.size();
    }

}
