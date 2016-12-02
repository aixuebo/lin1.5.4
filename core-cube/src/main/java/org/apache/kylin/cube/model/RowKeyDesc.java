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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class RowKeyDesc {

    @JsonProperty("rowkey_columns")
    private RowKeyColDesc[] rowkeyColumns;//存储cube中rowkey的列集合

    // computed content
    private long fullMask;//如果有5列,则最终结果是11111
    private CubeDesc cubeDesc;//属于哪个cube

    //每一个rowkey的列对象 与 rowkeyColumn的映射
    private Map<TblColRef, RowKeyColDesc> columnMap;

    private Set<TblColRef> shardByColumns;//分片的列
    private int[] columnsNeedIndex;//存储需要索引的字段序号

    public RowKeyColDesc[] getRowKeyColumns() {
        return rowkeyColumns;
    }

    public void setCubeDesc(CubeDesc cubeRef) {
        this.cubeDesc = cubeRef;
    }

    public int getColumnBitIndex(TblColRef col) {
        return getColDesc(col).getBitIndex();
    }

    public RowKeyColDesc getColDesc(TblColRef col) {
        RowKeyColDesc desc = columnMap.get(col);
        if (desc == null)
            throw new NullPointerException("Column " + col + " does not exist in row key desc");
        return desc;
    }

    //判断该列是否是dict
    public boolean isUseDictionary(TblColRef col) {
        return getColDesc(col).isUsingDictionary();
    }

    public Set<TblColRef> getShardByColumns() {
        return shardByColumns;
    }

    public void init(CubeDesc cubeDesc) {

        setCubeDesc(cubeDesc);

        //刨除derived的列,以及额外的列,返回剩余列的集合
        Map<String, TblColRef> colNameAbbr = cubeDesc.buildColumnNameAbbreviation();

        buildRowKey(colNameAbbr);

        int[] tmp = new int[100];//存储需要index索引的字段序号
        int x = 0;//有多少个满足条件的字段
        for (int i = 0, n = rowkeyColumns.length; i < n; i++) {
            if ("true".equalsIgnoreCase(rowkeyColumns[i].getIndex()) && DictionaryDimEnc.ENCODING_NAME.equalsIgnoreCase(rowkeyColumns[i].getEncoding())) {//dict
                tmp[x] = i;
                x++;
            }
        }

        columnsNeedIndex = ArrayUtils.subarray(tmp, 0, x);
    }

    public void setRowkeyColumns(RowKeyColDesc[] rowkeyColumns) {
        this.rowkeyColumns = rowkeyColumns;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("RowKeyColumns", Arrays.toString(rowkeyColumns)).toString();
    }

    private void buildRowKey(Map<String, TblColRef> colNameAbbr) {
        columnMap = new HashMap<TblColRef, RowKeyColDesc>();
        shardByColumns = new HashSet<>();

        for (int i = 0; i < rowkeyColumns.length; i++) {
            RowKeyColDesc rowKeyColDesc = rowkeyColumns[i];
            rowKeyColDesc.init(rowkeyColumns.length - i - 1, colNameAbbr, cubeDesc);//初始化rowkey的某一列
            columnMap.put(rowKeyColDesc.getColRef(), rowKeyColDesc);

            if (rowKeyColDesc.isShardBy()) {
                shardByColumns.add(rowKeyColDesc.getColRef());
            }
        }

        this.fullMask = 0L;
        for (int i = 0; i < this.rowkeyColumns.length; i++) {
            int index = rowkeyColumns[i].getBitIndex();
            this.fullMask |= 1L << index;
        }
    }

    public long getFullMask() {
        return this.fullMask;
    }

    public int[] getColumnsNeedIndex() {
        return columnsNeedIndex;

    }

}
