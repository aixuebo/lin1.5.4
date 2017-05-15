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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 为一个cube的segment产生一个hive的临时中间表,用于存储该build需要的数据内容
 * 字段只是包含rowkey需要的列+扩展列+字典列+度量需要的列
 */
public class CubeJoinedFlatTableDesc implements IJoinedFlatTableDesc {

    private String tableName;//hive的中间表名
    private final CubeDesc cubeDesc;
    private final CubeSegment cubeSegment;//处理属于该cube对应的一段数据,即segment,如果不分区,则该属性为null

    private int columnCount;//列的数量

    private List<TblColRef> columnList = Lists.newArrayList();//所需要的属性集合
    private Map<TblColRef, Integer> columnIndexMap;//属性对应的序号映射

    public CubeJoinedFlatTableDesc(CubeDesc cubeDesc) {
        this(cubeDesc, null);
    }
    
    public CubeJoinedFlatTableDesc(CubeSegment cubeSegment) {
        this(cubeSegment.getCubeDesc(), cubeSegment);
    }
    
    private CubeJoinedFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment /* can be null */) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.columnIndexMap = Maps.newHashMap();
        parseCubeDesc();
    }

    // check what columns from hive tables are required, and index them
    //解析该cube
    private void parseCubeDesc() {

        //临时表
        if (cubeSegment == null) {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName();
        } else {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName() + "_" + cubeSegment.getUuid().replaceAll("-", "_");
        }

        int columnIndex = 0;
        for (TblColRef col : cubeDesc.listDimensionColumnsExcludingDerived(false)) {//选择需要的属性集合---刨除derived的列,因为推测列不应该参与builder
            columnIndexMap.put(col, columnIndex);
            columnList.add(col);
            columnIndex++;
        }

        //添加度量需要的列
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        int measureSize = measures.size();
        for (int i = 0; i < measureSize; i++) {
            FunctionDesc func = measures.get(i).getFunction();
            List<TblColRef> colRefs = func.getParameter().getColRefs();//该度量函数需要的列集合
            if (colRefs != null) {
                for (int j = 0; j < colRefs.size(); j++) {
                    TblColRef c = colRefs.get(j);
                    if (columnList.indexOf(c) < 0) {//函数使用的属性 目前没涉及,因此要添加该属性
                        columnIndexMap.put(c, columnIndex);
                        columnList.add(c);
                        columnIndex++;
                    }
                }
            }
        }

        //添加字典需要的列
        if (cubeDesc.getDictionaries() != null) {
            for (DictionaryDesc dictDesc : cubeDesc.getDictionaries()) {
                TblColRef c = dictDesc.getColumnRef();
                if (columnList.indexOf(c) < 0) {
                    columnIndexMap.put(c, columnIndex);
                    columnList.add(c);
                    columnIndex++;
                }
                if (dictDesc.getResuseColumnRef() != null) {
                    c = dictDesc.getResuseColumnRef();
                    if (columnList.indexOf(c) < 0) {
                        columnIndexMap.put(c, columnIndex);
                        columnList.add(c);
                        columnIndex++;
                    }
                }
            }
        }

        columnCount = columnIndex;
    }

    // sanity check the input record (in bytes) matches what's expected
    public void sanityCheck(BytesSplitter bytesSplitter) {
        if (columnCount != bytesSplitter.getBufferSize()) {
            throw new IllegalArgumentException("Expect " + columnCount + " columns, but see " + bytesSplitter.getBufferSize() + " -- " + bytesSplitter);
        }

        // TODO: check data types here
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return cubeDesc.getModel();
    }

    //获取一个列对应的序号
    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            throw new IllegalArgumentException("Column " + colRef.toString() + " wasn't found on flat table.");

        return index.intValue();
    }

    @Override
    public long getSourceOffsetStart() {
        return cubeSegment.getSourceOffsetStart();
    }

    @Override
    public long getSourceOffsetEnd() {
        return cubeSegment.getSourceOffsetEnd();
    }

    @Override
    public TblColRef getDistributedBy() {
        return cubeDesc.getDistributedByColumn();
    }

}
