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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * 
 * @author xjiang
 * 获取该列对应的数据(在一行中)
 */
public class ColumnTupleFilter extends TupleFilter {

    private TblColRef columnRef;//列名字
    private Object tupleValue;//具体的列值
    private List<Object> values;//第0个表示该列具体的值

    public ColumnTupleFilter(TblColRef column) {
        super(Collections.<TupleFilter> emptyList(), FilterOperatorEnum.COLUMN);
        this.columnRef = column;
        this.values = new ArrayList<Object>(1);
        this.values.add(null);//先设置为null,占用一个位置,最终用于存储该列最终的值
    }

    public TblColRef getColumn() {
        return columnRef;
    }

    public void setColumn(TblColRef col) {
        this.columnRef = col;
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "" + columnRef;
    }

    /**
     * 获取该行数据中该列对应的值
     * @param tuple 一行数据内容
     */
    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        this.tupleValue = tuple.getValue(columnRef);//获取具体的列值
        return true;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    //获取最终的值
    @Override
    public Collection<?> getValues() {
        this.values.set(0, this.tupleValue);//将具体的列值赋予values
        return this.values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        String table = columnRef.getTable();//该列所在table
        BytesUtil.writeUTFString(table, buffer);

        String columnId = columnRef.getColumnDesc().getId();//该列ID
        BytesUtil.writeUTFString(columnId, buffer);

        String columnName = columnRef.getName();//该列名字
        BytesUtil.writeUTFString(columnName, buffer);

        String dataType = columnRef.getDatatype();//该列数据类型
        BytesUtil.writeUTFString(dataType, buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

        TableDesc table = null;
        ColumnDesc column = new ColumnDesc();

        String tableName = BytesUtil.readUTFString(buffer);
        if (tableName != null) {
            table = new TableDesc();
            table.setName(tableName);
        }

        column.setId(BytesUtil.readUTFString(buffer));
        column.setName(BytesUtil.readUTFString(buffer));
        column.setDatatype(BytesUtil.readUTFString(buffer));
        column.init(table);

        this.columnRef = column.getRef();
    }
}
