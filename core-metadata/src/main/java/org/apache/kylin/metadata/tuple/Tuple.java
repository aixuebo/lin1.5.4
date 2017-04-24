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

import java.math.BigDecimal;
import java.util.List;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DoubleMutable;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.apache.kylin.metadata.model.TblColRef;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

/**
 * @author xjiang
 * 持有一行数据的schema以及具体的值
 */
public class Tuple implements ITuple {

    @IgnoreSizeOf
    private final TupleInfo info;//查询结果--表示一行记录
    private final Object[] values;//具体的值

    public Tuple(TupleInfo info) {
        this.info = info;
        this.values = new Object[info.size()];//每一个属性表示一个值
    }

    //该行数据所有的属性集合
    public List<String> getAllFields() {
        return info.getAllFields();
    }

    //该行数据所有的属性对象集合
    public List<TblColRef> getAllColumns() {
        return info.getAllColumns();
    }

    //一行数据具体的值
    public Object[] getAllValues() {
        return values;
    }

    @Override
    public Object clone() {
        return makeCopy();
    }

    @Override
    public ITuple makeCopy() {
        Tuple ret = new Tuple(this.info);
        for (int i = 0; i < this.values.length; ++i) {
            ret.values[i] = this.values[i];
        }
        return ret;
    }

    //行的属性schema
    public TupleInfo getInfo() {
        return info;
    }

    //属性对应的属性名字
    public String getFieldName(TblColRef col) {
        return info.getFieldName(col);
    }

    //属性名字对应的属性对象
    public TblColRef getFieldColumn(String fieldName) {
        return info.getColumn(fieldName);
    }

    //属性对应的具体的值
    public Object getValue(String fieldName) {
        int index = info.getFieldIndex(fieldName);
        return values[index];
    }

    //属性对应的具体的值
    public Object getValue(TblColRef col) {
        int index = info.getColumnIndex(col);
        return values[index];
    }

    //该属性对应的数据类型
    public String getDataTypeName(int idx) {
        return info.getDataTypeName(idx);
    }

    //为属性--赋予值
    public void setDimensionValue(String fieldName, String fieldValue) {
        //说先找到该属性对应的下标
        setDimensionValue(info.getFieldIndex(fieldName), fieldValue);
    }

    //为一个下标对应的属性赋予值
    public void setDimensionValue(int idx, String fieldValue) {
        Object objectValue = convertOptiqCellValue(fieldValue, getDataTypeName(idx));//类型转换
        values[idx] = objectValue;
    }

    //为度量设置属性值
    public void setMeasureValue(String fieldName, Object fieldValue) {
        setMeasureValue(info.getFieldIndex(fieldName), fieldValue);
    }

    public void setMeasureValue(int idx, Object fieldValue) {
        fieldValue = convertWritableToJava(fieldValue);//类型转换

        String dataType = getDataTypeName(idx);
        // special handling for BigDecimal, allow double be aggregated as
        // BigDecimal during cube build for best precision
        if ("double".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).doubleValue();
        } else if ("integer".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).intValue();
        } else if ("smallint".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).shortValue();
        } else if ("tinyint".equals(dataType)) {
            fieldValue = ((Number) fieldValue).byteValue();
        } else if ("float".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).floatValue();
        } else if ("date".equals(dataType) && fieldValue instanceof Long) {
            long millis = ((Long) fieldValue).longValue();
            fieldValue = (int) (millis / (1000 * 3600 * 24));
        } else if ("smallint".equals(dataType) && fieldValue instanceof Long) {
            fieldValue = ((Long) fieldValue).shortValue();
        } else if ((!"varchar".equals(dataType) || !"char".equals(dataType)) && fieldValue instanceof String) {
            fieldValue = convertOptiqCellValue((String) fieldValue, dataType);
        } else if ("bigint".equals(dataType) && fieldValue instanceof Double) {
            fieldValue = ((Double) fieldValue).longValue();
        }

        values[idx] = fieldValue;
    }

    private Object convertWritableToJava(Object o) {
        if (o instanceof LongMutable)
            o = ((LongMutable) o).get();
        else if (o instanceof DoubleMutable)
            o = ((DoubleMutable) o).get();
        return o;
    }

    public boolean hasColumn(TblColRef column) {
        return info.hasColumn(column);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String field : info.getAllFields()) {
            sb.append(field);
            sb.append("=");
            sb.append(getValue(field));
            sb.append(",");
        }
        return sb.toString();
    }

    //获取分区列具体的值
    public static long getTs(ITuple row, TblColRef partitionCol) {
        //ts column type differentiate
        if (partitionCol.getDatatype().equals("date")) {//是日期
            return epicDaysToMillis(Integer.valueOf(row.getValue(partitionCol).toString()));//将日期的时间戳加入时分秒
        } else {
            return Long.valueOf(row.getValue(partitionCol).toString());//直接获取分区列的值
        }
    }

    //days是日期的时间戳,要加入时分秒转换成long
    private static long epicDaysToMillis(int days) {
        return 1L * days * (1000 * 3600 * 24);
    }

    /**
     * 类型转换
     * @param strValue 字符串具体的值
     * @param dataTypeName 属性数据类型
     */
    public static Object convertOptiqCellValue(String strValue, String dataTypeName) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataTypeName.equals("string"))
            return null;

        // TODO use data type enum instead of string comparison
        if ("date".equals(dataTypeName)) {
            // convert epoch time
            return Integer.valueOf(dateToEpicDays(strValue));// Optiq expects Integer instead of Long. by honma
        } else if ("timestamp".equals(dataTypeName) || "datetime".equals(dataTypeName)) {
            return Long.valueOf(DateFormat.stringToMillis(strValue));
        } else if ("tinyint".equals(dataTypeName)) {
            return Byte.valueOf(strValue);
        } else if ("smallint".equals(dataTypeName)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataTypeName)) {
            return Integer.valueOf(strValue);
        } else if ("bigint".equals(dataTypeName)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataTypeName)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataTypeName)) {
            return new BigDecimal(strValue);
        } else if ("float".equals(dataTypeName)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataTypeName)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }

    //转换成天的时间戳
    private static int dateToEpicDays(String strValue) {
        long millis = DateFormat.stringToMillis(strValue);
        return (int) (millis / (1000 * 3600 * 24));
    }

}
