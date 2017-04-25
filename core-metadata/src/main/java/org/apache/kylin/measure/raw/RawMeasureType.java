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

package org.apache.kylin.measure.raw;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 {
     "name": "R",
     "function": {
         "expression": "RAW",
         "parameter": {
             "type": "column",
             "value": "OPEN_TIME",
             "next_parameter": null
         },
        "returntype": "raw"
     },
     "dependent_measure_ref": null
 }
 */
public class RawMeasureType extends MeasureType<List<ByteArray>> {

    private static final Logger logger = LoggerFactory.getLogger(RawMeasureType.class);

    public static final String FUNC_RAW = "RAW";//函数
    public static final String DATATYPE_RAW = "raw";//返回类型

    public static class Factory extends MeasureTypeFactory<List<ByteArray>> {

        @Override
        public MeasureType<List<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new RawMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_RAW;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_RAW;
        }

        @Override
        public Class<? extends DataTypeSerializer<List<ByteArray>>> getAggrDataTypeSerializer() {
            return RawSerializer.class;
        }
    }

    private final DataType dataType;

    public RawMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (FUNC_RAW.equals(funcName) == false)
            throw new IllegalArgumentException();

        if (DATATYPE_RAW.equals(dataType.getName()) == false)
            throw new IllegalArgumentException();

    }

    //true 因为该操作是耗费内存的操作
    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<List<ByteArray>> newIngester() {
        return new MeasureIngester<List<ByteArray>>() {
            //encode measure value to dictionary
            //产生字节数组集合
            @Override
            public List<ByteArray> valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 1)
                    throw new IllegalArgumentException();

                //source input column value
                String literal = values[0];//具体的值
                // encode literal using dictionary
                TblColRef literalCol = getRawColumn(measureDesc.getFunction());//获取函数对应的第一个参数需要的列
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);//获取该列对应的字典

                int keyEncodedValue = dictionary.getIdFromValue(literal);//返回字典编码
                ByteArray key = new ByteArray(dictionary.getSizeOfId());
                BytesUtil.writeUnsigned(keyEncodedValue, key.array(), key.offset(), dictionary.getSizeOfId());//将keyEncodedValue变成字节数组,存储在key中

                List<ByteArray> valueList = new ArrayList<ByteArray>(1);
                valueList.add(key);
                return valueList;
            }

            //merge measure dictionary
            //用于合并merge操作
            @Override
            public List<ByteArray> reEncodeDictionary(List<ByteArray> value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TblColRef colRef = getRawColumn(measureDesc.getFunction());//该函数使用了哪一列
                Dictionary<String> sourceDict = oldDicts.get(colRef);//老的编码
                Dictionary<String> mergedDict = newDicts.get(colRef);//新的编码

                int valueSize = value.size();//要为一组集合进行编码
                byte[] newIdBuf = new byte[valueSize * mergedDict.getSizeOfId()];//新的字节数组---值的个数*每一个需要新的字典id大小
                byte[] literal = new byte[sourceDict.getSizeOfValue()];

                int bufOffset = 0;//在newIdBuf字节数组中的偏移量
                for (ByteArray c : value) {//循环每一个value
                    int oldId = BytesUtil.readUnsigned(c.array(), c.offset(), c.length());//老的编码ID
                    int newId;
                    int size = sourceDict.getValueBytesFromId(oldId, literal, 0);//将老的编码id还原成字节数组
                    if (size < 0) {
                        newId = mergedDict.nullId();
                    } else {
                        newId = mergedDict.getIdFromValueBytes(literal, 0, size);//对字节数组重新编码
                    }

                    BytesUtil.writeUnsigned(newId, newIdBuf, bufOffset, mergedDict.getSizeOfId());//将新的id存储到newIdBuf中
                    c.set(newIdBuf, bufOffset, mergedDict.getSizeOfId());//将新的编码内容存放到c中

                    bufOffset += mergedDict.getSizeOfId();//移动偏移量
                }
                return value;//新的值已经变成新的字典编码了
            }
        };
    }

    //如何对数据进行聚类
    @Override
    public MeasureAggregator<List<ByteArray>> newAggregator() {
        return new RawAggregator();
    }

    //获取需要编码的列集合
    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        TblColRef literalCol = functionDesc.getParameter().getColRefs().get(0);//获取该函数使用到的列
        return Collections.singletonList(literalCol);
    }

    public CapabilityResult.CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc measureDesc) {
        //is raw query
        if (!digest.isRawQuery) //必须得是true
            return null;

        TblColRef rawColumn = getRawColumn(measureDesc.getFunction());//获取该raw需要对应列
        if (!digest.allColumns.isEmpty() && !digest.allColumns.contains(rawColumn)) {//rawColumn必须存在
            return null;
        }

        unmatchedAggregations.remove(measureDesc.getFunction());

        //contain one raw measure : cost * 0.9
        return new CapabilityResult.CapabilityInfluence() {
            @Override
            public double suggestCostMultiplier() {
                return 0.9;
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return null;
    }

    //调整sql
    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {

        if (sqlDigest.isRawQuery) {//一定是true
            for (MeasureDesc measureDesc : measureDescs) {
                TblColRef col = this.getRawColumn(measureDesc.getFunction());//度量需要的字段

                //老字段
                ParameterDesc colParameter = new ParameterDesc();
                colParameter.setType("column");
                colParameter.setValue(col.getName());

                //新的字段
                FunctionDesc rawFunc = new FunctionDesc();
                rawFunc.setExpression("RAW");
                rawFunc.setParameter(colParameter);

                if (sqlDigest.allColumns.contains(col)) {
                    if (measureDesc.getFunction().equals(rawFunc)) {//找到raw度量函数
                        FunctionDesc sumFunc = new FunctionDesc();
                        sumFunc.setExpression("SUM");
                        sumFunc.setParameter(colParameter);//对该字段求sum
                        sqlDigest.aggregations.remove(sumFunc);//删除sum函数
                        sqlDigest.aggregations.add(rawFunc);//添加raw函数
                        logger.info("Add RAW measure on column " + col);
                    }
                    if (!sqlDigest.metricColumns.contains(col)) {
                        sqlDigest.metricColumns.add(col);
                    }
                }
            }
        }
    }

    /**
     * 是否需要更高级的填充,即一行数据填充到多行中
     * 即getAdvancedTupleFiller方法是否有实现
     */
    @Override
    public boolean needAdvancedTupleFilling() {
        return true;
    }

    //简单的填充模式,一行记录填充到一个属性中
    @Override
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        throw new UnsupportedOperationException();
    }

    //高级的填充模式,每一条结果被填充到多行中
    @Override
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo tupleInfo, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final TblColRef literalCol = getRawColumn(function);//获取函数对应的第一个参数需要的列
        final Dictionary<String> rawColDict = dictionaryMap.get(literalCol);//获取该列对应的字典
        final int literalTupleIdx = tupleInfo.hasColumn(literalCol) ? tupleInfo.getColumnIndex(literalCol) : -1;//该列存在,则返回该列的下标,如果该列不存在,则返回-1

        return new IAdvMeasureFiller() {
            private List<ByteArray> rawList;//全部数据
            private Iterator<ByteArray> rawIterator;//数据迭代器
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.rawList = (List<ByteArray>) measureValue;
                this.rawIterator = rawList.iterator();
                this.expectRow = 0;
            }

            //有多少行数据
            @Override
            public int getNumOfRows() {
                return rawList.size();
            }

            //将第row行的数据填充到tuple中
            @Override
            public void fillTuple(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                ByteArray raw = rawIterator.next();
                int key = BytesUtil.readUnsigned(raw.array(), raw.offset(), raw.length());//字典key
                String colValue = rawColDict.getValueFromId(key);//还原字典对应的具体的内容
                tuple.setDimensionValue(literalTupleIdx, colValue);//为该下标填充值
            }
        };
    }

    //获取函数对应的第一个参数需要的列
    private TblColRef getRawColumn(FunctionDesc functionDesc) {
        return functionDesc.getParameter().getColRefs().get(0);
    }

    @Override
    public boolean onlyAggrInBaseCuboid() {
        return true;
    }
}
