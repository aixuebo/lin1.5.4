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

package org.apache.kylin.measure.topn;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TopNMeasureType extends MeasureType<TopNCounter<ByteArray>> {

    private static final Logger logger = LoggerFactory.getLogger(TopNMeasureType.class);

    public static final String FUNC_TOP_N = "TOP_N";
    public static final String DATATYPE_TOPN = "topn";

    public static final String CONFIG_ENCODING_PREFIX = "topn.encoding.";
    public static final String CONFIG_AGG = "topn.aggregation";
    public static final String CONFIG_ORDER = "topn.order";

    public static class Factory extends MeasureTypeFactory<TopNCounter<ByteArray>> {

        @Override
        public MeasureType<TopNCounter<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new TopNMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_TOP_N;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_TOPN;
        }

        @Override
        public Class<? extends DataTypeSerializer<TopNCounter<ByteArray>>> getAggrDataTypeSerializer() {
            return TopNCounterSerializer.class;
        }
    }

    // ============================================================================

    private final DataType dataType;

    public TopNMeasureType(String funcName, DataType dataType) {
        // note at query parsing phase, the data type may be null, because only function and parameters are known
        this.dataType = dataType;
    }

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (FUNC_TOP_N.equals(funcName) == false)
            throw new IllegalArgumentException();

        if (DATATYPE_TOPN.equals(dataType.getName()) == false)
            throw new IllegalArgumentException();

        if (dataType.getPrecision() < 1 || dataType.getPrecision() > 10000)
            throw new IllegalArgumentException();
    }

    //占用内存
    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public MeasureIngester<TopNCounter<ByteArray>> newIngester() {
        return new MeasureIngester<TopNCounter<ByteArray>>() {

            private DimensionEncoding[] dimensionEncodings = null;//函数用到的每一个字段对应的字典映射数组
            private List<TblColRef> literalCols = null;//处理第一个字段以外,所有的字段集合
            private int keyLength = 0;//字典需要的字节和

            private DimensionEncoding[] newDimensionEncodings = null;
            private int newKeyLength = 0;
            private boolean needReEncode = true;

            @Override
            public TopNCounter<ByteArray> valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);

                if (dimensionEncodings == null) {
                    literalCols = getTopNLiteralColumn(measureDesc.getFunction());//处理第一个字段以外,所有的字段集合
                    dimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, dictionaryMap);//函数用到的每一个字段对应的字典映射数组
                    for (DimensionEncoding encoding : dimensionEncodings) {
                        keyLength += encoding.getLengthOfEncoding();
                    }

                    if (values.length != (literalCols.size() + 1)) {
                        throw new IllegalArgumentException();
                    }
                }

                final ByteArray key = new ByteArray(keyLength);//存储编码后的key
                int offset = 0;
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    byte[] valueBytes = Bytes.toBytes(values[i + 1]);//获取value的值
                    dimensionEncodings[i].encode(valueBytes, valueBytes.length, key.array(), offset);//将value值进行编码
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }

                TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(dataType.getPrecision() * TopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);//key就是编码后的集合,value是该count对象
                return topNCounter;
            }

            @Override
            public TopNCounter<ByteArray> reEncodeDictionary(TopNCounter<ByteArray> value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TopNCounter<ByteArray> topNCounter = value;

                if (newDimensionEncodings == null) {
                    literalCols = getTopNLiteralColumn(measureDesc.getFunction());
                    dimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, oldDicts);
                    keyLength = 0;
                    boolean hasDictEncoding = false;
                    for (DimensionEncoding encoding : dimensionEncodings) {
                        keyLength += encoding.getLengthOfEncoding();
                        if (encoding instanceof DictionaryDimEnc) {
                            hasDictEncoding = true;
                        }
                    }

                    newDimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, newDicts);
                    newKeyLength = 0;
                    for (DimensionEncoding encoding : newDimensionEncodings) {
                        newKeyLength += encoding.getLengthOfEncoding();
                    }

                    needReEncode = hasDictEncoding;
                }

                if (needReEncode == false) {
                    // no need re-encode
                    return topNCounter;
                }

                int topNSize = topNCounter.size();
                byte[] newIdBuf = new byte[topNSize * newKeyLength];

                int bufOffset = 0;
                for (Counter<ByteArray> c : topNCounter) {
                    int offset = c.getItem().offset();
                    int innerBuffOffset = 0;
                    for (int i = 0; i < dimensionEncodings.length; i++) {
                        String dimValue = dimensionEncodings[i].decode(c.getItem().array(), offset, dimensionEncodings[i].getLengthOfEncoding());
                        byte[] dimValueBytes = Bytes.toBytes(dimValue);
                        newDimensionEncodings[i].encode(dimValueBytes, dimValueBytes.length, newIdBuf, bufOffset + innerBuffOffset);
                        innerBuffOffset += newDimensionEncodings[i].getLengthOfEncoding();
                        offset += dimensionEncodings[i].getLengthOfEncoding();
                    }

                    c.getItem().set(newIdBuf, bufOffset, newKeyLength);
                    bufOffset += newKeyLength;
                }
                return topNCounter;
            }
        };
    }

    //聚合函数
    @Override
    public MeasureAggregator<TopNCounter<ByteArray>> newAggregator() {
        return new TopNAggregator();
    }

    //获取需要编码的列
    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        List<TblColRef> columnsNeedDict = Lists.newArrayList();
        List<TblColRef> allCols = functionDesc.getParameter().getColRefs();//参数需要的列集合
        int start = (functionDesc.getParameter().isColumnType() == true) ? 1 : 0;
        for (int i = start; i < allCols.size(); i++) {
            TblColRef tblColRef = allCols.get(i);
            String encoding = functionDesc.getConfiguration().get(CONFIG_ENCODING_PREFIX + tblColRef.getName());
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                columnsNeedDict.add(tblColRef);
            }
        }

        return columnsNeedDict;
    }

    @Override
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc topN) {
        // TopN measure can (and only can) provide one numeric measure and one literal dimension
        // e.g. select seller, sum(gmv) from ... group by seller order by 2 desc limit 100

        // check digest requires only one measure
        if (digest.aggregations.size() != 1)
            return null;

        // the measure function must be SUM
        FunctionDesc onlyFunction = digest.aggregations.iterator().next();
        if (isTopNCompatibleSum(topN.getFunction(), onlyFunction) == false)
            return null;

        List<TblColRef> literalCol = getTopNLiteralColumn(topN.getFunction());
        if (unmatchedDimensions.containsAll(literalCol) == false)
            return null;
        if (digest.groupbyColumns.containsAll(literalCol) == false)
            return null;

        unmatchedDimensions.removeAll(literalCol);
        unmatchedAggregations.remove(onlyFunction);
        return new CapabilityInfluence() {
            @Override
            public double suggestCostMultiplier() {
                return 0.3; // make sure TopN get ahead of other matched realizations
            }
        };
    }

    private boolean isTopNCompatibleSum(FunctionDesc topN, FunctionDesc sum) {
        if (sum == null)
            return false;

        if (!isTopN(topN))
            return false;

        TblColRef topnNumCol = getTopNNumericColumn(topN);//第一个字段

        if (topnNumCol == null) {
            if (sum.isCount())//COUNT函数
                return true;

            return false;
        }

        if (sum.isSum() == false)
            return false;

        if (sum.getParameter() == null || sum.getParameter().getColRefs() == null || sum.getParameter().getColRefs().size() == 0)
            return false;

        TblColRef sumCol = sum.getParameter().getColRefs().get(0);
        return sumCol.equals(topnNumCol);
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return null;
    }

    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (MeasureDesc measureDesc : measureDescs) {//循环度量对象
            FunctionDesc topnFunc = measureDesc.getFunction();
            List<TblColRef> topnLiteralCol = getTopNLiteralColumn(topnFunc);//处理第一个字段以外,所有的字段集合

            if (sqlDigest.groupbyColumns.containsAll(topnLiteralCol) == false) //如果group by 已经全包含了,则返回true,不是全包含才返回false
                return;//因为没有全包含,所以没有优化方式,因此返回即可

            //代码执行到这里,说明group by 包含所有的top需要的字段

            if (sqlDigest.aggregations.size() > 1) {//当查询topN的时候,只能允许有一个度量聚合函数
                throw new IllegalStateException("When query with topN, only one metrics is allowed.");
            }

            if (sqlDigest.aggregations.size() > 0) {//说明有聚合函数
                FunctionDesc origFunc = sqlDigest.aggregations.iterator().next();//获取这一个聚合函数
                if (origFunc.isSum() == false && origFunc.isCount() == false) {//说明该函数不是sum,也不是count,是不允许的
                    throw new IllegalStateException("When query with topN, only SUM function is allowed.");//topN的时候,只能允许是sum或者count函数
                }
                logger.info("Rewrite function " + origFunc + " to " + topnFunc);
            }

            sqlDigest.aggregations = Lists.newArrayList(topnFunc);//设置聚合函数只能有topN这一个函数
            sqlDigest.groupbyColumns.removeAll(topnLiteralCol);//不需要group by参数,都移除掉
            sqlDigest.metricColumns.addAll(topnLiteralCol);//添加度量维度
        }
    }

    @Override
    public boolean needAdvancedTupleFilling() {
        return true;
    }

    @Override
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo tupleInfo, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final List<TblColRef> literalCols = getTopNLiteralColumn(function);//函数除了第一个字段外的所有字段
        final TblColRef numericCol = getTopNNumericColumn(function);//函数的第一个字段

        final DimensionEncoding[] dimensionEncodings = getDimensionEncodings(function, literalCols, dictionaryMap);//每一个字段的编码方式

        //存储每一个字段的索引位置
        final int[] literalTupleIdx = new int[literalCols.size()];
        for (int i = 0; i < literalCols.size(); i++) {
            TblColRef colRef = literalCols.get(i);
            literalTupleIdx[i] = tupleInfo.hasColumn(colRef) ? tupleInfo.getColumnIndex(colRef) : -1;
        }

        // for TopN, the aggr must be SUM, so the number fill into the column position (without rewrite)
        final int numericTupleIdx;//第一个字段的索引位置
        if (numericCol != null) {
            numericTupleIdx = tupleInfo.hasColumn(numericCol) ? tupleInfo.getColumnIndex(numericCol) : -1;
        } else {
            numericTupleIdx = tupleInfo.getFieldIndex("COUNT__");
        }

        return new IAdvMeasureFiller() {
            private TopNCounter<ByteArray> topNCounter;//元素集合
            private Iterator<Counter<ByteArray>> topNCounterIterator;//每个元素
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.topNCounter = (TopNCounter<ByteArray>) measureValue;
                this.topNCounterIterator = topNCounter.iterator();
                this.expectRow = 0;
            }

            //总记录数
            @Override
            public int getNumOfRows() {
                return topNCounter.size();
            }

            @Override
            public void fillTuple(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                Counter<ByteArray> counter = topNCounterIterator.next();//元素
                int offset = counter.getItem().offset();
                for (int i = 0; i < dimensionEncodings.length; i++) {//循环每一个编码维度
                    String colValue = dimensionEncodings[i].decode(counter.getItem().array(), offset, dimensionEncodings[i].getLengthOfEncoding());//还原每一个维度对应的值
                    tuple.setDimensionValue(literalTupleIdx[i], colValue);//设置该列的值
                    offset += dimensionEncodings[i].getLengthOfEncoding();//移动偏移量
                }
                tuple.setMeasureValue(numericTupleIdx, counter.getCount());//设置度量值
            }
        };
    }

    //函数用到的每一个字段对应的字典映射数组
    private static DimensionEncoding[] getDimensionEncodings(FunctionDesc function, List<TblColRef> literalCols, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final DimensionEncoding[] dimensionEncodings = new DimensionEncoding[literalCols.size()];//表示每一个字段的编码方式
        for (int i = 0; i < literalCols.size(); i++) {//循环每一个字段
            TblColRef colRef = literalCols.get(i);
            String encoding = function.getConfiguration().get(TopNMeasureType.CONFIG_ENCODING_PREFIX + colRef.getName());//获取该字段的编码方式
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {//如果编码方式是dict
                dimensionEncodings[i] = new DictionaryDimEnc(dictionaryMap.get(colRef));
            } else {
                Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                dimensionEncodings[i] = DimensionEncodingFactory.create((String) encodingConf[0], (String[]) encodingConf[1]);
            }
        }

        return dimensionEncodings;
    }

    //第一个字段
    private TblColRef getTopNNumericColumn(FunctionDesc functionDesc) {
        if (functionDesc.getParameter().isColumnType() == true) {
            return functionDesc.getParameter().getColRefs().get(0);
        }
        return null;
    }

    /**
     * 处理第一个字段以外,所有的字段集合
     */
    private List<TblColRef> getTopNLiteralColumn(FunctionDesc functionDesc) {
        List<TblColRef> allColumns = functionDesc.getParameter().getColRefs();
        if (functionDesc.getParameter().isColumnType() == false) {
            return allColumns;
        }
        return allColumns.subList(1, allColumns.size());
    }

    //true表示确定是topN方法
    private boolean isTopN(FunctionDesc functionDesc) {
        return FUNC_TOP_N.equalsIgnoreCase(functionDesc.getExpression());
    }

}
