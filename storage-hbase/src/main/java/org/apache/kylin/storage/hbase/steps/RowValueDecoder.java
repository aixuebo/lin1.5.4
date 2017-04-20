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

package org.apache.kylin.storage.hbase.steps;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;

import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.MeasureDecoder;
import org.apache.kylin.metadata.datatype.DoubleMutable;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.storage.hbase.util.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析hbase的一行数据
 */
public class RowValueDecoder implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(RowValueDecoder.class);

    private final HBaseColumnDesc hbaseColumn;//列族对象
    private final byte[] hbaseColumnFamily;//列族
    private final byte[] hbaseColumnQualifier;//列

    private final BitSet projectionIndex;

    private final MeasureDecoder codec;//如何解析度量对应的值
    private final MeasureDesc[] measures;//该字段下存储的measureRefs度量对应的度量对象集合

    private final Object[] values;//度量对应的值解析后的集合

    //参数是列族对象
    public RowValueDecoder(HBaseColumnDesc hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
        this.hbaseColumnFamily = Bytes.toBytes(hbaseColumn.getColumnFamilyName());
        this.hbaseColumnQualifier = Bytes.toBytes(hbaseColumn.getQualifier());
        this.projectionIndex = new BitSet();
        this.measures = hbaseColumn.getMeasures();
        this.codec = new MeasureDecoder(measures);
        this.values = new Object[measures.length];
    }

    public void decodeAndConvertJavaObj(Result hbaseRow) {
        decode(hbaseRow, true);
    }

    public void decode(Result hbaseRow) {
        decode(hbaseRow, false);
    }

    /**
     * 解析一行hbase的结果集
     */
    private void decode(Result hbaseRow, boolean convertToJavaObject) {
        ByteBuffer buffer = Results.getValueAsByteBuffer(hbaseRow, hbaseColumnFamily, hbaseColumnQualifier);//获取结果集中属于该列族 该列的字节数组值
        decode(buffer, convertToJavaObject);
    }

    public void decodeAndConvertJavaObj(byte[] bytes) {
        decode(ByteBuffer.wrap(bytes), true);
    }

    /**
     * @param bytes  一行hbase的结果集对应的字节数组
     */
    public void decode(byte[] bytes) {
        decode(ByteBuffer.wrap(bytes), false);
    }

    /**
     *
     * @param buffer 一行hbase的结果集对应的字节数组
     * @param convertToJavaObject
     */
    private void decode(ByteBuffer buffer, boolean convertToJavaObject) {
        codec.decode(buffer, values);//解析字节数组,转换成value对象集合
        if (convertToJavaObject) {
            convertToJavaObjects(values, values, convertToJavaObject);
        }
    }

    private void convertToJavaObjects(Object[] mapredObjs, Object[] results, boolean convertToJavaObject) {
        for (int i = 0; i < mapredObjs.length; i++) {
            Object o = mapredObjs[i];

            if (o instanceof LongMutable)
                o = ((LongMutable) o).get();
            else if (o instanceof DoubleMutable)
                o = ((DoubleMutable) o).get();

            results[i] = o;
        }
    }

    public void setProjectIndex(int bitIndex) {
        projectionIndex.set(bitIndex);
    }

    public BitSet getProjectionIndex() {
        return projectionIndex;
    }

    public HBaseColumnDesc getHBaseColumn() {
        return hbaseColumn;
    }

    public Object[] getValues() {
        return values;
    }

    public MeasureDesc[] getMeasures() {
        return measures;
    }

    // result is in order of <code>CubeDesc.getMeasures()</code>
    public void loadCubeMeasureArray(Object[] result) {
        int[] measureIndex = hbaseColumn.getMeasureIndex();
        for (int i = 0; i < measureIndex.length; i++) {
            result[measureIndex[i]] = values[i];
        }
    }

    public boolean hasMemHungryMeasures() {
        for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
            FunctionDesc func = measures[i].getFunction();
            if (func.getMeasureType().isMemoryHungry())
                return true;
        }
        return false;
    }

    public static boolean hasMemHungryMeasures(Collection<RowValueDecoder> rowValueDecoders) {
        for (RowValueDecoder decoder : rowValueDecoders) {
            if (decoder.hasMemHungryMeasures())
                return true;
        }
        return false;
    }

}
