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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * @author George Song (ysong1)
 * 每一个列族对应一个该对象
 *
 * 用于存储 属于该列族下的某一列对应的具体度量值
 */
public class KeyValueCreator {
    byte[] cfBytes;//列族名字
    byte[] qBytes;//列名字
    long timestamp;

    int[] refIndex;//该字段下存储的measureRefs度量对应的在度量集合中的序号
    MeasureDesc[] refMeasures;//该字段下存储的measureRefs度量对应的度量对象集合

    BufferedMeasureEncoder codec;//对度量的值进行编码,存户到hbase中
    Object[] colValues;//存储属于该列的每一个度量的具体值

    public boolean isFullCopy;//true表示该列存储了全部的度量

    /**
     * @param cubeDesc
     * @param colDesc 列族对象
     */
    public KeyValueCreator(CubeDesc cubeDesc, HBaseColumnDesc colDesc) {

        cfBytes = Bytes.toBytes(colDesc.getColumnFamilyName());
        qBytes = Bytes.toBytes(colDesc.getQualifier());
        timestamp = 0; // use 0 for timestamp

        refIndex = colDesc.getMeasureIndex();
        refMeasures = colDesc.getMeasures();

        codec = new BufferedMeasureEncoder(refMeasures);
        colValues = new Object[refMeasures.length];

        isFullCopy = true;
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        for (int i = 0; i < measures.size(); i++) {
            if (refIndex.length <= i || refIndex[i] != i) //说明不是全部度量都在一个列族的一个列下存储
                isFullCopy = false;
        }
    }

    /**
     * @param key rowkey
     * @param measureValues 度量的所有值
     */
    public KeyValue create(Text key, Object[] measureValues) {
        return create(key.getBytes(), 0, key.getLength(), measureValues);
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, Object[] measureValues) {
        for (int i = 0; i < colValues.length; i++) {
            colValues[i] = measureValues[refIndex[i]];//设置属于该列的每一个度量的具体值
        }

        ByteBuffer valueBuf = codec.encode(colValues);//对这些值进行编码

        return create(keyBytes, keyOffset, keyLength, valueBuf.array(), 0, valueBuf.position());
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, byte[] value, int voffset, int vlen) {
        return new KeyValue(keyBytes, keyOffset, keyLength, //设置rowkey
                cfBytes, 0, cfBytes.length, //设置列族
                qBytes, 0, qBytes.length, //设置列名
                timestamp, KeyValue.Type.Put, //设置时间戳和put
                value, voffset, vlen);//设置该rowkey的该列族的该列,存储的具体的度量值
    }

    public KeyValue create(Text key, byte[] value, int voffset, int vlen) {
        return create(key.getBytes(), 0, key.getLength(), value, voffset, vlen);
    }

}
