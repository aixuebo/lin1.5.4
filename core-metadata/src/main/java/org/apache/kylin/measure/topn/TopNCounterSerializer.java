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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * 
 */
public class TopNCounterSerializer extends DataTypeSerializer<TopNCounter<ByteArray>> {

    private DoubleDeltaSerializer dds = new DoubleDeltaSerializer(3);

    private int precision;

    public TopNCounterSerializer(DataType dataType) {
        this.precision = dataType.getPrecision();
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        @SuppressWarnings("unused")
        int capacity = in.getInt();
        int size = in.getInt();
        int keyLength = in.getInt();
        dds.deserialize(in);
        int len = in.position() - mark + keyLength * size;
        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return precision * TopNCounter.EXTRA_SPACE_RATE * (4 + 8);
    }

    @Override
    public int getStorageBytesEstimate() {
        return precision * TopNCounter.EXTRA_SPACE_RATE * 8;
    }

    @Override
    public void serialize(TopNCounter<ByteArray> value, ByteBuffer out) {
        double[] counters = value.getCounters();
        List<ByteArray> peek = value.peek(1);
        int keyLength = peek.size() > 0 ? peek.get(0).length() : 0;//1说明有元素在队列中
        out.putInt(value.getCapacity());//容量
        out.putInt(value.size());//最终队列真实数量
        out.putInt(keyLength);//是否有元素
        dds.serialize(counters, out);//序列化增量的count
        Iterator<Counter<ByteArray>> iterator = value.iterator();
        ByteArray item;
        while (iterator.hasNext()) {
            item = iterator.next().getItem();
            out.put(item.array(), item.offset(), item.length());//序列化每一个value值
        }
    }

    //反序列化
    @Override
    public TopNCounter<ByteArray> deserialize(ByteBuffer in) {
        int capacity = in.getInt();//容量
        int size = in.getInt();//真实大小
        int keyLength = in.getInt();//是否有数据
        double[] counters = dds.deserialize(in);//还原每一个增量的count数组

        TopNCounter<ByteArray> counter = new TopNCounter<ByteArray>(capacity);//创建对象
        ByteArray byteArray;
        byte[] keyArray = new byte[size * keyLength];//
        int offset = 0;//偏移量
        for (int i = 0; i < size; i++) {
            in.get(keyArray, offset, keyLength);//还原value值
            byteArray = new ByteArray(keyArray, offset, keyLength);
            counter.offerToHead(byteArray, counters[i]);
            offset += keyLength;//累加偏移量
        }

        return counter;
    }

}
