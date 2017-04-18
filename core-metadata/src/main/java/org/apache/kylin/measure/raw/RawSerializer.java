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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * 对原始的字节数组集合进行序列化
 */
@SuppressWarnings("unused")
public class RawSerializer extends DataTypeSerializer<List<ByteArray>> {

    //one dictionary id value need 1~4 bytes,length need 1~4 bytes, this buffer can contain 1024/(2 to 8) * 1024 values
    //FIXME to config this and RowConstants.ROWVALUE_BUFFER_SIZE in properties file
    public static final int RAW_BUFFER_SIZE = 1024 * 1024;//1M

    //线程持有的字节数组集合
    private ThreadLocal<List<ByteArray>> current = new ThreadLocal<>();

    public RawSerializer(DataType dataType) {
    }

    private List<ByteArray> current() {
        List<ByteArray> l = current.get();
        if (l == null) {
            l = new ArrayList<ByteArray>();
            current.set(l);
        }
        return l;
    }

    //返回总的字节数
    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();//开始位置
        int len = 0;//总的占用字节数
        if (in.hasRemaining()) {
            int size = BytesUtil.readVInt(in);//获取多少个数组
            len = in.position() - mark;//size占用字节数
            for (int i = 0; i < size; i++) {
                int length = BytesUtil.peekByteArrayLength(in);//集合的元素是一个字节数组对象,该字节数组占用多少字节
                in.position(in.position() + length);//读取若干个字节
                len += length;//累加所有的字节长度集合
            }
        }
        in.position(mark);//恢复原始位置
        return len;
    }

    @Override
    public int maxLength() {
        return RAW_BUFFER_SIZE;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 8;
    }

    /**
     * 将List<ByteArray> values追加到out中
     */
    @Override
    public void serialize(List<ByteArray> values, ByteBuffer out) {
        if (values == null) {
            BytesUtil.writeVInt(0, out);
        } else {
            BytesUtil.writeVInt(values.size(), out);//写入size
            for (ByteArray array : values) {
                if (!out.hasRemaining() || out.remaining() < array.length()) {//必须空间能容纳该array
                    throw new RuntimeException("BufferOverflow! Please use one higher cardinality column for dimension column when build RAW cube!");
                }
                BytesUtil.writeByteArray(BytesUtil.subarray(array.array(), array.offset(), array.offset() + array.length()), out);//截取一段字节数组,从开始位置,到结束位置,将其写入到out中
            }
        }
    }

    @Override
    public List<ByteArray> deserialize(ByteBuffer in) {
        List<ByteArray> values = current();
        values.clear();
        int size = BytesUtil.readVInt(in);
        if (size >= 0) {
            for (int i = 0; i < size; i++) {
                ByteArray ba = new ByteArray(BytesUtil.readByteArray(in));
                if (ba.length() != 0) {
                    values.add(ba);
                }
            }
        } else {
            throw new RuntimeException("Read error data size:" + size);
        }
        return values;
    }
}
