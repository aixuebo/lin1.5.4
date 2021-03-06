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

package org.apache.kylin.metadata.datatype;

import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.BooleanUtils;

public class BooleanSerializer extends DataTypeSerializer<LongMutable> {

    public final static String[] TRUE_VALUE_SET = { "true", "t", "on", "yes" };//表示true含义的集合

    // be thread-safe and avoid repeated obj creation
    //使用一个long代表该boolean值
    private ThreadLocal<LongMutable> current = new ThreadLocal<LongMutable>();

    public BooleanSerializer(DataType type) {
    }

    //序列化就是将一个long存储到字节数组中
    @Override
    public void serialize(LongMutable value, ByteBuffer out) {
        out.putLong(value.get());
    }

    @Override
    public LongMutable deserialize(ByteBuffer in) {
        LongMutable l = current();
        l.set(in.getLong());
        return l;
    }

    //获取当前线程对应的值
    private LongMutable current() {
        LongMutable l = current.get();
        if (l == null) {
            l = new LongMutable();
            current.set(l);
        }
        return l;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 8;
    }

    @Override
    public int maxLength() {
        return 8;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 8;
    }

    @Override
    public LongMutable valueOf(String str) {
        LongMutable l = current();
        if (str == null)
            l.set(0L);//默认是false
        else
            l.set(BooleanUtils.toInteger(ArrayUtils.contains(TRUE_VALUE_SET, str.toLowerCase())));//判断内容是否是true的字符串
        return l;
    }
}