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

import org.apache.kylin.common.util.BytesUtil;

/**
 * LongSerializer表示可变化的vlong
 */
public class LongSerializer extends DataTypeSerializer<LongMutable> {

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<LongMutable> current = new ThreadLocal<LongMutable>();

    public LongSerializer(DataType type) {
    }

    @Override
    public void serialize(LongMutable value, ByteBuffer out) {
        BytesUtil.writeVLong(value.get(), out);
    }

    private LongMutable current() {
        LongMutable l = current.get();
        if (l == null) {
            l = new LongMutable();
            current.set(l);
        }
        return l;
    }

    @Override
    public LongMutable deserialize(ByteBuffer in) {
        LongMutable l = current();
        l.set(BytesUtil.readVLong(in));
        return l;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();//当前位置

        BytesUtil.readVLong(in);//读取一个long
        int len = in.position() - mark;//记录该long占用多少个字节

        in.position(mark);//恢复到未读取之前的位置
        return len;
    }

    @Override
    public int maxLength() {
        return 9; // vlong: 1 + 8
    } //需要第一个位置存储多少个字节

    @Override
    public int getStorageBytesEstimate() {
        return 5;
    }//平均需要5个字节

    @Override
    public LongMutable valueOf(String str) {
        return new LongMutable(Long.parseLong(str));
    }
}
