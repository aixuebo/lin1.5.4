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

package org.apache.kylin.dict;

import org.apache.kylin.common.util.Bytes;

/**
 * @author yangli9
 * 对数字类型的数据进行目录映射处理
 */
public class NumberDictionaryBuilder<T> extends TrieDictionaryBuilder<T> {

    //如何对value进行编码
    NumberDictionary.NumberBytesCodec codec = new NumberDictionary.NumberBytesCodec(NumberDictionary.MAX_DIGITS_BEFORE_DECIMAL_POINT);//如何对数字进行编码

    public NumberDictionaryBuilder(BytesConverter<T> bytesConverter) {
        super(bytesConverter);
    }

    @Override
    public void addValue(byte[] value) {
        codec.encodeNumber(value, 0, value.length);//先对数字进行编码
        byte[] copy = Bytes.copy(codec.buf, codec.bufOffset, codec.bufLen);//属于该字节数组转换后的字节数组
        super.addValue(copy);//添加转换后的字节数组
    }

    public NumberDictionary<T> build(int baseId) {
        byte[] trieBytes = buildTrieBytes(baseId);
        NumberDictionary2<T> r = new NumberDictionary2<T>(trieBytes);
        return r;
    }

}
