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
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;

import com.google.common.collect.Maps;

/**
 * Note: the implementations MUST be thread-safe.
 * 序列化和反序列化,即对象T和字节数组的转换
 */
abstract public class DataTypeSerializer<T> implements BytesSerializer<T> {

    /**
没有对应关系的类型
     register("any",
     "binary", //
     "real",  "decimal", "numeric", //
     "time",
     InnerDataTypeEnum.LITERAL.getDataType(), InnerDataTypeEnum.DERIVED.getDataType());
     */
    //类型字符串与具体实现序列化类的映射
    final static Map<String, Class<?>> implementations = Maps.newHashMap();
    static {
        implementations.put("char", StringSerializer.class);
        implementations.put("varchar", StringSerializer.class);

        implementations.put("double", DoubleSerializer.class);
        implementations.put("float", DoubleSerializer.class);

        //LongSerializer表示可变化的vlong
        implementations.put("long", LongSerializer.class);
        implementations.put("integer", LongSerializer.class);
        implementations.put("int", LongSerializer.class);
        implementations.put("tinyint", LongSerializer.class);
        implementations.put("smallint", LongSerializer.class);
        implementations.put("bigint", LongSerializer.class);
        implementations.put("int4", Int4Serializer.class);//真的用4个字节存储int
        implementations.put("long8", Long8Serializer.class);//真的用8个字节存储long

        implementations.put("boolean", BooleanSerializer.class);

        implementations.put("date", DateTimeSerializer.class);
        implementations.put("datetime", DateTimeSerializer.class);
        implementations.put("timestamp", DateTimeSerializer.class);
    }

    public static void register(String dataTypeName, Class<? extends DataTypeSerializer<?>> impl) {
        implementations.put(dataTypeName, impl);
    }

    //创建具体实现类
    public static DataTypeSerializer<?> create(String dataType) {
        return create(DataType.getType(dataType));
    }

    public static DataTypeSerializer<?> create(DataType type) {
        Class<?> clz = implementations.get(type.getName());//找到对应的具体实现类
        if (clz == null)
            throw new RuntimeException("No DataTypeSerializer for type " + type);

        try {
            return (DataTypeSerializer<?>) clz.getConstructor(DataType.class).newInstance(type);
        } catch (Exception e) {
            throw new RuntimeException(e); // never happen
        }
    }

    /** Peek into buffer and return the length of serialization which is previously written by this.serialize().
     *  The current position of input buffer is guaranteed to be at the beginning of the serialization.
     *  The implementation must not alter the buffer position by its return.
     *  从buffer中读取下一个元素需要占用个字节,常常用于vlong等情况
     **/
    abstract public int peekLength(ByteBuffer in);

    /** Return the max number of bytes to the longest possible serialization
     * 序列化后最长的字节数
     **/
    abstract public int maxLength();

    /** Get an estimate of the average size in bytes of this kind of serialized data
     * 获取预估的平均字节大小
     **/
    abstract public int getStorageBytesEstimate();

    /** An optional convenient method that converts a string to this data type (for dimensions)
     * 将一个字符串转换成对应的数据类型
     **/
    public T valueOf(String str) {
        throw new UnsupportedOperationException();
    }

    /** Convert from obj to string */
    public String toString(T value) {
        if (value == null)
            return "NULL";
        else
            return value.toString();
    }
}
