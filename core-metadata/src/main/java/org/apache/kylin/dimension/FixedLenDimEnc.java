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

package org.apache.kylin.dimension;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用固定字节数进行编码---即不需要编码,只是将value只是获取固定字符长度,即对大的value进行截断处理就算编码了
 * 超过该固定字节数的要进行截断,并且打印日志
 * 少于固定字节数的要进行用9字节进行占位补充
 */
public class FixedLenDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(FixedLenDimEnc.class);

    // row key fixed length place holder
    public static final byte ROWKEY_PLACE_HOLDER_BYTE = 9;//占位符字节

    public static final String ENCODING_NAME = "fixed_length";//编码的名字

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new FixedLenDimEnc(Integer.parseInt(args[0]));
        }
    };

    // ============================================================================

    private int fixedLen;//固定字节长度

    transient private int avoidVerbose = 0;//输错的数据次数

    //no-arg constructor is required for Externalizable
    public FixedLenDimEnc() {
    }

    /**
     * 参数是value值的大小,即创建字典的时候提供value的长度大小即可,可以推断出需要字典编码大小
     */
    public FixedLenDimEnc(int len) {
        this.fixedLen = len;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FixedLenDimEnc that = (FixedLenDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    /**
     * 对value进行编码
     * @param value 原始value
     * @param valueLen 原始value的长度
     * @param output 输出到哪个字节数组中
     * @param outputOffset 从哪个字节开始使用output
     */
    @Override
    public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);//填充fixedLen个null到output输出流中
            return;
        }

        if (valueLen > fixedLen) {//长度超过了固定值
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueLen + ", will truncate, value string: " + Bytes.toString(value, 0, valueLen) + " times:" + avoidVerbose);//打印日志,需要固定长度,但是现在超过了固定长度
            }
        }

        int n = Math.min(valueLen, fixedLen);//如果超过固定长度,则截断数据,如果没超过固定长度,则就使用传入的value
        System.arraycopy(value, 0, output, outputOffset, n);//将有效的数据写入到out中

        if (n < fixedLen) {//因为固定的比有效的长,因此补充少的字节9作为占位字节
            Arrays.fill(output, outputOffset + n, outputOffset + fixedLen, ROWKEY_PLACE_HOLDER_BYTE);
        }
    }

    //解码
    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {//是null
            return null;
        }

        while (len > 0 && bytes[offset + len - 1] == ROWKEY_PLACE_HOLDER_BYTE)//一直循环,找到非占位字符为止
            len--;

        return Bytes.toString(bytes, offset, len);//获取真实的具体值
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new FixedLenSerializer();
    }

    public class FixedLenSerializer extends DataTypeSerializer<Object> {
        // be thread-safe and avoid repeated obj creation
        private ThreadLocal<byte[]> current = new ThreadLocal<byte[]>();//线程下的字节数组的缓冲池

        //获取线程下的字节数组缓冲池
        private byte[] currentBuf() {
            byte[] buf = current.get();
            if (buf == null) {
                buf = new byte[fixedLen];
                current.set(buf);
            }
            return buf;
        }

        //将value进行序列化
        @Override
        public void serialize(Object value, ByteBuffer out) {
            byte[] buf = currentBuf();
            byte[] bytes = value == null ? null : Bytes.toBytes(value.toString());
            encode(bytes, bytes == null ? 0 : bytes.length, buf, 0);
            out.put(buf);
        }

        //反序列化
        @Override
        public Object deserialize(ByteBuffer in) {
            byte[] buf = currentBuf();
            in.get(buf);
            return decode(buf, 0, buf.length);
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return fixedLen;
        }

        @Override
        public int maxLength() {
            return fixedLen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return fixedLen;
        }

        @Override
        public Object valueOf(String str) {
            return str;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(fixedLen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fixedLen = in.readShort();
    }

}
