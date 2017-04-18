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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * deprecated use IntegerDimEnc instead
 * @deprecated
 * 整数编码
 */
public class IntDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(IntDimEnc.class);

    private static final long[] CAP = { 0, 0xffL, 0xffffL, 0xffffffL, 0xffffffffL, 0xffffffffffL, 0xffffffffffffL, 0xffffffffffffffL, Long.MAX_VALUE };//每一个字节位置的最大长度.比如固定长度是3,则不允许超过第3个位置对应的整数

    public static final String ENCODING_NAME = "int";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new IntDimEnc(Integer.parseInt(args[0]));
        }
    };

    // ============================================================================

    private int fixedLen;//固定长度

    transient private int avoidVerbose = 0;

    //no-arg constructor is required for Externalizable
    public IntDimEnc() {
    }

    public IntDimEnc(int len) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException();

        this.fixedLen = len;
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    //对value字节数组进行编码,输出到output中,value的有效字节长度是valueLen
    @Override
    public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);//写入固定长度的null字节数组
            return;
        }

        encode(Bytes.toString(value, 0, valueLen), output, outputOffset);//将value转化成字符串,然后输出
    }

    //将字符串进行输出
    void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);//写入固定长度的null字节数组
            return;
        }

        long integer = Long.parseLong(valueStr);//最多允许long类型
        if (integer > CAP[fixedLen]) {
            if (avoidVerbose++ % 10000 == 0) {//超出范围
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueStr + ", will truncate, hit times:" + avoidVerbose);
            }
        }

        BytesUtil.writeLong(integer, output, outputOffset, fixedLen);//写入固定长度字节到输出中
    }

    //解码
    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        long integer = BytesUtil.readLong(bytes, offset, len);
        return String.valueOf(integer);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new IntegerSerializer();
    }

    public class IntegerSerializer extends DataTypeSerializer<Object> {
        // be thread-safe and avoid repeated obj creation
        private ThreadLocal<byte[]> current = new ThreadLocal<byte[]>();

        private byte[] currentBuf() {
            byte[] buf = current.get();
            if (buf == null) {
                buf = new byte[fixedLen];
                current.set(buf);
            }
            return buf;
        }

        @Override
        public void serialize(Object value, ByteBuffer out) {
            byte[] buf = currentBuf();
            String valueStr = value == null ? null : value.toString();
            encode(valueStr, buf, 0);
            out.put(buf);
        }

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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IntDimEnc that = (IntDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }
}
