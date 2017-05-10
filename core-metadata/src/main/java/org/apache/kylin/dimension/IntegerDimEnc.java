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
 * replacement for IntegerDimEnc, the diff is VLongDimEnc supports negative values
 * 对一个long类型的整数进行编码,支持负数
 *
 * 该编码方式是对int使用vlong方式进行编码,节省空间,但是其实不如bitSet方式好
 */
public class IntegerDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(IntegerDimEnc.class);

    /**
     0,7个1，15个1，23个1,31个1,39个1,47个1,55个1,63个1
     因为是long,因此最多8个字节

     具体的值表示0,127,32767,8388607,2147483647,549755813887,140737488355327,36028797018963967,9223372036854775807
     */
    private static final long[] CAP = { 0, 0x7fL, 0x7fffL, 0x7fffffL, 0x7fffffffL, 0x7fffffffffL, 0x7fffffffffffL, 0x7fffffffffffffL, 0x7fffffffffffffffL };
    /**
     0,8个1,16个1,24个1,32个1,40个1,48个1,56个1,66个1

     具体的值表示0,255,65535,16777215,4294967295,1099511627775,281474976710655,72057594037927935,-1
     */
    private static final long[] MASK = { 0, 0xffL, 0xffffL, 0xffffffL, 0xffffffffL, 0xffffffffffL, 0xffffffffffffL, 0xffffffffffffffL, 0xffffffffffffffffL };
    /**
     0,8个0,16个0,24个0,32个0,40个0,48个0,56个0,66个0

     具体的值表示0,128,32768,8388608,2147483648,549755813888,140737488355328,36028797018963968,-9223372036854775808
     */
    private static final long[] TAIL = { 0, 0x80L, 0x8000L, 0x800000L, 0x80000000L, 0x8000000000L, 0x800000000000L, 0x80000000000000L, 0x8000000000000000L };

    /**
     0
     1111111111111111111111111111111111111111111111111111111110000000
     1111111111111111111111111111111111111111111111111000000000000000
     1111111111111111111111111111111111111111100000000000000000000000
     1111111111111111111111111111111110000000000000000000000000000000
     1111111111111111111111111000000000000000000000000000000000000000
     1111111111111111100000000000000000000000000000000000000000000000
     1111111110000000000000000000000000000000000000000000000000000000
     1000000000000000000000000000000000000000000000000000000000000000
     初始化之后TAIL都变成64位的,只是值二进制后是以上内容

     具体的值表示0,-128,-32768,-8388608,-2147483648,-549755813888,-140737488355328,-36028797018963968,-9223372036854775808
     */
    static {
        for (int i = 1; i < TAIL.length; ++i) {
            long head = ~MASK[i];
            TAIL[i] = head | TAIL[i];
        }
    }

    public static final String ENCODING_NAME = "integer";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new IntegerDimEnc(Integer.parseInt(args[0]));
        }
    };

    // ============================================================================

    private int fixedLen;

    transient private int avoidVerbose = 0;
    transient private int avoidVerbose2 = 0;

    //no-arg constructor is required for Externalizable
    public IntegerDimEnc() {
    }

    //表示int的范围最多不会超过几个字节
    public IntegerDimEnc(int len) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException();

        this.fixedLen = len;
    }

    //返回固定存储该int对应的字节
    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);//设置固定字节的null
            return;
        }

        encode(Bytes.toString(value, 0, valueLen), output, outputOffset);//将字节数组转换成字符串,然后在输出
    }

    //将字符串进行输出
    void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);//设置固定字节的null
            return;
        }

        long integer = Long.parseLong(valueStr);//将value字符串转换成整数
        if (integer > CAP[fixedLen] || integer < TAIL[fixedLen]) {//如果fixedLen为4,说明4个字节存储的long的范围,即[-xxx,+xxxx]
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueStr + ", will truncate, hit times:" + avoidVerbose);
            }
        }

        if (integer == TAIL[fixedLen]) {
            if (avoidVerbose2++ % 10000 == 0) {
                logger.warn("Value " + valueStr + " does not fit into " + fixedLen + " bytes ");
            }
        }

        BytesUtil.writeLong(integer + CAP[fixedLen], output, outputOffset, fixedLen);//apply an offset to preserve binary order, overflow is okay
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        long integer = BytesUtil.readLong(bytes, offset, len) - CAP[fixedLen];

        //only take useful bytes
        integer = integer & MASK[fixedLen];
        boolean positive = (integer & ((0x80) << ((fixedLen - 1) << 3))) == 0;
        if (!positive) {
            integer |= (~MASK[fixedLen]);
        }

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

        IntegerDimEnc that = (IntegerDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }
}
