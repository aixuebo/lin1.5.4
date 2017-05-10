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

import com.google.common.base.Preconditions;

/**
 * used to store hex values like "1A2BFF" 该类用于存储16进制的value,即value存储的字符串只能是由0-9A-F这16个字符组成
 * <p>
 * <p>
 * limitations: (take FixedLenHexDimEnc(2) as example: )
 * <p>
 * 1. "FFFF" will become null encode and decode
 * 2. "AB" will become "AB00"
 * <p>
 * Due to these limitations hex representation of hash values(with no padding, better with even characters) is more suitable
 * 使用16进制存储数据,因此一个字节8位,4个位置就可以容纳一个16进制,因此一个字节最终可以容纳2个16进制的字节
 *
 * 即输入的value是16进制的字节数组,因此可以压缩,将2个字节压缩成一个字节,最终输出
 *
 */
public class FixedLenHexDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(FixedLenHexDimEnc.class);

    public static byte[] dict = new byte[256];//存储内容分别是16进制对应的数字,比如F对应的acsii位置存储的是15,即通过F就可以立刻获取到他是15
    public static byte[] revdict = new byte[16];//反转,即你给我1个16进制的数,我返回给你一个对应的编码,比如15对应F

    static {
        for (int i = 0; i < dict.length; i++) {//先初始化都是-1
            dict[i] = -1;
        }
        dict['0'] = 0;
        dict['1'] = 1;
        dict['2'] = 2;
        dict['3'] = 3;
        dict['4'] = 4;
        dict['5'] = 5;
        dict['6'] = 6;
        dict['7'] = 7;
        dict['8'] = 8;
        dict['9'] = 9;
        dict['A'] = 10;
        dict['B'] = 11;
        dict['C'] = 12;
        dict['D'] = 13;
        dict['E'] = 14;
        dict['F'] = 15;
        dict['a'] = 10;
        dict['b'] = 11;
        dict['c'] = 12;
        dict['d'] = 13;
        dict['e'] = 14;
        dict['f'] = 15;

        revdict[0] = '0';
        revdict[1] = '1';
        revdict[2] = '2';
        revdict[3] = '3';
        revdict[4] = '4';
        revdict[5] = '5';
        revdict[6] = '6';
        revdict[7] = '7';
        revdict[8] = '8';
        revdict[9] = '9';
        revdict[10] = 'A';
        revdict[11] = 'B';
        revdict[12] = 'C';
        revdict[13] = 'D';
        revdict[14] = 'E';
        revdict[15] = 'F';
    }

    // row key fixed length place holder 用0作为占位符
    public static final byte ROWKEY_PLACE_HOLDER_BYTE = 0;

    public static final String ENCODING_NAME = "fixed_length_hex";//编码名字

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new FixedLenHexDimEnc(Integer.parseInt(args[0]));
        }
    }

    // ============================================================================

    private int hexLength;//value的字节长度,因为字节内容是16进制的,比如hexLength=4,说明存储的value是由4个16进制字节数组组成的
    private int bytelen;//4个16进制的数据,只需要2个字节就可以存储到,因此编码后就需要2个字节

    transient private int avoidVerbose = 0;
    transient private int avoidVerbose2 = 0;

    //no-arg constructor is required for Externalizable
    public FixedLenHexDimEnc() {
    }

    /**
     * 参数是value值的大小,即创建字典的时候提供value的长度大小即可,可以推断出需要字典编码大小
     */
    public FixedLenHexDimEnc(int len) {
        if (len < 1) {
            throw new IllegalArgumentException("len has to be positive: " + len);
        }
        this.hexLength = len;//比如该值是4
        this.bytelen = (hexLength + 1) / 2;//因此该值是5/2= 2
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FixedLenHexDimEnc that = (FixedLenHexDimEnc) o;

        return hexLength == that.hexLength;
    }

    @Override
    public int hashCode() {
        return hexLength;
    }

    @Override
    public int getLengthOfEncoding() {
        return bytelen;
    }

    /**
     * 编码:将value写入到output中
     * @param value value是16进制的字节数组,即每一个字节只是16进制的一个数
     * @param valueLen value的字节长度
     * @param output
     * @param outputOffset  output的开始位置
     */
    @Override
    public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + bytelen, NULL);//因为是Null,因此设置null需要的字节数组
            return;
        }

        int endOffset = outputOffset + bytelen;

        if (valueLen > hexLength) {//不允许value长度大于设置的值
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + hexLength + " bytes, but got " + valueLen + ", will truncate, value string: " + Bytes.toString(value, 0, valueLen) + " times:" + avoidVerbose);
            }
        }

        if (valueLen >= hexLength && isF(value, 0, hexLength)) {
            if (avoidVerbose2++ % 10000 == 0) {
                logger.warn("All 'F' value: " + Bytes.toString(value, 0, valueLen) + "will become null after encode/decode. times:" + avoidVerbose);
            }
        }

        //循环每一个value的字节,虽然字节是byte,8位,但是他其实存储的是16进制的数据.因此只需要4位即可,即每次获取两个字节,可以输出到output中,2个字节输出成1个字节
        int n = Math.min(valueLen, hexLength);//对于value大于设置的长度的时候,要进行剪切
        for (int i = 0; i < n; i += 2) {
            byte temp = 0;
            byte iCode = dict[value[i]];//对应的第一个16进制字节
            temp |= (iCode << 4);

            int j = i + 1;
            if (j < n) {
                byte jCode = dict[value[j]];//对应的第二个16进制字节
                temp |= jCode;
            }

            output[outputOffset++] = temp;//两个字节组成新的字节
        }

        Arrays.fill(output, outputOffset, endOffset, ROWKEY_PLACE_HOLDER_BYTE);//用0占位,因为是固定字节长度,因此要占位
    }

    //解码
    @Override
    public String decode(byte[] bytes, int offset, int len) {
        Preconditions.checkArgument(len == bytelen, "len " + len + " not equals " + bytelen);//校验长度必须是固定的字节长度

        if (isNull(bytes, offset, len)) {//说明是null
            return null;
        }

        byte[] ret = new byte[hexLength];//解码后的字节数组
        for (int i = 0; i < ret.length; i += 2) {
            byte temp = bytes[i / 2];
            ret[i] = revdict[(temp & 0xF0) >>> 4];

            int j = i + 1;
            if (j < hexLength) {
                ret[j] = revdict[temp & 0x0F];
            }
        }

        return Bytes.toString(ret, 0, ret.length);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new FixedLenSerializer();
    }

    public class FixedLenSerializer extends DataTypeSerializer<Object> {
        // be thread-safe and avoid repeated obj creation
        private ThreadLocal<byte[]> current = new ThreadLocal<byte[]>();

        private byte[] currentBuf() {
            byte[] buf = current.get();
            if (buf == null) {
                buf = new byte[bytelen];
                current.set(buf);
            }
            return buf;
        }

        //value存储的字符串只能是由0-9A-F这16个字符组成
        @Override
        public void serialize(Object value, ByteBuffer out) {
            byte[] buf = currentBuf();
            byte[] bytes = value == null ? null : Bytes.toBytes(value.toString());
            encode(bytes, bytes == null ? 0 : bytes.length, buf, 0);
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
            return bytelen;
        }

        @Override
        public int maxLength() {
            return bytelen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return bytelen;
        }

        @Override
        public Object valueOf(String str) {
            return str;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(hexLength);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        hexLength = in.readShort();
    }

    //true表示value里面都是F,false表示肯定没有F字节存在
    private boolean isF(byte[] value, int offset, int length) {
        for (int i = offset; i < length + offset; ++i) {
            if (value[i] != 'F') {
                return false;
            }
        }
        return true;
    }

}
