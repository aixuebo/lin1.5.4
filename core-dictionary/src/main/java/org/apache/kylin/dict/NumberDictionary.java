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
 * 
 */
@SuppressWarnings("serial")
public class NumberDictionary<T> extends TrieDictionary<T> {

    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT_LEGACY = 16;
    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT = 19;//小数点之前最多允许多少个位置,即整数部分有多少位

    // encode a number into an order preserving byte sequence
    // for positives -- padding '0'
    // for negatives -- '-' sign, padding '9', invert digits, and terminate by ';'
    static class NumberBytesCodec {
        int maxDigitsBeforeDecimalPoint;
        byte[] buf;
        int bufOffset;//从buf中哪个偏移量开始
        int bufLen;//添加的数字占用多少字节

        NumberBytesCodec(int maxDigitsBeforeDecimalPoint) {
            this.maxDigitsBeforeDecimalPoint = maxDigitsBeforeDecimalPoint;
            this.buf = new byte[maxDigitsBeforeDecimalPoint * 3];
            this.bufOffset = 0;
            this.bufLen = 0;
        }

        void encodeNumber(byte[] value, int offset, int len) {
            if (len == 0) {
                bufOffset = 0;
                bufLen = 0;
                return;
            }

            if (len > buf.length) {
                throw new IllegalArgumentException("Too many digits for NumberDictionary: " + Bytes.toString(value, offset, len) + ". Internal buffer is only " + buf.length + " bytes");
            }

            boolean negative = value[offset] == '-';

            // terminate negative ';'
            int start = buf.length - len;
            int end = buf.length;
            if (negative) {//因为是负数,因此最后一个位置设置;表示是负数,因此start和end都向前移动一个位置,即给负号提供一个坑
                start--;
                end--;
                buf[end] = ';';
            }

            // copy & find decimal point
            int decimalPoint = end;
            //i是buffer的指针,j是value的指针
            for (int i = start, j = offset; i < end; i++, j++) {
                buf[i] = value[j];
                if (buf[i] == '.' && i < decimalPoint) {//说明遇到小数点了,后面的条件说明是第一次遇见小数点
                    decimalPoint = i;//设置小数点在buffer中的位置
                }
            }
            // remove '-' sign
            if (negative) {//将start位置移动一下,即让负号位置移动开
                start++;
            }

            // prepend '0'
            //decimalPoint - start表示整数部分占用多少位置
            //maxDigitsBeforeDecimalPoint - 整数部分,就是整数部分要补充多少个0
            int nZeroPadding = maxDigitsBeforeDecimalPoint - (decimalPoint - start);
            if (nZeroPadding < 0 || nZeroPadding + 1 > start) // < 0说明整数部分太多了, nZeroPadding + 1 > start 说明小数部分太多了,没有多余的位置给整数部分补充0了
                throw new IllegalArgumentException("Too many digits for NumberDictionary: " + Bytes.toString(value, offset, len) + ". Expect " + maxDigitsBeforeDecimalPoint + " digits before decimal point at max.");
            for (int i = 0; i < nZeroPadding; i++) {//补充0
                buf[--start] = '0';
            }

            // consider negative
            if (negative) {
                buf[--start] = '-';//添加负号
                for (int i = start + 1; i < buf.length; i++) {//start + 1 已经忽略了负号
                    int c = buf[i];
                    if (c >= '0' && c <= '9') {
                        buf[i] = (byte) ('9' - (c - '0'));
                    }
                }
            } else {
                buf[--start] = '0';//正数,用0填补
            }

            bufOffset = start;//重新设置开始位置
            bufLen = buf.length - start;//添加的数字占用多少字节
        }

        int decodeNumber(byte[] returnValue, int offset) {
            if (bufLen == 0) {
                return 0;
            }

            int in = bufOffset;
            int end = bufOffset + bufLen;
            int out = offset;

            // sign
            boolean negative = buf[in] == '-';//说明是负数
            if (negative) {
                returnValue[out++] = '-';//添加负号
                in++;//start位置移动一个
                end--;//最后一个位置;要忽略
            }

            // remove padding
            byte padding = (byte) (negative ? '9' : '0'); //负数补充的位置是9,正数补充的位置是0
            for (; in < end; in++) {//过滤掉补充的位置
                if (buf[in] != padding)
                    break;
            }

            // all paddings before '.', special case for '0'
            //in == end 说明是正数,没有小数,并且是0,
            //!(buf[in] >= '0' && buf[in] <= '9')  后面接的正数部分一定是数字,如果不是数字,则直接填写0,说明是0.xxx,即接下来存储的可能是.也可能是E
            if (in == end || !(buf[in] >= '0' && buf[in] <= '9')) {
                returnValue[out++] = '0';
            }

            // copy the rest
            if (negative) {//负数的时候
                for (; in < end; in++, out++) {//循环剩余的内容
                    int c = buf[in];
                    if (c >= '0' && c <= '9') {//如果是数字,写如该数字
                        c = '9' - (c - '0');
                    }
                    returnValue[out] = (byte) c;//如果不是数字,则直接写入内容即可
                }
            } else {//说明是非负数
                System.arraycopy(buf, in, returnValue, out, end - in);//将buf的内容.直接复制到returnValue中即可
                out += end - in;
            }

            return out - offset;//最终占用多少字节
        }
    }

    static ThreadLocal<NumberBytesCodec> localCodec = new ThreadLocal<NumberBytesCodec>();

    // ============================================================================

    public NumberDictionary() { // default constructor for Writable interface
        super();
    }

    public NumberDictionary(byte[] trieBytes) {
        super(trieBytes);
    }

    protected NumberBytesCodec getCodec() {
        NumberBytesCodec codec = localCodec.get();
        if (codec == null) {
            codec = new NumberBytesCodec(MAX_DIGITS_BEFORE_DECIMAL_POINT_LEGACY);
            localCodec.set(codec);
        }
        return codec;
    }

    @Override
    protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        NumberBytesCodec codec = getCodec();
        codec.encodeNumber(value, offset, len);//对value字节数组编码
        return super.getIdFromValueBytesImpl(codec.buf, codec.bufOffset, codec.bufLen, roundingFlag);
    }

    @Override
    protected boolean isNullObjectForm(T value) {
        return value == null || value.equals("");
    }

    @Override
    protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        NumberBytesCodec codec = getCodec();
        codec.bufOffset = 0;
        codec.bufLen = super.getValueBytesFromIdImpl(id, codec.buf, 0);
        return codec.decodeNumber(returnValue, offset);
    }

    @Override
    public void enableIdToValueBytesCache() {
        enableIdToValueBytesCache(new EnableIdToValueBytesCacheVisitor() {
            NumberBytesCodec codec = getCodec();
            byte[] tmp = new byte[getSizeOfValue()];

            @Override
            public byte[] getBuffer() {
                return codec.buf;
            }

            @Override
            public byte[] makeValueBytes(byte[] buf, int length) {
                // the given buf is the codec buf, which we returned in getBuffer()
                codec.bufOffset = 0;
                codec.bufLen = length;
                int numLen = codec.decodeNumber(tmp, 0);

                byte[] result = new byte[numLen];
                System.arraycopy(tmp, 0, result, 0, numLen);
                return result;
            }
        });
    }

    public static void main(String[] args) throws Exception {
        NumberDictionaryBuilder<String> b = new NumberDictionaryBuilder<String>(new StringBytesConverter());
        b.addValue("-10.02.100ab00");//这样也是可以被存储进去的,因为没有校验操作
        b.addValue("-100");
        b.addValue("40");
        b.addValue("7");
        TrieDictionary<String> dict = b.build(0);

        dict.enableIdToValueBytesCache();
        for (int i = 0; i <= dict.getMaxId(); i++) {
            System.out.println(Bytes.toString(dict.getValueBytesFromId(i)));
        }
    }
}