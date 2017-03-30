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

package org.apache.kylin.common.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 代表如何将一行数据拆分成多列
 * 可以多设置一些列以及每一列多设置一些字节,避免每次都创建新的字节数组
 */
public class BytesSplitter {
    private static final Logger logger = LoggerFactory.getLogger(BytesSplitter.class);

    //公共的分隔符
    private static final int[] COMMON_DELIMS = new int[] { "\177".codePointAt(0), "|".codePointAt(0), "\t".codePointAt(0), ",".codePointAt(0) };

    private SplittedBytes[] splitBuffers;//每一列对应一个该对象
    private int bufferSize;//该行一共有多少列

    public SplittedBytes[] getSplitBuffers() {
        return splitBuffers;
    }

    //返回每一列对应的数据内容,从0开始计算
    public SplittedBytes getSplitBuffer(int index) {
        return splitBuffers[index];
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * 初始化一行数据
     * @param splitLen 有多少列
     * @param bytesLen 每一列默认的字节数组大小
     */
    public BytesSplitter(int splitLen, int bytesLen) {
        this.splitBuffers = new SplittedBytes[splitLen];
        for (int i = 0; i < splitLen; i++) {
            this.splitBuffers[i] = new SplittedBytes(bytesLen);
        }
        this.bufferSize = 0;
    }

    /**
     * 对一行数据进行拆分成列
     * @param bytes 一行数据对应的字节数组集合
     * @param byteLen 一行数据内容所占用的字节大小总和
     * @param delimiter 列的拆分字节
     * @return 返回一共拆分了多少列
     */
    public int split(byte[] bytes, int byteLen, byte delimiter) {
        this.bufferSize = 0;
        int offset = 0;
        int length = 0;//该列的字节长度
        for (int i = 0; i < byteLen; i++) {
            if (bytes[i] == delimiter) {//说明是拆分字节
                SplittedBytes split = this.splitBuffers[this.bufferSize++];//返回一列对象
                if (length > split.value.length) {//扩容
                    length = split.value.length;
                }
                System.arraycopy(bytes, offset, split.value, 0, length);//像该列对象添加数据,数据从bytes中的offset位置开始获取,获取length个字节,存储到列的value字节数组中
                split.length = length;
                offset = i + 1;
                length = 0;
            } else {
                length++;
            }
        }
        //设置最后一列
        SplittedBytes split = this.splitBuffers[this.bufferSize++];
        if (length > split.value.length) {
            length = split.value.length;
        }
        System.arraycopy(bytes, offset, split.value, 0, length);
        split.length = length;

        return bufferSize;
    }

    //一行数据转换成字节数组
    public void setBuffers(byte[][] buffers) {
        for (int i = 0; i < buffers.length; i++) {//循环每一列
            splitBuffers[i].value = buffers[i];
            splitBuffers[i].length = buffers[i].length;
        }
        this.bufferSize = buffers.length;
    }

    //将每一列的信息打印成字符串
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[");
        for (int i = 0; i < bufferSize; i++) {
            if (i > 0)
                buf.append(", ");

            buf.append(Bytes.toString(splitBuffers[i].value, 0, splitBuffers[i].length));
        }
        return buf.toString();
    }

    //将一行数据字节数组bytes进行拆分成列,每一列的数据存储到list中---按照顺序
    public static List<String> splitToString(byte[] bytes, int offset, byte delimiter) {
        List<String> splitStrings = new ArrayList<String>();
        int splitOffset = 0;//整行数据对应的偏移量
        int splitLength = 0;//该列有多少个字节
        for (int i = offset; i < bytes.length; i++) {
            if (bytes[i] == delimiter) {//说明遇到列的拆分符号了
                String str = Bytes.toString(bytes, splitOffset, splitLength);
                splitStrings.add(str);
                splitOffset = i + 1;
                splitLength = 0;
            } else {
                splitLength++;
            }
        }
        String str = Bytes.toString(bytes, splitOffset, splitLength);
        splitStrings.add(str);
        return splitStrings;
    }

}
