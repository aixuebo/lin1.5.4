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

import static org.apache.kylin.common.util.DateFormat.DEFAULT_DATE_PATTERN;
import static org.apache.kylin.common.util.DateFormat.dateToString;
import static org.apache.kylin.common.util.DateFormat.stringToDate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.DateDimEnc;

/**
 * A dictionary for date string (date only, no time).
 * 仅仅是日期,不包含时间
 * Dates are numbered from 0000-1-1 -- 0 for "0000-1-1", 1 for "0000-1-2", 2 for "0000-1-3" and
 * up to 3652426 for "9999-12-31".
 * 
 * Note the implementation is not thread-safe.
 * 这个实现不是线程安全的
 *
 * @author yangli9
 * 将date形式转换成int天的转换
 */
@SuppressWarnings("serial")
public class DateStrDictionary extends Dictionary<String> {

    private String pattern;//日期格式,默认是yyyy-MM-dd
    private int baseId;
    private int maxId;

    //默认日期格式是yyyy-MM-dd
    public DateStrDictionary() {
        init(DEFAULT_DATE_PATTERN, 0);
    }

    public DateStrDictionary(String datePattern, int baseId) {
        init(datePattern, baseId);
    }

    private void init(String datePattern, int baseId) {
        this.pattern = datePattern;
        this.baseId = baseId;
        this.maxId = baseId + DateDimEnc.ID_9999_12_31;//总共容纳多少天
    }

    @Override
    public int getMinId() {
        return baseId;
    }

    @Override
    public int getMaxId() {
        return maxId;
    }

    //估算该id占用多少字节---3个字节是24位,2的24次方=16777216,完全容纳3652426个元素了
    @Override
    public int getSizeOfId() {
        return 3;
    }

    //key未压缩前占用多少字节,即跟匹配模式有关系
    @Override
    public int getSizeOfValue() {
        return pattern.length();
    }

    @Override
    protected boolean isNullByteForm(byte[] value, int offset, int len) {
        return value == null || len == 0;
    }

    /**
     * 将日期转换成int
     * 转换规则是将日期转换成Date,从而知道时间戳,从而到得天数,即int
     * @param value
     * @param roundFlag
     * @return
     */
    @Override
    final protected int getIdFromValueImpl(String value, int roundFlag) {
        Date date = stringToDate(value, pattern);//字符串形式日期转换成Date对象
        int id = calcIdFromSeqNo((int) DateDimEnc.getNumOfDaysSince0000FromMillis(date.getTime()));
        if (id < baseId || id > maxId)
            throw new IllegalArgumentException("'" + value + "' encodes to '" + id + "' which is out of range [" + baseId + "," + maxId + "]");

        return id;
    }

    //将天数转换成字符串日期
    @Override
    final protected String getValueFromIdImpl(int id) {
        if (id < baseId || id > maxId)
            throw new IllegalArgumentException("ID '" + id + "' is out of range [" + baseId + "," + maxId + "]");
        long millis = DateDimEnc.getMillisFromNumOfDaysSince0000(calcSeqNoFromId(id));
        return dateToString(new Date(millis), pattern);
    }

    //从value字节数组中获取date,转换成int天
    @Override
    final protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        try {
            return getIdFromValue(new String(value, offset, len, "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
    }

    //将int转换成date,然后在转换成字节数组
    @Override
    final protected byte[] getValueBytesFromIdImpl(int id) {
        String date = getValueFromId(id);
        byte[] bytes;
        try {
            bytes = date.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
        return bytes;
    }

    //将id的天数转换成String的date对应的字节数组,追加到returnValue里面,从returnValue的offset位置开始追加写
    @Override
    final protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        byte[] bytes = getValueBytesFromIdImpl(id);//将int转换成date,然后在转换成字节数组
        System.arraycopy(bytes, 0, returnValue, offset, bytes.length);
        return bytes.length;
    }

    private int calcIdFromSeqNo(int seq) {
        return seq < 0 ? seq : baseId + seq;
    }

    private int calcSeqNoFromId(int id) {
        return id - baseId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pattern);
        out.writeInt(baseId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String pattern = in.readUTF();
        int baseId = in.readInt();
        init(pattern, baseId);
    }

    @Override
    public int hashCode() {
        return 31 * baseId + pattern.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof DateStrDictionary) == false)
            return false;
        DateStrDictionary that = (DateStrDictionary) o;
        return StringUtils.equals(this.pattern, that.pattern) && this.baseId == that.baseId;
    }

    @Override
    public boolean contains(Dictionary<?> other) {
        return this.equals(other);
    }

    @Override
    public void dump(PrintStream out) {
        out.println(this.toString());
    }

    @Override
    public String toString() {
        return "DateStrDictionary [pattern=" + pattern + ", baseId=" + baseId + "]";
    }

}
