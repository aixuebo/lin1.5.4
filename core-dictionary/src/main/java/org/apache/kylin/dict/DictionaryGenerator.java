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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author yangli9
 * 字典的构造器,如何构建一个字典,根据字段的类型不同,构建不同的字典
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DictionaryGenerator {

    private static final int DICT_MAX_CARDINALITY = getDictionaryMaxCardinality();

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGenerator.class);

    private static final String[] DATE_PATTERNS = new String[] { "yyyy-MM-dd", "yyyyMMdd" };//日期格式

    //加载字典的最多允许多少个不同的值存在---仅仅限制TrieDictionary类型的字典
    private static int getDictionaryMaxCardinality() {
        try {
            return KylinConfig.getInstanceFromEnv().getDictionaryMaxCardinality();
        } catch (Throwable e) {
            return 30000000; // some test case does not have KylinConfig setup properly
        }
    }

    /**
     * 构建一个字典
     * @param dataType 列的类型
     * @param valueEnumerator 如何读取该列的数据
     */
    public static Dictionary<String> buildDictionary(DataType dataType, IDictionaryValueEnumerator valueEnumerator) throws IOException {
        Preconditions.checkNotNull(dataType, "dataType cannot be null");

        // build dict, case by data type
        IDictionaryBuilder builder;
        if (dataType.isDateTimeFamily()) {//时间类型
            if (dataType.isDate())
                builder = new DateDictBuilder();
            else
                builder = new TimeDictBuilder();
        } else if (dataType.isNumberFamily()) {//数字类型
            builder = new NumberDictBuilder();
        } else {
            builder = new StringDictBuilder();//字符串类型
        }

        return buildDictionary(builder, null, valueEnumerator);
    }

    /**
     * 构建一个字典
     * @param builder 字典类
     * @param dictInfo 字典描述信息
     * @param valueEnumerator 如何读取该字典的列的数据
     */
    public static Dictionary<String> buildDictionary(IDictionaryBuilder builder, DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator) throws IOException {
        int baseId = 0; // always 0 for now
        int nSamples = 5;
        ArrayList<String> samples = new ArrayList<String>(nSamples);

        Dictionary<String> dict = builder.build(dictInfo, valueEnumerator, baseId, nSamples, samples);//真正产生字典

        // log a few samples 打印抽样数据内容
        StringBuilder buf = new StringBuilder();
        for (String s : samples) {
            if (buf.length() > 0) {
                buf.append(", ");
            }
            buf.append(s.toString()).append("=>").append(dict.getIdFromValue(s));
        }
        logger.debug("Dictionary value samples: " + buf.toString());
        logger.debug("Dictionary cardinality: " + dict.getSize());
        logger.debug("Dictionary builder class: " + builder.getClass().getName());
        logger.debug("Dictionary class: " + dict.getClass().getName());
        if (dict instanceof TrieDictionary && dict.getSize() > DICT_MAX_CARDINALITY) {//说明字典的值太多了
            throw new IllegalArgumentException("Too high cardinality is not suitable for dictionary -- cardinality: " + dict.getSize());
        }
        return dict;
    }

    //读取多个字典文件 进行合并
    public static Dictionary mergeDictionaries(DataType dataType, List<DictionaryInfo> sourceDicts) throws IOException {
        return buildDictionary(dataType, new MultipleDictionaryValueEnumerator(sourceDicts));
    }

    //存储Date类型数据的字典
    private static class DateDictBuilder implements IDictionaryBuilder {
        @Override
        public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
            final int BAD_THRESHOLD = 0;
            String matchPattern = null;//日期格式
            byte[] value;

            for (String ptn : DATE_PATTERNS) {//循环日期格式
                matchPattern = ptn; // be optimistic
                int badCount = 0;//解析失败的次数
                SimpleDateFormat sdf = new SimpleDateFormat(ptn);
                while (valueEnumerator.moveNext()) {
                    value = valueEnumerator.current();
                    if (value == null || value.length == 0)
                        continue;

                    String str = Bytes.toString(value);
                    try {
                        sdf.parse(str);
                        if (returnSamples.size() < nSamples && returnSamples.contains(str) == false) //添加抽样数据
                            returnSamples.add(str);
                    } catch (ParseException e) {
                        logger.info("Unrecognized date value: " + str);
                        badCount++;
                        if (badCount > BAD_THRESHOLD) {
                            matchPattern = null;
                            break;
                        }
                    }
                }
                if (matchPattern != null) {
                    return new DateStrDictionary(matchPattern, baseId);
                }
            }

            throw new IllegalStateException("Unrecognized datetime value");
        }
    }

    //存储time类型的字典
    private static class TimeDictBuilder implements IDictionaryBuilder {
        @Override
        public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
            return new TimeStrDictionary(); // base ID is always 0
        }
    }

    //存储String类型的字典
    private static class StringDictBuilder implements IDictionaryBuilder {
        @Override
        public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
            TrieDictionaryBuilder builder = new TrieDictionaryBuilder(new StringBytesConverter());
            byte[] value;
            while (valueEnumerator.moveNext()) {//不断读取数据
                value = valueEnumerator.current();
                if (value == null)
                    continue;
                String v = Bytes.toString(value);
                builder.addValue(v);//添加该值
                if (returnSamples.size() < nSamples && !returnSamples.contains(v))//该值不再抽样集合内
                    returnSamples.add(v);//添加抽样数据
            }
            return builder.build(baseId);
        }
    }

    //存数数字类型的字典
    private static class NumberDictBuilder implements IDictionaryBuilder {
        @Override
        public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
            NumberDictionaryBuilder builder = new NumberDictionaryBuilder(new StringBytesConverter());
            byte[] value;
            while (valueEnumerator.moveNext()) {
                value = valueEnumerator.current();
                if (value == null)
                    continue;
                String v = Bytes.toString(value);
                if (StringUtils.isBlank(v)) // empty string is null for numbers
                    continue;

                builder.addValue(v);
                if (returnSamples.size() < nSamples && returnSamples.contains(v) == false)
                    returnSamples.add(v);
            }
            return builder.build(baseId);
        }
    }
}
