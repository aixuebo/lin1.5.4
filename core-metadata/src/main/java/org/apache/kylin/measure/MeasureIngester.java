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

package org.apache.kylin.measure;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * 如何对字典的内容进行编码
 */
abstract public class MeasureIngester<V> {

    public static MeasureIngester<?> create(MeasureDesc measure) {
        return measure.getFunction().getMeasureType().newIngester();
    }

    public static MeasureIngester<?>[] create(Collection<MeasureDesc> measures) {
        MeasureIngester<?>[] result = new MeasureIngester<?>[measures.size()];
        int i = 0;
        for (MeasureDesc measure : measures) {
            result[i++] = create(measure);
        }
        return result;
    }

    /**
     * @param values 该度量函数执行时候的参数集合,这里面参数已经转换成具体的值了,比如sum(account)此时account已经是具体的值了
     * @param measureDesc 度量对象
     * @param dictionaryMap 度量对象中需要的每一个列与字典的对应关系
     * 返回该属性此时具体的值
     */
    abstract public V valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap);

    /**
     * 用于合并merge操作
     * @param value 具体的一个值
     * @param measureDesc 执行的函数
     * @param oldDicts 老的字典
     * @param newDicts 新的字典
     * @return
     */
    public V reEncodeDictionary(V value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
        throw new UnsupportedOperationException();
    }
}
