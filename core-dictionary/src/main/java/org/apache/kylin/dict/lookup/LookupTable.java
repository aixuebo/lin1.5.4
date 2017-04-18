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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableReader;

import com.google.common.collect.Sets;

/**
 * An in-memory lookup table, in which each cell is an object of type T. The
 * table is indexed by specified PK for fast lookup.
 * 在内存中读取look up表中数据内容
 * @author yangli9
 */
abstract public class LookupTable<T> {

    protected TableDesc tableDesc;//look up表对象
    protected String[] keyColumns;//表中作为join的key的列集合
    protected ReadableTable table;//如何读取该表的数据

    //存储该lookup表中 所有数据,Array对象是key的集合,即join时候on语法相关联的key对应的真实值的集合,value数组就是该表内所有的列数据集合
    protected ConcurrentHashMap<Array<T>, T[]> data;

    public LookupTable(TableDesc tableDesc, String[] keyColumns, ReadableTable table) throws IOException {
        this.tableDesc = tableDesc;
        this.keyColumns = keyColumns;
        this.table = table;
        this.data = new ConcurrentHashMap<Array<T>, T[]>();
        init();
    }

    protected void init() throws IOException {
        int[] keyIndex = new int[keyColumns.length];//key列对应的表中序号
        for (int i = 0; i < keyColumns.length; i++) {
            keyIndex[i] = tableDesc.findColumnByName(keyColumns[i]).getZeroBasedIndex();
        }

        //读取表内数据
        TableReader reader = table.getReader();
        try {
            while (reader.next()) {
                initRow(reader.getRow(), keyIndex);
            }
        } finally {
            reader.close();
        }
    }

    /**
     * @param cols 一行数据中所有的列
     * @param keyIndex join中on相关的key字段所在index集合
     */
    @SuppressWarnings("unchecked")
    private void initRow(String[] cols, int[] keyIndex) {
        T[] value = convertRow(cols);//将每一个列的数据转换成String类型
        T[] keyCols = (T[]) java.lang.reflect.Array.newInstance(getType(), keyIndex.length);//key对应的数据类型
        for (int i = 0; i < keyCols.length; i++)
            keyCols[i] = value[keyIndex[i]];//获取join时候on对应的String具体值

        Array<T> key = new Array<T>(keyCols);

        //说明key已经存在在look up表了,相当于复合主键冲突,因此抛异常,将冲突的数据和现在的数据都打印出来
        if (data.containsKey(key))
            throw new IllegalStateException("Dup key found, key=" + toString(keyCols) + ", value1=" + toString(data.get(key)) + ", value2=" + toString(value));

        //初始化表内数据内容
        data.put(key, value);
    }

    abstract protected T[] convertRow(String[] cols);

    //获取key对应的一行数据内容
    public T[] getRow(Array<T> key) {
        return data.get(key);
    }

    //获取表内所有的数据内容
    public Collection<T[]> getAllRows() {
        return data.values();
    }

    /**
     * 该方法很耗时
     * 扫描col列所有的数据,如果该数据在values中存在,则将returnCol列此时真实的值收集起来,一起返回
     */
    public List<T> scan(String col, List<T> values, String returnCol) {
        ArrayList<T> result = new ArrayList<T>();
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();//找到列的下标
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();//找到列的下标
        for (T[] row : data.values()) {//循环所有的表内数据
            if (values.contains(row[colIdx]))//该列数据在values中存在
                result.add(row[returnIdx]);//添加returnCol列对应的值
        }
        return result;
    }

    /**
     * 与scan相同,只是返回的是Set集合
     */
    public Set<T> mapValues(String col, Set<T> values, String returnCol) {
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();//找到列的下标
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();//找到列的下标
        Set<T> result = Sets.newHashSetWithExpectedSize(values.size());
        for (T[] row : data.values()) {//循环所有的表内数据
            if (values.contains(row[colIdx])) {
                result.add(row[returnIdx]);
            }
        }
        return result;
    }

    /**
     * 获取表的col属性在[beginValue,endValue]之间的数据,如何符合,则找到对应的returnCol列数据值
     * 最终返回returnCol列的最大值和最小值元组
     */
    public Pair<T, T> mapRange(String col, T beginValue, T endValue, String returnCol) {
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();//找到列的下标
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();//找到列的下标

        //获取该列对应的排序算法
        Comparator<T> colComp = getComparator(colIdx);
        Comparator<T> returnComp = getComparator(returnIdx);

        T returnBegin = null;
        T returnEnd = null;
        for (T[] row : data.values()) {//循环所有的表内数据
            if (between(beginValue, row[colIdx], endValue, colComp)) {//如果该值在范围内
                T returnValue = row[returnIdx];//需要的列的具体的值

                //获取最大值和最小值具体的值
                if (returnBegin == null || returnComp.compare(returnValue, returnBegin) < 0) {
                    returnBegin = returnValue;
                }
                if (returnEnd == null || returnComp.compare(returnValue, returnEnd) > 0) {
                    returnEnd = returnValue;
                }
            }
        }
        if (returnBegin == null && returnEnd == null)
            return null;
        else
            return Pair.newPair(returnBegin, returnEnd);
    }

    /**
     * true表示 数据v在[beginValue,endValue]之间
     */
    private boolean between(T beginValue, T v, T endValue, Comparator<T> comp) {
        return (beginValue == null || comp.compare(beginValue, v) <= 0) && (endValue == null || comp.compare(v, endValue) <= 0);
    }

    abstract protected Comparator<T> getComparator(int colIdx);

    public String toString() {
        return "LookupTable [path=" + table + "]";
    }

    //打印一行数据
    protected String toString(T[] cols) {
        StringBuilder b = new StringBuilder();
        b.append("[");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0)
                b.append(",");
            b.append(toString(cols[i]));
        }
        b.append("]");
        return b.toString();
    }

    abstract protected String toString(T cell);

    abstract public Class<?> getType();

    //打印表内所有的数据,该方法耗时并且输出较多
    public void dump() {
        for (Array<T> key : data.keySet()) {
            System.out.println(toString(key.data) + " => " + toString(data.get(key)));
        }
    }

}
