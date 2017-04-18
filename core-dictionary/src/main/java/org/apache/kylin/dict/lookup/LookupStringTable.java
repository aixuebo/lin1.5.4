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
import java.util.Comparator;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;

/**
 * @author yangli9
 * 在内存中读取look up表中数据内容   表内的数据都转换成String类型
 */
public class LookupStringTable extends LookupTable<String> {

    //比较两个字符串是Date类型的,因为date类型就是long类型,因此比较的是long类型
    private static final Comparator<String> dateStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            long l1 = Long.parseLong(o1);
            long l2 = Long.parseLong(o2);
            return Long.compare(l1, l2);
        }
    };

    //比较两个数字类型字符串
    private static final Comparator<String> numStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            double d1 = Double.parseDouble(o1);
            double d2 = Double.parseDouble(o2);
            return Double.compare(d1, d2);
        }
    };

    //比较两个字符串
    private static final Comparator<String> defaultStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };

    //表中每一个列对应一个位置,true表示是日期或者数字类型,false表示该字段不是日期或者数字类型
    boolean[] colIsDateTime;
    boolean[] colIsNumber;

    /**
     * @param tableDesc  look up表对象
     * @param keyColumns lookup表中作为join的key的列集合
     * @param table 如何读取该表的数据
     */
    public LookupStringTable(TableDesc tableDesc, String[] keyColumns, ReadableTable table) throws IOException {
        super(tableDesc, keyColumns, table);
    }

    @Override
    protected void init() throws IOException {
        ColumnDesc[] cols = tableDesc.getColumns();//该表的列的数量

        //初始化两个boolean数组
        colIsDateTime = new boolean[cols.length];
        colIsNumber = new boolean[cols.length];
        for (int i = 0; i < cols.length; i++) {//循环每一个列
            DataType t = cols[i].getType();//根据列的类型,设置该列是否是日期或者数字类型的列
            colIsDateTime[i] = t.isDateTimeFamily();
            colIsNumber[i] = t.isNumberFamily();
        }

        super.init();
    }

    //对列的数据进行转换成字符串
    @Override
    protected String[] convertRow(String[] cols) {
        for (int i = 0; i < cols.length; i++) {
            if (colIsDateTime[i]) {//如果该列是日期类型的
                cols[i] = String.valueOf(DateFormat.stringToMillis(cols[i]));//则将该列转换成日期的格式化字符串
            }
        }
        return cols;
    }

    /**
     * @param idx 参数是列的序号
     * @return 通过列是什么类型的,返回该列的比较方式
     */
    @Override
    protected Comparator<String> getComparator(int idx) {
        if (colIsDateTime[idx])
            return dateStrComparator;
        else if (colIsNumber[idx])
            return numStrComparator;
        else
            return defaultStrComparator;
    }

    @Override
    protected String toString(String cell) {
        return cell;
    }

    //每一列对应的转换后的数据类型
    public Class<?> getType() {
        return String.class;
    }

}
