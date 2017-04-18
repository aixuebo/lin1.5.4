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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author yangli9
 * 一个table的快照可能有多个版本,因此是对应多个快照文件的
 *
 * 快照的内容包括:表的内容以及表的元数据
 *
 * 非常占用内存,因为要将table内的数据都加载到字典表中,存放在内存中
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SnapshotTable extends RootPersistentEntity implements ReadableTable {

    @JsonProperty("tableName")
    private String tableName;//hive的table表名
    @JsonProperty("signature")
    private TableSignature signature;//快照该表时候,该表的情况,比如最后提交时间等信息
    @JsonProperty("useDictionary")
    private boolean useDictionary;//true表示序列化的时候要将字典也序列化到磁盘

    //序列化和反序列化下面两个字段是很耗时的,因此有时候可能不需要序列化这两个字段
    private ArrayList<int[]> rowIndices;//存储每一行每一列数据对应的字典
    private Dictionary<String> dict;//字典,用于存储表中所有的字段的值

    // default constructor for JSON serialization
    public SnapshotTable() {
    }

    SnapshotTable(ReadableTable table, String tableName) throws IOException {
        this.tableName = tableName;
        this.signature = table.getSignature();
        this.useDictionary = true;
    }

    public void takeSnapshot(ReadableTable table, TableDesc tableDesc) throws IOException {
        this.signature = table.getSignature();

        int maxIndex = tableDesc.getMaxColumnIndex();//该table的最大列ID

        TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<String>(new StringBytesConverter());//构造字典树---存储表中所有的字段的值

        TableReader reader = table.getReader();//读取该table对应的数据
        try {
            while (reader.next()) {
                String[] row = reader.getRow();//数据内容
                if (row.length <= maxIndex) {
                    throw new IllegalStateException("Bad hive table row, " + tableDesc + " expect " + (maxIndex + 1) + " columns, but got " + Arrays.toString(row));
                }
                for (ColumnDesc column : tableDesc.getColumns()) {//循环每一个列对象
                    String cell = row[column.getZeroBasedIndex()];//获取该列对象的值
                    if (cell != null)
                        b.addValue(cell);//添加该列对象的值到字典中
                }
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }

        this.dict = b.build(0);

        ArrayList<int[]> allRowIndices = new ArrayList<int[]>();//存储每一行每一列数据对应的字典
        reader = table.getReader();//再次读取数据
        try {
            while (reader.next()) {
                String[] row = reader.getRow();
                int[] rowIndex = new int[tableDesc.getColumnCount()];//表中列数量+1,用于存储该行每一个列的值对应的字典
                for (ColumnDesc column : tableDesc.getColumns()) {//循环每一个列对象
                    /**
                     * 等式左边
                     * 1.column.getZeroBasedIndex() 表示该列是第几个列
                     * 2.rowIndex[index]表示存储该列对应的字典
                     * 等式右边,
                     * 1.row[column.getZeroBasedIndex()返回该列具体的值
                     * 2.dict.getIdFromValue 将该列具体的value进行转换压缩成int字典
                     */
                    rowIndex[column.getZeroBasedIndex()] = dict.getIdFromValue(row[column.getZeroBasedIndex()]);
                }
                allRowIndices.add(rowIndex);
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }

        this.rowIndices = allRowIndices;
    }

    //快照文件存储路径
    public String getResourcePath() {
        return getResourceDir() + "/" + uuid + ".snapshot";
    }

    //快照文件存储路径
    public String getResourceDir() {
        if (Strings.isNullOrEmpty(tableName)) {
            return getOldResourceDir();
        } else {
            return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + tableName;
        }
    }

    //快照文件存储路径
    private String getOldResourceDir() {
        return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + new File(signature.getPath()).getName();
    }

    //读取该表内的数据----通过内建的字典去获取原始数据
    @Override
    public TableReader getReader() throws IOException {
        return new TableReader() {

            int i = -1;//读取到第几行数据了

            @Override
            public boolean next() throws IOException {
                i++;
                return i < rowIndices.size();//只要行数没到,一定就可以继续读下一行数据
            }

            @Override
            public String[] getRow() {
                int[] rowIndex = rowIndices.get(i);//获取该行数据的每一列对应的字典
                String[] row = new String[rowIndex.length];//每一列的字典对应的具体值数组集合
                for (int x = 0; x < row.length; x++) {
                    row[x] = dict.getValueFromId(rowIndex[x]);//通过具体的字典id,反转成具体的值
                }
                return row;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return signature;
    }

    /**
     * a naive implementation
     *
     * @return
     */
    @Override
    public int hashCode() {
        int[] parts = new int[this.rowIndices.size()];
        for (int i = 0; i < parts.length; ++i)
            parts[i] = Arrays.hashCode(this.rowIndices.get(i));
        return Arrays.hashCode(parts);
    }

    /**
     * 1.字典内容相同,即文件内容相同
     * 2.记录的行数相同
     * 3.记录每一行每一列的字典ID相同
     */
    @Override
    public boolean equals(Object o) {
        if ((o instanceof SnapshotTable) == false)
            return false;
        SnapshotTable that = (SnapshotTable) o;

        if (this.dict.equals(that.dict) == false)
            return false;

        //compare row by row
        if (this.rowIndices.size() != that.rowIndices.size())
            return false;
        for (int i = 0; i < this.rowIndices.size(); ++i) {
            if (!ArrayUtils.isEquals(this.rowIndices.get(i), that.rowIndices.get(i)))
                return false;
        }

        return true;
    }

    private static String NULL_STR;
    {
        try {
            // a special placeholder to indicate a NULL; 0, 9, 127, 255 are a few invisible ASCII characters
            //表示null的字节数组对应的字符串
            NULL_STR = new String(new byte[] { 0, 9, 127, (byte) 255 }, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            // does not happen
        }
    }

    //序列化输出
    void writeData(DataOutput out) throws IOException {
        out.writeInt(rowIndices.size());//行数
        if (rowIndices.size() > 0) {
            int n = rowIndices.get(0).length;
            out.writeInt(n);//列数

            if (this.useDictionary == true) {
                dict.write(out);//序列化字典
                for (int i = 0; i < rowIndices.size(); i++) {//循环每一行数据
                    int[] row = rowIndices.get(i);
                    for (int j = 0; j < n; j++) {
                        out.writeInt(row[j]);//将每一行中的每一列数据对应的字典ID写入到输出中
                    }
                }

            } else {
                for (int i = 0; i < rowIndices.size(); i++) {//循环每一行数据
                    int[] row = rowIndices.get(i);
                    for (int j = 0; j < n; j++) {
                        // NULL_STR is tricky, but we don't want to break the current snapshots
                        out.writeUTF(dict.getValueFromId(row[j]) == null ? NULL_STR : dict.getValueFromId(row[j]));//从字典表中将数据还原成String,输出出去
                    }
                }
            }
        }
    }


    //参见序列化过程
    void readData(DataInput in) throws IOException {
        int rowNum = in.readInt();//读取行数
        if (rowNum > 0) {
            int n = in.readInt();//读取列数
            rowIndices = new ArrayList<int[]>(rowNum);

            if (this.useDictionary == true) {//存储了字典
                this.dict = new TrieDictionary<String>();//构建字典
                dict.readFields(in);//读取字典内容

                for (int i = 0; i < rowNum; i++) {//读取每一行每一列对应的字典ID
                    int[] row = new int[n];
                    this.rowIndices.add(row);
                    for (int j = 0; j < n; j++) {
                        row[j] = in.readInt();
                    }
                }
            } else {
                List<String[]> rows = new ArrayList<String[]>(rowNum);
                TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<String>(new StringBytesConverter());

                for (int i = 0; i < rowNum; i++) {
                    String[] row = new String[n];
                    rows.add(row);
                    for (int j = 0; j < n; j++) {
                        row[j] = in.readUTF();
                        // NULL_STR is tricky, but we don't want to break the current snapshots
                        if (row[j].equals(NULL_STR))
                            row[j] = null;

                        b.addValue(row[j]);
                    }
                }
                this.dict = b.build(0);
                for (String[] row : rows) {
                    int[] rowIndex = new int[n];
                    for (int i = 0; i < n; i++) {
                        rowIndex[i] = dict.getIdFromValue(row[i]);
                    }
                    this.rowIndices.add(rowIndex);
                }
            }
        } else {
            rowIndices = new ArrayList<int[]>();
            dict = new TrieDictionary<String>();
        }
    }

}
