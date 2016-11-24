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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.kylin.source.ReadableTable.TableReader;

/**
 * An implementation of TableReader with HCatalog for Hive table.
 * 一个实现,去读取hive的一个table
 */
public class HiveTableReader implements TableReader {

    //要读取的数据库和表名
    private String dbName;
    private String tableName;
    private Map<String, String> partitionKV = null;//分区的key和value信息

    private int currentSplit = -1;//当前读取到哪个split了
    private ReaderContext readCntxt = null;//读取的上下文对象
    private Iterator<HCatRecord> currentHCatRecordItr = null;//当前的split对应的流
    private HCatRecord currentHCatRecord;//读取当前split的的当前记录
    private int numberOfSplits = 0;//总split数量


    /**
     * Constructor for reading whole hive table
     * @param dbName
     * @param tableName
     * @throws IOException
     */
    public HiveTableReader(String dbName, String tableName) throws IOException {
        this(dbName, tableName, null);
    }

    /**
     * Constructor for reading a partition of the hive table
     * @param dbName
     * @param tableName
     * @param partitionKV key-value pairs condition on the partition
     * @throws IOException
     */
    public HiveTableReader(String dbName, String tableName, Map<String, String> partitionKV) throws IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionKV = partitionKV;
        initialize();
    }

    private void initialize() throws IOException {
        try {
            //读取hive的表
            this.readCntxt = getHiveReaderContext(dbName, tableName, partitionKV);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        this.numberOfSplits = readCntxt.numSplits();

        //        HCatTableInfo tableInfo = HCatTableInfo.
        //        HCatSchema schema = HCatBaseInputFormat.getTableSchema(context.getConfiguration);
    }

    @Override
    public boolean next() throws IOException {

        while (currentHCatRecordItr == null || !currentHCatRecordItr.hasNext()) {//说明当前的reader没数据了
            currentSplit++;
            if (currentSplit == numberOfSplits) {//说明最后一个split了,返回false,结束读取
                return false;
            }

            //切换到下一个split,读取该split
            currentHCatRecordItr = loadHCatRecordItr(readCntxt, currentSplit);
        }

        //一行一行迭代
        currentHCatRecord = currentHCatRecordItr.next();

        return true;
    }

    //将当前对象转换成数组,该数组内容是列的属性值集合
    @Override
    public String[] getRow() {
        return getRowAsStringArray(currentHCatRecord);
    }

    public List<String> getRowAsList() {
        return getRowAsList(currentHCatRecord);
    }

    //将这行信息,添加到第二个参数集合里面
    public static List<String> getRowAsList(HCatRecord record, List<String> rowValues) {
        List<Object> allFields = record.getAll();
        for (Object o : allFields) {
            rowValues.add((o == null) ? null : o.toString());
        }
        return rowValues;
    }

    //将参数内容转换成集合
    public static List<String> getRowAsList(HCatRecord record) {
        return Arrays.asList(getRowAsStringArray(record));
    }

    //读取hive的一行记录,将每一列转换成字符串,返回数组
    public static String[] getRowAsStringArray(HCatRecord record) {
        String[] arr = new String[record.size()];
        for (int i = 0; i < arr.length; i++) {
            Object o = record.get(i);
            arr[i] = (o == null) ? null : o.toString();
        }
        return arr;
    }

    @Override
    public void close() throws IOException {
        this.readCntxt = null;
        this.currentHCatRecordItr = null;
        this.currentHCatRecord = null;
        this.currentSplit = -1;
    }

    public String toString() {
        return "hive table reader for: " + dbName + "." + tableName;
    }

    //读取该表
    private static ReaderContext getHiveReaderContext(String database, String table, Map<String, String> partitionKV) throws Exception {
        HiveConf hiveConf = new HiveConf(HiveTableReader.class);
        //hive的配置信息读取到map中
        Iterator<Entry<String, String>> itr = hiveConf.iterator();
        Map<String, String> map = new HashMap<String, String>();
        while (itr.hasNext()) {
            Entry<String, String> kv = itr.next();
            map.put(kv.getKey(), kv.getValue());
        }

        //读表或者读某个分区的表
        ReadEntity entity;
        if (partitionKV == null || partitionKV.size() == 0) {
            entity = new ReadEntity.Builder().withDatabase(database).withTable(table).build();
        } else {
            entity = new ReadEntity.Builder().withDatabase(database).withTable(table).withPartition(partitionKV).build();
        }

        HCatReader reader = DataTransferFactory.getHCatReader(entity, map);
        ReaderContext cntxt = reader.prepareRead();

        return cntxt;
    }

    //读取某一个split
    private static Iterator<HCatRecord> loadHCatRecordItr(ReaderContext readCntxt, int dataSplit) throws HCatException {
        HCatReader currentHCatReader = DataTransferFactory.getHCatReader(readCntxt, dataSplit);

        return currentHCatReader.read();
    }
}
