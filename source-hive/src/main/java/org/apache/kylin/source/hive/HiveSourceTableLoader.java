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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Management class to sync hive table metadata with command See main method for
 * how to use the class
 *
 * @author jianliu
 * 用于加载hive的表结构到kylin中,或者在kylin内存中删除hive的表结构映射信息
 */
public class HiveSourceTableLoader {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveSourceTableLoader.class);

    public static final String OUTPUT_SURFIX = "json";
    public static final String TABLE_FOLDER_NAME = "table";
    public static final String TABLE_EXD_FOLDER_NAME = "table_exd";

    //移除该表的元数据以及参数信息
    public static void unLoadHiveTable(String hiveTable) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        metaMgr.removeSourceTable(hiveTable);
        metaMgr.removeTableExd(hiveTable);
    }

    /**
     * 加载若干数据库表集合
     * @param hiveTables 数据库.表组成的集合
     * @param config
     * @return
     * @throws IOException
     */
    public static Set<String> reloadHiveTables(String[] hiveTables, KylinConfig config) throws IOException {

        //key是database,value是该下table集合
        Map<String, Set<String>> db2tables = Maps.newHashMap();

        for (String table : hiveTables) {
            String[] parts = HadoopUtil.parseHiveTableName(table);//返回数据库和table名字组成的数组
            Set<String> set = db2tables.get(parts[0]);
            if (set == null) {
                set = Sets.newHashSet();
                db2tables.put(parts[0], set);
            }
            set.add(parts[1]);
        }

        // extract from hive
        Set<String> loadedTables = Sets.newHashSet();
        for (String database : db2tables.keySet()) {//循环所有数据库
            List<String> loaded = extractHiveTables(database, db2tables.get(database), config);
            loadedTables.addAll(loaded);
        }

        return loadedTables;
    }

    /**
     * 加载某一个库下的一些表集合
     * @param database 数据库
     * @param tables 该数据库下表集合
     * @param config
     * @return
     * @throws IOException
     */
    private static List<String> extractHiveTables(String database, Set<String> tables, KylinConfig config) throws IOException {

        //返回加载的database.table字符串集合
        List<String> loadedTables = Lists.newArrayList();

        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());//获取元数据对象

        for (String tableName : tables) {//循环要加载的表
            HiveClient hiveClient = new HiveClient();
            Table table = null;//表对象
            List<FieldSchema> partitionFields = null;//分区字段集合
            List<FieldSchema> fields = null;//该列属性集合
            try {
                table = hiveClient.getHiveTable(database, tableName);//获取table信息
                partitionFields = table.getPartitionKeys();
                fields = hiveClient.getHiveTableFields(database, tableName);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e);
            }

            if (fields != null && partitionFields != null && partitionFields.size() > 0) {//列中加入分区
                fields.addAll(partitionFields);
            }

            long tableSize = hiveClient.getFileSizeForTable(table);//表大小
            long tableFileNum = hiveClient.getFileNumberForTable(table);//该table的文件数

            TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);//返回kylin中存储的该表信息
            if (tableDesc == null) {//创建一个表
                tableDesc = new TableDesc();
                tableDesc.setDatabase(database.toUpperCase());
                tableDesc.setName(tableName.toUpperCase());
                tableDesc.setUuid(UUID.randomUUID().toString());
                tableDesc.setLastModified(0);
            }
            if (table.getTableType() != null) {//是内部表还是外部表
                tableDesc.setTableType(table.getTableType());
            }

            int columnNumber = fields.size();
            List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);//存储所有的列信息

            for (int i = 0; i < columnNumber; i++) {
                FieldSchema field = fields.get(i);
                ColumnDesc cdesc = new ColumnDesc();
                cdesc.setName(field.getName().toUpperCase());
                // use "double" in kylin for "float"
                if ("float".equalsIgnoreCase(field.getType())) {//转换成double类型
                    cdesc.setDatatype("double");
                } else {
                    cdesc.setDatatype(field.getType());
                }
                cdesc.setId(String.valueOf(i + 1));//第几个列
                columns.add(cdesc);
            }
            tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

            //设置所有分区信息,分区信息用逗号拆分
            StringBuffer partitionColumnString = new StringBuffer();
            for (int i = 0, n = partitionFields.size(); i < n; i++) {
                if (i > 0)
                    partitionColumnString.append(", ");
                partitionColumnString.append(partitionFields.get(i).getName().toUpperCase());
            }

            //存放该table的元数据参数信息
            Map<String, String> map = metaMgr.getTableDescExd(tableDesc.getIdentity());

            if (map == null) {
                map = Maps.newHashMap();
            }
            map.put(MetadataConstants.TABLE_EXD_TABLENAME, table.getTableName());
            map.put(MetadataConstants.TABLE_EXD_LOCATION, table.getSd().getLocation());//hive的Location,hdfs://host:port/log/stat/path
            map.put(MetadataConstants.TABLE_EXD_IF, table.getSd().getInputFormat());//hive的输入格式化类org.apache.hadoop.mapred.TextInputFormat
            map.put(MetadataConstants.TABLE_EXD_OF, table.getSd().getOutputFormat());//hive的输出格式化类org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            map.put(MetadataConstants.TABLE_EXD_OWNER, table.getOwner());//表的所有者
            map.put(MetadataConstants.TABLE_EXD_LAT, String.valueOf(table.getLastAccessTime()));//表的最后访问时间
            map.put(MetadataConstants.TABLE_EXD_PC, partitionColumnString.toString());//表的分区信息,多个分区字段用逗号拆分,比如column1,column2
            map.put(MetadataConstants.TABLE_EXD_TFS, String.valueOf(tableSize));//表此时的大小
            map.put(MetadataConstants.TABLE_EXD_TNF, String.valueOf(tableFileNum));//表此时的文件数量
            map.put(MetadataConstants.TABLE_EXD_PARTITIONED, Boolean.valueOf(partitionFields != null && partitionFields.size() > 0).toString());//表是否包含分区

            metaMgr.saveSourceTable(tableDesc);//保存该table的元数据信息到kylin
            metaMgr.saveTableExd(tableDesc.getIdentity(), map);//保存该table的一些参数信息
            loadedTables.add(tableDesc.getIdentity());
        }

        return loadedTables;
    }

}
