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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.mr.DFSFileTable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 如何读取hive的一个表内容
 */
public class HiveTable implements ReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTable.class);

    //读取哪个数据库的哪个表
    final private String database;
    final private String hiveTable;

    private HiveClient hiveClient;

    public HiveTable(TableDesc tableDesc) {
        this.database = tableDesc.getDatabase();
        this.hiveTable = tableDesc.getName();
    }

    //读取该表
    @Override
    public TableReader getReader() throws IOException {
        return new HiveTableReader(database, hiveTable);
    }

    //设置该table的真实的最后修改时间和文件总大小以及文件所在路径
    @Override
    public TableSignature getSignature() throws IOException {
        try {
            String path = computeHDFSLocation();//获取hive的路径

            Pair<Long, Long> sizeAndLastModified = DFSFileTable.getSizeAndLastModified(path);//返回该路径下文件的最后修改时间和总字节大小
            long size = sizeAndLastModified.getFirst();
            long lastModified = sizeAndLastModified.getSecond();

            // for non-native hive table, cannot rely on size & last modified on HDFS
            if (getHiveClient().isNativeTable(database, hiveTable) == false) {
                lastModified = System.currentTimeMillis(); // assume table is ever changing
            }

            return new TableSignature(path, size, lastModified);

        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw new IOException(e);
        }
    }

    //获取该table所在路径
    private String computeHDFSLocation() throws Exception {

        //hive的所在路径
        String override = KylinConfig.getInstanceFromEnv().getOverrideHiveTableLocation(hiveTable);
        if (override != null) {
            logger.debug("Override hive table location " + hiveTable + " -- " + override);
            return override;
        }

        //没有则读取该路径
        return getHiveClient().getHiveTableLocation(database, hiveTable);
    }

    //创建连接hive的客户端
    public HiveClient getHiveClient() {

        if (hiveClient == null) {
            hiveClient = new HiveClient();
        }
        return hiveClient;
    }

    @Override
    public String toString() {
        return "hive: database=[" + database + "], table=[" + hiveTable + "]";
    }

}
