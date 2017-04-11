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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 计算每一个host上有多少个table
 * 例如 ./kylin.sh org.apache.kylin.storage.hbase.util.HBaseUsage
 * 输出结果 kylin_metadata has htable count: 76
 */
public class HBaseUsage {

    public static void main(String[] args) throws IOException {
        show();
    }

    private static void show() throws IOException {
        /**
         * key表示host,value表示该host上有哪些table集合
         */
        Map<String, List<String>> envs = Maps.newHashMap();

        // get all kylin hbase tables
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;//hbase的表以KYLIN_开头的集合
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);//获取表的KYLIN_HOST属性值
            if (StringUtils.isEmpty(host)) {
                add("unknown", desc.getNameAsString(), envs);
            } else {
                add(host, desc.getNameAsString(), envs);
            }
        }

        for (Map.Entry<String, List<String>> entry : envs.entrySet()) {
            System.out.println(entry.getKey() + " has htable count: " + entry.getValue().size());//表示每一个host上有多少个表
        }
        hbaseAdmin.close();
    }

    /**
     *
     * @param tag 属性值
     * @param tableName 属于哪个表的
     * @param envs 总的环境
     * 向环境envs中添加属性
     */
    private static void add(String tag, String tableName, Map<String, List<String>> envs) {
        if (!envs.containsKey(tag)) {//不包含该key
            envs.put(tag, Lists.<String> newArrayList());
        }
        envs.get(tag).add(tableName);
    }
}
