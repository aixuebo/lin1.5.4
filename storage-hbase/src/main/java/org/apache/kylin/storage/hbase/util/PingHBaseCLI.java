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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.storage.hbase.HBaseConnection;

/**
 * @author yangli9
 * 例如 ./kylin.sh org.apache.kylin.storage.hbase.util.PingHBaseCLI KYLIN_YLBKVYK3QO
 * 输出 前20个rowkey
 */
public class PingHBaseCLI {

    public static void main(String[] args) throws IOException {
        String hbaseTable = args[0];

        System.out.println("Hello friend.");

        Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
        if (User.isHBaseSecurityEnabled(hconf)) {
            try {
                System.out.println("--------------Getting kerberos credential for user " + UserGroupInformation.getCurrentUser().getUserName());
                TokenUtil.obtainAndCacheToken(hconf, UserGroupInformation.getCurrentUser());
            } catch (InterruptedException e) {
                System.out.println("--------------Error while getting kerberos credential for user " + UserGroupInformation.getCurrentUser().getUserName());
            }
        }

        Scan scan = new Scan();
        int limit = 20;

        HConnection conn = null;
        HTableInterface table = null;
        ResultScanner scanner = null;
        try {
            conn = HConnectionManager.createConnection(hconf);
            table = conn.getTable(hbaseTable);//扫描一个表
            scanner = table.getScanner(scan);
            int count = 0;
            for (Result r : scanner) {
                byte[] rowkey = r.getRow();//获取rowkey
                System.out.println(Bytes.toStringBinary(rowkey));//将rowkey的16进制转换一下,如果是字母 数字 特殊字符可以看到具体内容,否则还是16进制
                count++;
                if (count == limit)
                    break;
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }
}
