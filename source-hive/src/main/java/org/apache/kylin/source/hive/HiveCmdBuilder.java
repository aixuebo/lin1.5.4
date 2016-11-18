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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 用于构建hive的sql
 * 生成hive -e sql 或者 beeline 参数 -f 执行的文件;rm -f 删除执行的文件,用于执行sql
 */
public class HiveCmdBuilder {
    private static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    public enum HiveClientMode {
        CLI,//用hive -e sql方式执行
        BEELINE//用.hql文件文件执行
    }

    private HiveClientMode clientMode;
    private KylinConfig kylinConfig;
    //sql的一段,每一个元素表示sql的一行内容
    final private ArrayList<String> statements = Lists.newArrayList();

    public HiveCmdBuilder() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        clientMode = HiveClientMode.valueOf(kylinConfig.getHiveClientMode().toUpperCase());
    }

    //返回hive -e sql 或者 beeline 参数 -f 执行的文件;rm -f 删除执行的文件
    public String build() {
        StringBuffer buf = new StringBuffer();

        switch (clientMode) {
        case CLI:
            buf.append("hive -e \"");
            for (String statement : statements) {//组装sql
                buf.append(statement).append("\n");
            }
            buf.append("\"");
            break;
        case BEELINE:
            BufferedWriter bw = null;
            try {
                File tmpHql = File.createTempFile("beeline_", ".hql");//创建一个beeline_.hql文件
                StringBuffer hqlBuf = new StringBuffer();//日志信息,用于存储sql
                bw = new BufferedWriter(new FileWriter(tmpHql));
                for (String statement : statements) {//组件sql,存储到文件中
                    bw.write(statement);
                    bw.newLine();

                    hqlBuf.append(statement).append("\n");
                }
                buf.append("beeline ");
                buf.append(kylinConfig.getHiveBeelineParams());
                buf.append(" -f ");//执行那个文件
                buf.append(tmpHql.getAbsolutePath());
                buf.append(";rm -f ");//执行完该文件后删除该文件
                buf.append(tmpHql.getAbsolutePath());

                //打印日志
                logger.info("The statements to execute in beeline: \n" + hqlBuf);
                if (logger.isDebugEnabled()) {
                    logger.debug("THe SQL to execute in beeline: \n" + IOUtils.toString(new FileReader(tmpHql)));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeQuietly(bw);
            }
            break;
        default:
            throw new RuntimeException("Hive client cannot be recognized: " + clientMode);
        }

        return buf.toString();
    }

    //重置sql内容
    public void reset() {
        statements.clear();
    }

    //添加一行sql内容
    public void addStatement(String statement) {
        statements.add(statement);
    }

    //添加多行sql内容
    public void addStatements(String[] stats) {
        for (String s : stats) {
            statements.add(s);
        }
    }

    @Override
    public String toString() {
        return build();
    }
}