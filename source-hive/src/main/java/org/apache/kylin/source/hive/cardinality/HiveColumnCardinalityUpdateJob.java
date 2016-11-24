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

package org.apache.kylin.source.hive.cardinality;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This job will update save the cardinality result into Kylin table metadata store.
 *
 * @author shaoshi
 */
public class HiveColumnCardinalityUpdateJob extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Kylin Hive Column Cardinality Update Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    private String table;

    private static final Logger logger = LoggerFactory.getLogger(HiveColumnCardinalityUpdateJob.class);

    public HiveColumnCardinalityUpdateJob() {

    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_TABLE);//table名字
            options.addOption(OPTION_OUTPUT_PATH);//输出

            parseOptions(options, args);

            this.table = getOptionValue(OPTION_TABLE).toUpperCase();
            // start job
            String jobName = JOB_TITLE + getOptionsAsString();
            logger.info("Starting: " + jobName);
            Configuration conf = getConf();
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            updateKylinTableExd(table.toUpperCase(), output.toString(), conf);
            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }

    }

    public void updateKylinTableExd(String tableName, String outPath, Configuration config) throws IOException {
        List<String> columns = null;
        try {
            columns = readLines(new Path(outPath), config);//读取所有的文件内容,每一行一个元素
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to resolve cardinality for " + tableName + " from " + outPath);
            return;
        }

        StringBuffer cardi = new StringBuffer();
        Iterator<String> it = columns.iterator();
        while (it.hasNext()) {
            String string = (String) it.next();
            String[] ss = StringUtils.split(string, "\t");

            if (ss.length != 2) {
                logger.info("The hadoop cardinality value is not valid " + string);
                continue;
            }
            cardi.append(ss[1]);
            cardi.append(",");
        }
        String scardi = cardi.toString();
        if (scardi.length() > 0) {
            scardi = scardi.substring(0, scardi.length() - 1);
            MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
            Map<String, String> tableExd = metaMgr.getTableDescExd(tableName);
            tableExd.put(MetadataConstants.TABLE_EXD_CARDINALITY, scardi);
            metaMgr.saveTableExd(tableName.toUpperCase(), tableExd);
        } else {
            throw new IllegalArgumentException("No cardinality data is collected for table " + tableName);
        }
    }

    /**
     * 返回location下所有文件对应的所有行内容
     * @param location
     * @param conf
     * @return
     * @throws Exception
     */
    private static List<String> readLines(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);//目录下所有文件
        if (items == null)
            return new ArrayList<String>();

        List<String> results = new ArrayList<String>();//存储所有的文件内容,每一行是一个元素

        for (FileStatus item : items) {//循环每一个文件

            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            //读取文件内容
            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            } else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");//将文件内容转换成String字符串
            String raw = writer.toString();
            for (String str : raw.split("\n")) {//文件的内容按照\n拆分成一行
                results.add(str);//添加到结果集中
            }
        }
        return results;
    }

}
