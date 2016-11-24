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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * This hadoop job will scan all rows of the hive table and then calculate the cardinality on each column.
 * @author shaoshi
 * 该job扫描hive table的所有行,然后计算每一个列的基数
 *
 * 对hive的table表中每一个列进行估算distinct(value),
 * 输入列序号,统计值
 * 输出每一个列序号作为key,value是该列distinct(value)的估值
 */
public class HiveColumnCardinalityJob extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Kylin Hive Column Cardinality Job";//列基数job

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    public static final String OUTPUT_PATH = BatchConstants.CFG_KYLIN_HDFS_TEMP_DIR + "cardinality";///tmp/kylin/cardinality临时目录存储基数

    public HiveColumnCardinalityJob() {
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_TABLE);//设置table
            options.addOption(OPTION_OUTPUT_PATH);//设置输出目录

            parseOptions(options, args);

            // start job
            String jobName = JOB_TITLE + getOptionsAsString();
            logger.info("Starting: " + jobName);
            Configuration conf = getConf();

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();//获取本地的KylinConfig对象

            JobEngineConfig jobEngineConfig = new JobEngineConfig(kylinConfig);
            conf.addResource(new Path(jobEngineConfig.getHadoopJobConfFilePath(null)));

            job = Job.getInstance(conf, jobName);

            //设置kylin启动的环境信息
            setJobClasspath(job, kylinConfig);

            String table = getOptionValue(OPTION_TABLE);//获取hive的table信息
            job.getConfiguration().set(BatchConstants.CFG_TABLE_NAME, table);//设置table.name属性

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));//设置输出
            FileOutputFormat.setOutputPath(job, output);
            job.getConfiguration().set("dfs.block.size", "67108864");//64M

            // Mapper
            IMRTableInputFormat tableInputFormat = MRUtil.getTableInputFormat(table);
            tableInputFormat.configureJob(job);

            job.setMapperClass(ColumnCardinalityMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BytesWritable.class);

            // Reducer - only one
            job.setReducerClass(ColumnCardinalityReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);//1个reduce

            this.deletePath(job.getConfiguration(), output);//删除输出原有内容

            logger.info("Going to submit HiveColumnCardinalityJob for table '" + table + "'");

            TableDesc tableDesc = MetadataManager.getInstance(kylinConfig).getTableDesc(table);
            attachKylinPropsAndMetadata(tableDesc, job.getConfiguration());//上传job的元数据信息

            int result = waitForCompletion(job);

            return result;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }

    }

}
