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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1.用于将rowkey中每一个字典列需要的字段值收集起来,一个reduce一个字段的不重复的字典值
 * 2.统计rowkey的各种组合情况,即每一个cuboid有多少个不同的元素
 */
public class FactDistinctColumnsJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_OUTPUT_PATH);//输出root/jobid/cube/fact_distinct_columns
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_STATISTICS_ENABLED);
            options.addOption(OPTION_STATISTICS_OUTPUT);
            options.addOption(OPTION_STATISTICS_SAMPLING_PERCENT);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String job_id = getOptionValue(OPTION_CUBING_JOB_ID);
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, job_id);
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String statistics_enabled = getOptionValue(OPTION_STATISTICS_ENABLED);
            String statistics_output = getOptionValue(OPTION_STATISTICS_OUTPUT);
            String statistics_sampling_percent = getOptionValue(OPTION_STATISTICS_SAMPLING_PERCENT);

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            List<TblColRef> columnsNeedDict = cubeMgr.getAllDictColumnsOnFact(cube.getDescriptor());

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_ENABLED, statistics_enabled);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_OUTPUT, statistics_output);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, statistics_sampling_percent);
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            CubeSegment segment = cube.getSegmentById(segmentID);
            if (segment == null) {
                logger.error("Failed to find {} in cube {}", segmentID, cube);
                System.out.println("Failed to find {} in cube {} " + segmentID + "," + cube);
                for (CubeSegment s : cube.getSegments()) {
                    logger.error(s.getName() + " with status " + s.getStatus());
                    System.out.println(s.getName() + " with status " + s.getStatus());
                }
                throw new IllegalStateException();
            } else {
                logger.info("Found segment: " + segment);
                System.out.println("Found segment " + segment);
            }
            setupMapper(cube.getSegmentById(segmentID));
            setupReducer(output, "true".equalsIgnoreCase(statistics_enabled) ? columnsNeedDict.size() + 1 : columnsNeedDict.size());//每一个reduce表示一个字典的一个字段

            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            return waitForCompletion(job);

        } catch (Exception e) {
            logger.error("error in FactDistinctColumnsJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }

    }

    private void setupMapper(CubeSegment cubeSeg) throws IOException {
        IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();//读取cube的hive临时表数据内容,即根据segment的名字,读取到hive的临时宽表
        flatTableInputFormat.configureJob(job);

        job.setMapperClass(FactDistinctHiveColumnsMapper.class);//输出没i个列的index和对应的列值
        job.setCombinerClass(FactDistinctColumnsCombiner.class);//每一个index列对应的列值保留一份
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    }

    //输出和reduce的数量作为参数
    private void setupReducer(Path output, int numberOfReducers) throws IOException {
        job.setReducerClass(FactDistinctColumnsReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(FactDistinctColumnPartitioner.class);
        job.setNumReduceTasks(numberOfReducers);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

        deletePath(job.getConfiguration(), output);
    }

    public static void main(String[] args) throws Exception {
        FactDistinctColumnsJob job = new FactDistinctColumnsJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
