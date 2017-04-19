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
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.CubingJob.AlgorithmEnum;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Save the cube segment statistic to Kylin metadata store
 */
public class SaveStatisticsStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SaveStatisticsStep.class);

    public SaveStatisticsStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        //获取一个segment
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context, CubingExecutableUtil.getCubeName(this.getParams()), CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {
            //获取统计文件的路径
            Path statisticsFilePath = new Path(CubingExecutableUtil.getStatisticsPath(this.getParams()), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
            FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
            if (!fs.exists(statisticsFilePath))
                throw new IOException("File " + statisticsFilePath + " does not exists");

            FSDataInputStream is = fs.open(statisticsFilePath);
            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, is, System.currentTimeMillis());//将统计结果存储到statisticsFileName路径下
            } finally {
                IOUtils.closeStream(is);
                fs.delete(statisticsFilePath, true);
            }

            decideCubingAlgorithm(newSegment, kylinConf);

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    //根据对cube的统计结果,决定使用什么算法处理cube的build
    private void decideCubingAlgorithm(CubeSegment seg, KylinConfig kylinConf) throws IOException {
        String algPref = kylinConf.getCubeAlgorithm();//获取配置的算法
        AlgorithmEnum alg;
        if (AlgorithmEnum.INMEM.name().equalsIgnoreCase(algPref)) {
            alg = AlgorithmEnum.INMEM;
        } else if (AlgorithmEnum.LAYER.name().equalsIgnoreCase(algPref)) {
            alg = AlgorithmEnum.LAYER;
        } else {//说明是auto自动识别算法
            int memoryHungryMeasures = 0;//内存饥饿的次数
            for (MeasureDesc measure : seg.getCubeDesc().getMeasures()) {//循环每一个度量
                if (measure.getFunction().getMeasureType().isMemoryHungry()) {//true表示该度量很耗费内存操作
                    logger.info("This cube has memory-hungry measure " + measure.getFunction().getExpression());
                    memoryHungryMeasures++;
                }
            }

            if (memoryHungryMeasures > 0) {//说明有耗费内存的度量
                alg = AlgorithmEnum.LAYER;
            } else if ("random".equalsIgnoreCase(algPref)) { // for testing 随机算法是用于测试
                alg = new Random().nextBoolean() ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;
            } else { // the default
                double threshold = kylinConf.getCubeAlgorithmAutoThreshold();//自动算法的伐值
                double mapperOverlapRatio = new CubeStatsReader(seg, kylinConf).getMapperOverlapRatioOfFirstBuild();//重叠比例
                logger.info("mapperOverlapRatio for " + seg + " is " + mapperOverlapRatio + " and threshold is " + threshold);
                alg = mapperOverlapRatio < threshold ? AlgorithmEnum.INMEM : AlgorithmEnum.LAYER;//小于伐值则使用内存,否则使用LAYER
            }

        }
        logger.info("The cube algorithm for " + seg + " is " + alg);

        CubingJob cubingJob = (CubingJob) executableManager.getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        cubingJob.setAlgorithm(alg);
    }

}
