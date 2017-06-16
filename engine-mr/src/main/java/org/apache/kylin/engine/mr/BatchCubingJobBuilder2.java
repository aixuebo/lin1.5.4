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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCubingJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    private final IMRBatchCubingInputSide inputSide;//实现类是org.apache.kylin.source.hive.HiveMRInput类getBatchCubingInputSide方法,即org.apache.kylin.source.hive.HiveMRInput类的BatchCubingInputSide子类
    private final IMRBatchCubingOutputSide2 outputSide;//org.apache.kylin.storage.hbase.steps.HBaseMROutput2Transition

    public BatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2((CubeSegment) seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);

        final CubingJob result = CubingJob.createBuildJob((CubeSegment) seg, submitter, config);//设置该job的属性---比如开始时间、对应的cube以及segment等
        final String jobId = result.getId();//uuid
        final String cuboidRootPath = getCuboidRootPath(jobId);//cuboid存储的根目录

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        //制作宽表,此时临时hive表内是有数据了,即后续只需要查询该宽表即可---宽表名字由segment的UUID组成,因此获取segment,就等于读取到宽表了
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        //读取宽表
        result.addTask(createFactDistinctColumnsStepWithStats(jobId));//-output  root/jobid/cube/fact_distinct_columns  -statisticsoutput 统计输出目录 root/jobid/cube/statistics
        result.addTask(createBuildDictionaryStep(jobId));//输入是root/jobid/cube/fact_distinct_columns,读取输入下/column文件夹,为每一个列建立字典
        result.addTask(createSaveStatisticsStep(jobId));//根据对cube的统计结果,决定使用什么算法处理cube的build
        //初始化hbase的一些情况
        outputSide.addStepPhase2_BuildDictionary(result);//创建hbase的table

        // Phase 3: Build Cube 真正的builder过程
        addLayerCubingSteps(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute //执行MR任务,CuboidJob中会进行过滤,只有layout算法的才会被执行,否则都是skip输出,即跳过执行
        result.addTask(createInMemCubingStep(jobId, cuboidRootPath)); // inmem cubing, only selected algorithm will execute 只有内存方法的时候才会被执行,该函数里面有判断语句
        outputSide.addStepPhase3_BuildCube(result);//将cobid转换成hbase识别的hfile文件,然后使用hbase的bulk功能,将hfile数据剪切到hbase里面

        // Phase 4: Update Metadata & Cleanup 清理工作
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));//更新cube的元数据以及segment的元数据信息---非MR,所以很快会完成
        inputSide.addStepPhase4_Cleanup(result);//删除临时hive的宽表
        outputSide.addStepPhase4_Cleanup(result);//删除hbase的一些清理工作

        return result;
    }

    //执行MR任务
    private void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        RowKeyDesc rowKeyDesc = ((CubeSegment) seg).getCubeDesc().getRowkey();
        final int groupRowkeyColumnsCount = ((CubeSegment) seg).getCubeDesc().getBuildLevel();//组内最多循环次数
        final int totalRowkeyColumnsCount = rowKeyDesc.getRowKeyColumns().length;//rowkey参与的字段数量
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);//分配每一个cuboid所存放的路径
        // base cuboid step
        result.addTask(createBaseCuboidStep(cuboidOutputTempPath, jobId));
        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {//最多循环多少次
            int dimNum = totalRowkeyColumnsCount - i;//从最高维度开始运算
            result.addTask(createNDimensionCuboidStep(cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount, jobId));
        }
    }

    //根据对cube的统计结果,决定使用什么算法处理cube的build
    private SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(getStatisticsPath(jobId), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    //使用内存算法进行build
    private MapReduceExecutable createInMemCubingStep(String jobId, String cuboidRootPath) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidRootPath);//内存计算结果输出目录
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(getInMemCuboidJob());
        cubeStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES + "," + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

    protected Class<? extends AbstractHadoopJob> getInMemCuboidJob() {
        return InMemCuboidJob.class;
    }

    //执行base cuboid的MR任务
    private MapReduceExecutable createBaseCuboidStep(String[] cuboidOutputTempPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input 输入就是一个大宽表,即临时的hive表
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputTempPath[0]);//输出目录
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Base_Cuboid_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);//设置jobid

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(getBaseCuboidJob());
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);//统计原始文件行数和原始文件大小
        return baseCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getBaseCuboidJob() {
        return BaseCuboidJob.class;
    }

    //执行非base cuboid的MR任务
    private MapReduceExecutable createNDimensionCuboidStep(String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount, String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + (totalRowkeyColumnCount - dimNum));
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return NDCuboidJob.class;
    }
}
