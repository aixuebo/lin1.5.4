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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CreateDictionaryJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsJob;
import org.apache.kylin.engine.mr.steps.MergeDictionaryStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterMergeStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;

import com.google.common.base.Preconditions;

/**
 * Hold reusable steps for builders.
 * 如何生成一个hadoop的job任务
 */
public class JobBuilderSupport {

    final protected JobEngineConfig config;
    final protected CubeSegment seg;
    final protected String submitter;

    public JobBuilderSupport(CubeSegment seg, String submitter) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        this.config = new JobEngineConfig(seg.getConfig());
        this.seg = seg;
        this.submitter = submitter;
    }

    public MapReduceExecutable createFactDistinctColumnsStep(String jobId) {
        return createFactDistinctColumnsStep(jobId, false);
    }

    public MapReduceExecutable createFactDistinctColumnsStepWithStats(String jobId) {
        return createFactDistinctColumnsStep(jobId, true);
    }

    /**
     * JOBid以及是否执行过程中需要统计
     * 1.用于将rowkey中每一个字典列需要的字段值收集起来,一个reduce一个字段的不重复的字典值
     * 2.统计rowkey的各种组合情况,即每一个cuboid有多少个不同的元素
     */
    private MapReduceExecutable createFactDistinctColumnsStep(String jobId, boolean withStats) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);//设置任务的名字
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);//设置mr程序的入口
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);//追加-conf path 命令
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());//-cubename cubename
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getFactDistinctColumnsPath(jobId));//-output
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());//-segmentid segment的UUID
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_ENABLED, String.valueOf(withStats));//-statisticsenabled 是否开启统计
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_OUTPUT, getStatisticsPath(jobId));//-statisticsoutput 统计输出目录
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_SAMPLING_PERCENT, String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));//-statisticssamplingpercent 统计抽样百分比
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Fact_Distinct_Columns_" + seg.getRealization().getName() + "_Step");//-jobname
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);//-cubingJobId jobid
        result.setMapReduceParams(cmd.toString());//添加额外的参数
        return result;
    }

    /**
     * createFactDistinctColumnsStep 任务的输出目录做为输入目录,进行mr处理
     * 主要处理字典信息
     */
    public HadoopShellExecutable createBuildDictionaryStep(String jobId) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());//-cubename cubename
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());//-segmentid segment的UUID
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getFactDistinctColumnsPath(jobId));//-input

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);//mr的主要类
        return buildDictionaryStep;
    }

    public UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId) {
        final UpdateCubeInfoAfterBuildStep result = new UpdateCubeInfoAfterBuildStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(jobId), result.getParams());

        return result;
    }

    public MergeDictionaryStep createMergeDictionaryStep(List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());

        return result;
    }

    public UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(jobId), result.getParams());

        return result;
    }

    // ============================================================================
    //每一个job的根目录
    public String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }

    public String getRealizationRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getRealization().getName();
    }

    //获取该job的cuboid目录
    public String getCuboidRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/cuboid/";
    }

    //通过CubeSegment获得该CubeSegment的jobid,从而得到该CubeSegment的cuboid存储路径
    public String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }

    //二级索引目录
    public String getSecondaryIndexPath(String jobId) {
        return getRealizationRootPath(jobId) + "/secondary_index/";
    }

    //追加-conf path 命令
    public void appendMapReduceParameters(StringBuilder buf) {
        appendMapReduceParameters(buf, JobEngineConfig.DEFAUL_JOB_CONF_SUFFIX);
    }

    //追加-conf path 命令
    public void appendMapReduceParameters(StringBuilder buf, String jobType) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(jobType);
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //输出目录
    public String getFactDistinctColumnsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/fact_distinct_columns";
    }

    //统计的输出目录
    public String getStatisticsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/statistics";
    }

    // ============================================================================
    // static methods also shared by other job flow participant
    // ----------------------------------------------------------------------------

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return getJobWorkingDir(conf.getHdfsWorkingDirectory(), jobId);
    }

    public static String getJobWorkingDir(String hdfsDir, String jobId) {
        if (!hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir + "/";
        }
        return hdfsDir + "kylin-" + jobId;
    }

    //添加参数-paraName paraValue
    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

    /**
     * @param cuboidRootPath 存储cuboid的根目录
     * @param totalRowkeyColumnCount 全部维度数
     * @param groupRowkeyColumnsCount 多少组
     */
    public String[] getCuboidOutputPaths(String cuboidRootPath, int totalRowkeyColumnCount, int groupRowkeyColumnsCount) {
        String[] paths = new String[groupRowkeyColumnsCount + 1];//路径集合
        for (int i = 0; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnCount - i;//总维度
            if (dimNum == totalRowkeyColumnCount) {
                paths[i] = cuboidRootPath + "base_cuboid";//总维度目录
            } else {
                paths[i] = cuboidRootPath + dimNum + "d_cuboid";//每一个维度对应的目录
            }
        }
        return paths;
    }

}
