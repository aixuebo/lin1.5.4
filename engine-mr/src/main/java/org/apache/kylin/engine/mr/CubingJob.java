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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 继承链式job任务,可以执行一组job任务
 */
public class CubingJob extends DefaultChainedExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CubingJob.class);

    //根据对cube的统计结果,决定使用什么算法处理cube的build
    public enum AlgorithmEnum {
        LAYER,//耗费内存时候使用该算法
        INMEM
    }

    // KEYS of Output.extraInfo map, info passed across job steps 在basecuboid中查询宽表的时候统计到的结果
    public static final String SOURCE_RECORD_COUNT = "sourceRecordCount";//原始记录数
    public static final String SOURCE_SIZE_BYTES = "sourceSizeBytes";//原始文件大小

    public static final String CUBE_SIZE_BYTES = "byteSizeBytes";//cube的文件大小
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";//mr任务过程中等待的时间
    private static final String DEPLOY_ENV_NAME = "envName";
    private static final String PROJECT_INSTANCE_NAME = "projectName";//设置该cube所属的project

    //build一个CubeSegment
    public static CubingJob createBuildJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, "BUILD", submitter, config);
    }

    //merge合并一个CubeSegment
    public static CubingJob createMergeJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, "MERGE", submitter, config);
    }

    /**
     * @param jobType  BUILD 或者 MERGE
     */
    private static CubingJob initCubingJob(CubeSegment seg, String jobType, String submitter, JobEngineConfig config) {
        KylinConfig kylinConfig = config.getConfig();
        CubeInstance cube = seg.getCubeInstance();
        //找到符合该cube的项目,有且只有一个,否则都抛异常
        List<ProjectInstance> projList = ProjectManager.getInstance(kylinConfig).findProjects(cube.getType(), cube.getName());
        if (projList == null || projList.size() == 0) {
            throw new RuntimeException("Cannot find the project containing the cube " + cube.getName() + "!!!");
        } else if (projList.size() >= 2) {
            throw new RuntimeException("Find more than one project containing the cube " + cube.getName() + ". It does't meet the uniqueness requirement!!! ");
        }

        CubingJob result = new CubingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(kylinConfig.getDeployEnv());
        result.setProjectName(projList.get(0).getName());
        CubingExecutableUtil.setCubeName(seg.getCubeInstance().getName(), result.getParams());//设置该执行关联的cube
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());//设置关联的segment
        result.setName(seg.getCubeInstance().getName() + " - " + seg.getName() + " - " + jobType + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        result.setNotifyList(seg.getCubeInstance().getDescriptor().getNotifyList());
        return result;
    }

    public CubingJob() {
        super();
    }

    void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }

    void setProjectName(String name) {
        setParam(PROJECT_INSTANCE_NAME, name);
    }

    public String getProjectName() {
        return getParam(PROJECT_INSTANCE_NAME);
    }

    /**
     * 邮件通知内容,返回主题和内容
     */
    @Override
    protected Pair<String, String> formatNotifications(ExecutableContext context, ExecutableState state) {
        CubeInstance cubeInstance = CubeManager.getInstance(context.getConfig()).getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final Output output = jobService.getOutput(getId());
        String logMsg;
        state = output.getState();
        if (state != ExecutableState.ERROR && !cubeInstance.getDescriptor().getStatusNeedNotify().contains(state.toString())) {
            logger.info("state:" + state + " no need to notify users");
            return null;
        }
        switch (state) {
        case ERROR:
            logMsg = output.getVerboseMsg();
            break;
        case DISCARDED:
            logMsg = "job has been discarded";
            break;
        case SUCCEED:
            logMsg = "job has succeeded";
            break;
        default:
            return null;
        }

        //替换文件内容
        String content = ExecutableConstants.NOTIFY_EMAIL_TEMPLATE;
        content = content.replaceAll("\\$\\{job_name\\}", getName());
        content = content.replaceAll("\\$\\{result\\}", state.toString());
        content = content.replaceAll("\\$\\{env_name\\}", getDeployEnvName());
        content = content.replaceAll("\\$\\{project_name\\}", getProjectName());
        content = content.replaceAll("\\$\\{cube_name\\}", CubingExecutableUtil.getCubeName(this.getParams()));
        content = content.replaceAll("\\$\\{source_records_count\\}", String.valueOf(findSourceRecordCount()));
        content = content.replaceAll("\\$\\{start_time\\}", new Date(getStartTime()).toString());
        content = content.replaceAll("\\$\\{duration\\}", getDuration() / 60000 + "mins");
        content = content.replaceAll("\\$\\{mr_waiting\\}", getMapReduceWaitTime() / 60000 + "mins");
        content = content.replaceAll("\\$\\{last_update_time\\}", new Date(getLastModified()).toString());
        content = content.replaceAll("\\$\\{submitter\\}", StringUtil.noBlank(getSubmitter(), "missing submitter"));
        content = content.replaceAll("\\$\\{error_log\\}", Matcher.quoteReplacement(StringUtil.noBlank(logMsg, "no error message")));

        try {
            //job引擎在哪个ip上执行的
            InetAddress inetAddress = InetAddress.getLocalHost();
            content = content.replaceAll("\\$\\{job_engine\\}", inetAddress.getCanonicalHostName());
        } catch (UnknownHostException e) {
            logger.warn(e.getLocalizedMessage(), e);
        }

        String title = "[" + state.toString() + "] - [" + getDeployEnvName() + "] - [" + getProjectName() + "] - " + CubingExecutableUtil.getCubeName(this.getParams());

        return Pair.of(title, content);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        long time = 0L;//等待的总时间
        for (AbstractExecutable task : getTasks()) {
            final ExecutableState status = task.getStatus();
            if (status != ExecutableState.SUCCEED) {
                break;
            }
            if (task instanceof MapReduceExecutable) {
                time += ((MapReduceExecutable) task).getMapReduceWaitTime();
            }
        }
        setMapReduceWaitTime(time);//设置等待的总时间
        super.onExecuteFinished(result, executableContext);
    }

    /**
     * build fail because the metadata store has problem.
     * @param exception
     */
    @Override
    protected void handleMetaDataPersistException(Exception exception) {
        String title = "[ERROR] - [" + getDeployEnvName() + "] - [" + getProjectName() + "] - " + CubingExecutableUtil.getCubeName(this.getParams());
        String content = ExecutableConstants.NOTIFY_EMAIL_TEMPLATE;
        final String UNKNOWN = "UNKNOWN";
        String errMsg = null;
        if (exception != null) {
            final StringWriter out = new StringWriter();
            exception.printStackTrace(new PrintWriter(out));
            errMsg = out.toString();
        }

        content = content.replaceAll("\\$\\{job_name\\}", getName());
        content = content.replaceAll("\\$\\{result\\}", ExecutableState.ERROR.toString());
        content = content.replaceAll("\\$\\{env_name\\}", getDeployEnvName());
        content = content.replaceAll("\\$\\{project_name\\}", getProjectName());
        content = content.replaceAll("\\$\\{cube_name\\}", CubingExecutableUtil.getCubeName(this.getParams()));
        content = content.replaceAll("\\$\\{source_records_count\\}", UNKNOWN);
        content = content.replaceAll("\\$\\{start_time\\}", UNKNOWN);
        content = content.replaceAll("\\$\\{duration\\}", UNKNOWN);
        content = content.replaceAll("\\$\\{mr_waiting\\}", UNKNOWN);
        content = content.replaceAll("\\$\\{last_update_time\\}", UNKNOWN);
        content = content.replaceAll("\\$\\{submitter\\}", StringUtil.noBlank(getSubmitter(), "missing submitter"));
        content = content.replaceAll("\\$\\{error_log\\}", Matcher.quoteReplacement(StringUtil.noBlank(errMsg, "no error message")));

        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            content = content.replaceAll("\\$\\{job_engine\\}", inetAddress.getCanonicalHostName());
        } catch (UnknownHostException e) {
            logger.warn(e.getLocalizedMessage(), e);
        }
        sendMail(Pair.of(title, content));
    }

    public long getMapReduceWaitTime() {
        return getExtraInfoAsLong(MAP_REDUCE_WAIT_TIME, 0L);
    }

    public void setMapReduceWaitTime(long t) {
        addExtraInfo(MAP_REDUCE_WAIT_TIME, t + "");
    }

    public void setAlgorithm(AlgorithmEnum alg) {
        addExtraInfo("algorithm", alg.name());
    }

    public AlgorithmEnum getAlgorithm() {
        String alg = getExtraInfo().get("algorithm");
        return alg == null ? null : AlgorithmEnum.valueOf(alg);
    }

    public boolean isLayerCubing() {
        return AlgorithmEnum.LAYER == getAlgorithm();
    }

    public boolean isInMemCubing() {
        return AlgorithmEnum.INMEM == getAlgorithm();
    }

    public long findSourceRecordCount() {
        return Long.parseLong(findExtraInfo(SOURCE_RECORD_COUNT, "0"));
    }

    public long findSourceSizeBytes() {
        return Long.parseLong(findExtraInfo(SOURCE_SIZE_BYTES, "0"));
    }

    public long findCubeSizeBytes() {
        // look for the info BACKWARD, let the last step that claims the cube size win
        return Long.parseLong(findExtraInfoBackward(CUBE_SIZE_BYTES, "0"));
    }

    public String findExtraInfo(String key, String dft) {
        return findExtraInfo(key, dft, false);
    }

    public String findExtraInfoBackward(String key, String dft) {
        return findExtraInfo(key, dft, true);
    }

    /**
     * 查找key对应的输出的value信息
     * @param backward 表示任务集合从前往后找,还是从后往前找
     * @param dft 默认值
     */
    private String findExtraInfo(String key, String dft, boolean backward) {
        ArrayList<AbstractExecutable> tasks = new ArrayList<AbstractExecutable>(getTasks());//任务集合

        if (backward) {
            Collections.reverse(tasks);//反转集合顺序
        }

        for (AbstractExecutable child : tasks) {
            Output output = executableManager.getOutput(child.getId());//每一个job的输出
            String value = output.getExtra().get(key);//获取该key对应的value
            if (value != null)
                return value;//返回第一次出现key对应的value
        }
        return dft;
    }

}
