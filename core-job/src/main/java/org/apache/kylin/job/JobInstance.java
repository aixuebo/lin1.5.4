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

package org.apache.kylin.job;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepCmdTypeEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * 表示kylin监控中的一个job任务---即web的监视序列化的内容
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JobInstance extends RootPersistentEntity implements Comparable<JobInstance> {

    public static final String YARN_APP_URL = "yarn_application_tracking_url";
    public static final String MR_JOB_ID = "mr_job_id";

    @JsonProperty("name")
    private String name;//cubeName - 20170412063000_20170413063000 - BUILD - GMT+08:00 2017-04-13 09:22:07

    @JsonProperty("type")
    private CubeBuildTypeEnum type; // java implementation  该job操作的形式:BUILD  MERGE  REFRESH
    @JsonProperty("duration")
    private long duration;//该job已经执行的时间
    @JsonProperty("related_cube")
    private String relatedCube;//该job关联的cube
    @JsonProperty("related_segment")
    private String relatedSegment;//该job处理的cube的segmentid是哪个
    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonProperty("exec_end_time")
    private long execEndTime;
    @JsonProperty("mr_waiting")
    private long mrWaiting = 0;//cubeJob.getMapReduceWaitTime() / 1000
    @JsonManagedReference
    @JsonProperty("steps")
    private List<JobStep> steps;//job的子任务集合
    @JsonProperty("submitter")
    private String submitter;//该job的提交人
    @JsonProperty("job_status")
    private JobStatusEnum status;//job的运行状态

    //获取运行中或者等待的job,就是此时执行的子job
    public JobStep getRunningStep() {
        for (JobStep step : this.getSteps()) {
            if (step.getStatus().equals(JobStepStatusEnum.RUNNING) || step.getStatus().equals(JobStepStatusEnum.WAITING)) {
                return step;
            }
        }

        return null;
    }

    /**
     * 完成的job数量所占用的百分比
     */
    @JsonProperty("progress")
    public double getProgress() {
        int completedStepCount = 0;//完成的子job数
        for (JobStep step : this.getSteps()) {
            if (step.getStatus().equals(JobStepStatusEnum.FINISHED)) {
                completedStepCount++;
            }
        }

        return 100.0 * completedStepCount / steps.size();
    }

    public JobStatusEnum getStatus() {
        return this.status;
    }

    public void setStatus(JobStatusEnum status) {
        this.status = status;
    }

    //    @JsonProperty("job_status")
    //    public JobStatusEnum getStatus() {
    //
    //        // JobStatusEnum finalJobStatus;
    //        int compositResult = 0;
    //
    //        // if steps status are all NEW, then job status is NEW
    //        // if steps status are all FINISHED, then job status is FINISHED
    //        // if steps status are all PENDING, then job status is PENDING
    //        // if steps status are FINISHED and PENDING, the job status is PENDING
    //        // if one of steps status is RUNNING, then job status is RUNNING
    //        // if one of steps status is ERROR, then job status is ERROR
    //        // if one of steps status is KILLED, then job status is KILLED
    //        // default status is RUNNING
    //
    //        System.out.println(this.getName());
    //
    //        for (JobStep step : this.getSteps()) {
    //            //System.out.println("step: " + step.getSequenceID() + "'s status:" + step.getStatus());
    //            compositResult = compositResult | step.getStatus().getCode();
    //        }
    //
    //        System.out.println();
    //
    //        if (compositResult == JobStatusEnum.FINISHED.getCode()) {
    //            return JobStatusEnum.FINISHED;
    //        } else if (compositResult == JobStatusEnum.NEW.getCode()) {
    //            return JobStatusEnum.NEW;
    //        } else if (compositResult == JobStatusEnum.PENDING.getCode()) {
    //            return JobStatusEnum.PENDING;
    //        } else if (compositResult == (JobStatusEnum.FINISHED.getCode() | JobStatusEnum.PENDING.getCode())) {
    //            return JobStatusEnum.PENDING;
    //        } else if ((compositResult & JobStatusEnum.ERROR.getCode()) == JobStatusEnum.ERROR.getCode()) {
    //            return JobStatusEnum.ERROR;
    //        } else if ((compositResult & JobStatusEnum.DISCARDED.getCode()) == JobStatusEnum.DISCARDED.getCode()) {
    //            return JobStatusEnum.DISCARDED;
    //        } else if ((compositResult & JobStatusEnum.RUNNING.getCode()) == JobStatusEnum.RUNNING.getCode()) {
    //            return JobStatusEnum.RUNNING;
    //        }
    //
    //        return JobStatusEnum.RUNNING;
    //    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CubeBuildTypeEnum getType() {
        return type;
    }

    public void setType(CubeBuildTypeEnum type) {
        this.type = type;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getRelatedCube() {
        return relatedCube;
    }

    public void setRelatedCube(String relatedCube) {
        this.relatedCube = relatedCube;
    }

    public String getRelatedSegment() {
        return relatedSegment;
    }

    public void setRelatedSegment(String relatedSegment) {
        this.relatedSegment = relatedSegment;
    }

    /**
     * @return the execStartTime
     */
    public long getExecStartTime() {
        return execStartTime;
    }

    /**
     * @param execStartTime the execStartTime to set
     */
    public void setExecStartTime(long execStartTime) {
        this.execStartTime = execStartTime;
    }

    /**
     * @return the execEndTime
     */
    public long getExecEndTime() {
        return execEndTime;
    }

    /**
     * @param execEndTime the execEndTime to set
     */
    public void setExecEndTime(long execEndTime) {
        this.execEndTime = execEndTime;
    }

    public long getMrWaiting() {
        return this.mrWaiting;
    }

    public void setMrWaiting(long mrWaiting) {
        this.mrWaiting = mrWaiting;
    }

    public List<JobStep> getSteps() {
        if (steps == null) {
            steps = Lists.newArrayList();
        }
        return steps;
    }

    public void clearSteps() {
        getSteps().clear();
    }

    public void addSteps(Collection<JobStep> steps) {
        this.getSteps().addAll(steps);
    }

    public void addStep(JobStep step) {
        getSteps().add(step);
    }

    public void addStep(int index, JobStep step) {
        getSteps().add(index, step);
    }

    //通过子任务,查找对应的子job对象
    public JobStep findStep(String stepName) {
        for (JobStep step : getSteps()) {
            if (stepName.equals(step.getName())) {
                return step;
            }
        }
        return null;
    }

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String submitter) {
        this.submitter = submitter;
    }

    //按照最后更新时间排序
    @Override
    public int compareTo(JobInstance o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }

    //代表一个子job
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobStep implements Comparable<JobStep> {

        @JsonBackReference
        private JobInstance jobInstance;

        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("sequence_id")
        private int sequenceID;//子job的序号

        @JsonProperty("exec_cmd")
        private String execCmd;//执行job的输入参数集合,比如-table xxx -output xxx

        @JsonProperty("interrupt_cmd")
        private String InterruptCmd;

        @JsonProperty("exec_start_time")
        private long execStartTime;//该子任务的开始时间
        @JsonProperty("exec_end_time")
        private long execEndTime;//该子任务的结束时间
        @JsonProperty("exec_wait_time")
        private long execWaitTime;

        @JsonProperty("step_status")
        private JobStepStatusEnum status;

        @JsonProperty("cmd_type")
        private JobStepCmdTypeEnum cmdType = JobStepCmdTypeEnum.SHELL_CMD_HADOOP;

        //该job的输出内容
        @JsonProperty("info")
        private ConcurrentHashMap<String, String> info = new ConcurrentHashMap<String, String>();

        @JsonProperty("run_async")
        private boolean runAsync = false;

        private ConcurrentHashMap<String, String> getInfo() {
            return info;
        }

        public void putInfo(String key, String value) {
            getInfo().put(key, value);
        }

        public String getInfo(String key) {
            return getInfo().get(key);
        }

        public void clearInfo() {
            getInfo().clear();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSequenceID() {
            return sequenceID;
        }

        public void setSequenceID(int sequenceID) {
            this.sequenceID = sequenceID;
        }

        public String getExecCmd() {
            return execCmd;
        }

        public void setExecCmd(String execCmd) {
            this.execCmd = execCmd;
        }

        public JobStepStatusEnum getStatus() {
            return status;
        }

        public void setStatus(JobStepStatusEnum status) {
            this.status = status;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        /**
         * @return the execStartTime
         */
        public long getExecStartTime() {
            return execStartTime;
        }

        /**
         * @param execStartTime the execStartTime to set
         */
        public void setExecStartTime(long execStartTime) {
            this.execStartTime = execStartTime;
        }

        /**
         * @return the execEndTime
         */
        public long getExecEndTime() {
            return execEndTime;
        }

        /**
         * @param execEndTime the execEndTime to set
         */
        public void setExecEndTime(long execEndTime) {
            this.execEndTime = execEndTime;
        }

        public long getExecWaitTime() {
            return execWaitTime;
        }

        public void setExecWaitTime(long execWaitTime) {
            this.execWaitTime = execWaitTime;
        }

        public String getInterruptCmd() {
            return InterruptCmd;
        }

        public void setInterruptCmd(String interruptCmd) {
            InterruptCmd = interruptCmd;
        }

        public JobStepCmdTypeEnum getCmdType() {
            return cmdType;
        }

        public void setCmdType(JobStepCmdTypeEnum cmdType) {
            this.cmdType = cmdType;
        }

        /**
         * @return the runAsync
         */
        public boolean isRunAsync() {
            return runAsync;
        }

        /**
         * @param runAsync the runAsync to set
         */
        public void setRunAsync(boolean runAsync) {
            this.runAsync = runAsync;
        }

        /**
         * @return the jobInstance
         */
        public JobInstance getJobInstance() {
            return jobInstance;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + sequenceID;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            JobStep other = (JobStep) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (sequenceID != other.sequenceID)
                return false;
            return true;
        }

        @Override
        public int compareTo(JobStep o) {
            if (this.sequenceID < o.sequenceID) {
                return -1;
            } else if (this.sequenceID > o.sequenceID) {
                return 1;
            } else {
                return 0;
            }
        }
    }

}
