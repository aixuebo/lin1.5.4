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

package org.apache.kylin.job.common;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.Logger;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * shell的执行引擎
 */
public class ShellExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ShellExecutable.class);

    private static final String CMD = "cmd";//要执行的命令

    public ShellExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            logger.info("executing:" + getCmd());
            final ShellExecutableLogger logger = new ShellExecutableLogger();
            final Pair<Integer, String> result = context.getConfig().getCliCommandExecutor().execute(getCmd(), logger);//去执行命令,返回状态码以及输出内容
            executableManager.addJobInfo(getId(), logger.getInfo());//将该任务获取的数据信息存储到任务磁盘上
            return new ExecuteResult(result.getFirst() == 0 ? ExecuteResult.State.SUCCEED : ExecuteResult.State.FAILED, result.getSecond());//返回是否执行成功
        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setCmd(String cmd) {
        setParam(CMD, cmd);
    }

    public String getCmd() {
        return getParam(CMD);
    }

    private static class ShellExecutableLogger implements Logger {

        private final Map<String, String> info = Maps.newHashMap();//存储日志的信息内容


        private static final Pattern PATTERN_JOB_ID = Pattern.compile("Running job: (.*)");//存储mr的jobid


        private static final Pattern PATTERN_APP_ID = Pattern.compile("Submitted application (.*?) to ResourceManager");//解析日志提交哪个应用id到ResourceManager
        private static final Pattern PATTERN_APP_URL = Pattern.compile("The url to track the job: (.*)");//yarn上app的url


        private static final Pattern PATTERN_HDFS_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS: Number of bytes written=(\\d+)");//匹配写入多少数据到HDFS
        private static final Pattern PATTERN_SOURCE_RECORDS_COUNT = Pattern.compile("Map input records=(\\d+)");//匹配输入源有多少行数据
        private static final Pattern PATTERN_SOURCE_RECORDS_SIZE = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) HDFS Write");//匹配输入源读取多少字节数据

        // hive
        private static final Pattern PATTERN_HIVE_APP_ID_URL = Pattern.compile("Starting Job = (.*?), Tracking URL = (.*)");//hive的输出,比如Starting Job = job_1477918604929_15950, Tracking URL = http://path:8088/proxy/application_1477918604929_15950/
        private static final Pattern PATTERN_HIVE_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) HDFS Write: (\\d+) SUCCESS"); //hive读了多少个字节  写入多少个字节到hdfs

        @Override
        public void log(String message) {
            Matcher matcher = PATTERN_APP_ID.matcher(message);
            if (matcher.find()) {
                String appId = matcher.group(1);
                info.put(ExecutableConstants.YARN_APP_ID, appId);
            }

            matcher = PATTERN_APP_URL.matcher(message);
            if (matcher.find()) {
                String appTrackingUrl = matcher.group(1);
                info.put(ExecutableConstants.YARN_APP_URL, appTrackingUrl);
            }

            matcher = PATTERN_JOB_ID.matcher(message);
            if (matcher.find()) {
                String mrJobID = matcher.group(1);
                info.put(ExecutableConstants.MR_JOB_ID, mrJobID);
            }

            matcher = PATTERN_HDFS_BYTES_WRITTEN.matcher(message);
            if (matcher.find()) {
                String hdfsWritten = matcher.group(1);
                info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, hdfsWritten);
            }

            matcher = PATTERN_SOURCE_RECORDS_COUNT.matcher(message);
            if (matcher.find()) {
                String sourceCount = matcher.group(1);
                info.put(ExecutableConstants.SOURCE_RECORDS_COUNT, sourceCount);
            }

            matcher = PATTERN_SOURCE_RECORDS_SIZE.matcher(message);
            if (matcher.find()) {
                String sourceSize = matcher.group(1);
                info.put(ExecutableConstants.SOURCE_RECORDS_SIZE, sourceSize);
            }

            // hive
            matcher = PATTERN_HIVE_APP_ID_URL.matcher(message);
            if (matcher.find()) {
                String jobId = matcher.group(1);
                String trackingUrl = matcher.group(2);
                info.put(ExecutableConstants.MR_JOB_ID, jobId);
                info.put(ExecutableConstants.YARN_APP_URL, trackingUrl);
            }

            matcher = PATTERN_HIVE_BYTES_WRITTEN.matcher(message);
            if (matcher.find()) {
                // String hdfsRead = matcher.group(1);
                String hdfsWritten = matcher.group(2);
                info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, hdfsWritten);
            }
        }

        Map<String, String> getInfo() {
            return info;
        }
    }

}
