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

package org.apache.kylin.job.execution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.apache.kylin.job.manager.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 表示一个任务
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    protected static final String SUBMITTER = "submitter";//任务的提交人
    protected static final String NOTIFY_LIST = "notify_list";//任务的通知邮件集合
    protected static final String START_TIME = "startTime";//任务执行开始时间
    protected static final String END_TIME = "endTime";//任务的完成时间

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);
    protected int retry = 0;//尝试次数

    private String name;
    private String id;//该任务的ID,UUID
    private Map<String, String> params = Maps.newHashMap();//存储该job的输出额外的信息

    protected static ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());//管理如何将job的信息和job的输出信息存储到磁盘上

    public AbstractExecutable() {
        setId(UUID.randomUUID().toString());//设置唯一ID
    }

    //任务要执行
    protected void onExecuteStart(ExecutableContext executableContext) {
        Map<String, String> info = Maps.newHashMap();
        info.put(START_TIME, Long.toString(System.currentTimeMillis()));
        executableManager.updateJobOutput(getId(), ExecutableState.RUNNING, info, null);//更新该任务的状态内容
    }

    //任务执行完成
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        setEndTime(System.currentTimeMillis());//设置完成时间
        if (!isDiscarded()) {//设置状态
            if (result.succeed()) {
                executableManager.updateJobOutput(getId(), ExecutableState.SUCCEED, null, result.output());//成功
            } else {
                executableManager.updateJobOutput(getId(), ExecutableState.ERROR, null, result.output());//非成功完成
            }
        }
    }

    //出现错误,则job结束
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        if (!isDiscarded()) {
            executableManager.addJobInfo(getId(), END_TIME, Long.toString(System.currentTimeMillis()));//设置结束时间
            String output = null;
            if (exception != null) {//设置错误信息内容
                final StringWriter out = new StringWriter();
                exception.printStackTrace(new PrintWriter(out));
                output = out.toString();
            }
            executableManager.updateJobOutput(getId(), ExecutableState.ERROR, null, output);//更改状态为error
        }
    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {

        logger.info("Executing AbstractExecutable (" + this.getName() + ")");

        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result = null;
        try {
            onExecuteStart(executableContext);//初始化一个job
            Throwable exception;
            do {
                if (retry > 0) {
                    logger.info("Retry " + retry);
                }
                exception = null;//每一次尝试,都将异常设置为null
                result = null;
                try {
                    result = doWork(executableContext);//真正去执行job
                } catch (Throwable e) {
                    logger.error("error running Executable: " + this.toString());
                    exception = e;
                }
                retry++;
            } while (((result != null && result.succeed() == false) || exception != null) && needRetry() == true);

            if (exception != null) {//有异常说明有问题
                onExecuteError(exception, executableContext);
                throw new ExecuteException(exception);
            }

            //执行任务完成命令
            onExecuteFinished(result, executableContext);
        } catch (Exception e) {
            if (isMetaDataPersistException(e)) {
                handleMetaDataPersistException(e);
            }
            if (e instanceof ExecuteException) {
                throw e;
            } else {
                throw new ExecuteException(e);
            }
        }
        return result;
    }

    protected void handleMetaDataPersistException(Exception e) {
        // do nothing.
    }

    private boolean isMetaDataPersistException(Exception e) {
        if (e instanceof PersistentException) {
            return true;
        }

        Throwable t = e.getCause();
        int depth = 0;
        while (t != null && depth < 5) {
            depth++;
            if (t instanceof PersistentException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void cleanup() throws ExecuteException {

    }

    //已经准备好了,则说明可以运行了
    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.READY;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public final String getId() {
        return this.id;
    }

    public final void setId(String id) {
        this.id = id;
    }

    @Override
    public final ExecutableState getStatus() {
        return executableManager.getOutput(this.getId()).getState();
    }

    @Override
    public final Map<String, String> getParams() {
        return this.params;
    }

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final void setParams(Map<String, String> params) {
        this.params.putAll(params);
    }

    //任务的输出内容,表示任务的最后修改时间
    public final long getLastModified() {
        return executableManager.getOutput(getId()).getLastModified();
    }

    public final void setSubmitter(String submitter) {
        setParam(SUBMITTER, submitter);
    }

    //任务的邮件通知列表集合
    public final List<String> getNotifyList() {
        final String str = getParam(NOTIFY_LIST);
        if (str != null) {
            return Lists.newArrayList(StringUtils.split(str, ","));
        } else {
            return Collections.emptyList();
        }
    }

    public final void setNotifyList(String notifications) {
        setParam(NOTIFY_LIST, notifications);
    }

    public final void setNotifyList(List<String> notifications) {
        setNotifyList(StringUtils.join(notifications, ","));
    }

    //格式化邮件内容,返回要发送给邮件的内容  分别表示主题和邮件正文内容
    protected Pair<String, String> formatNotifications(ExecutableContext executableContext, ExecutableState state) {
        return null;
    }

    protected final void notifyUserStatusChange(ExecutableContext context, ExecutableState state) {
        try {
            final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            List<String> users = getAllNofifyUsers(kylinConfig);//获取收取邮件的人的集合
            if (users.isEmpty()) {
                logger.warn("no need to send email, user list is empty");
                return;
            }
            final Pair<String, String> email = formatNotifications(context, state);
            doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    //获取收取邮件的人的集合
    private List<String> getAllNofifyUsers(KylinConfig kylinConfig) {
        List<String> users = Lists.newArrayList();
        users.addAll(getNotifyList());
        final String[] adminDls = kylinConfig.getAdminDls();
        if (null != adminDls) {
            for (String adminDl : adminDls) {
                users.add(adminDl);
            }
        }
        return users;
    }

    /**
     *
     * @param kylinConfig
     * @param users  接收邮件的人集合
     * @param email 要发送给邮件的内容  分别表示主题和邮件正文内容
     */
    private void doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return;
        }
        logger.info("prepare to send email to:" + users);
        logger.info("job name:" + getName());
        logger.info("submitter:" + getSubmitter());
        logger.info("notify list:" + users);
        new MailService(kylinConfig).sendMail(users, email.getLeft(), email.getRight());
    }

    /**
     *
     * @param email 要发送给邮件的内容  分别表示主题和邮件正文内容
     */
    protected void sendMail(Pair<String, String> email) {
        try {
            final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            List<String> users = getAllNofifyUsers(kylinConfig);//获取收取邮件的人的集合
            if (users.isEmpty()) {
                logger.warn("no need to send email, user list is empty");
                return;
            }
            doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    @Override
    public final Output getOutput() {
        return executableManager.getOutput(getId());
    }

    protected long getExtraInfoAsLong(String key, long defaultValue) {
        return getExtraInfoAsLong(executableManager.getOutput(getId()), key, defaultValue);
    }

    public static long getStartTime(Output output) {
        return getExtraInfoAsLong(output, START_TIME, 0L);
    }

    public static long getEndTime(Output output) {
        return getExtraInfoAsLong(output, END_TIME, 0L);
    }

    //job已经执行了多久
    public static long getDuration(long startTime, long endTime) {
        if (startTime == 0) {
            return 0;
        }
        if (endTime == 0) {
            return System.currentTimeMillis() - startTime;
        } else {
            return endTime - startTime;
        }
    }

    public static long getExtraInfoAsLong(Output output, String key, long defaultValue) {
        final String str = output.getExtra().get(key);
        if (str != null) {
            return Long.parseLong(str);
        } else {
            return defaultValue;
        }
    }

    //添加job的附加信息,即key=value的附加信息
    protected final void addExtraInfo(String key, String value) {
        executableManager.addJobInfo(getId(), key, value);
    }

    //加载磁盘上已经存在的额外信息
    protected final Map<String, String> getExtraInfo() {
        return executableManager.getOutput(getId()).getExtra();
    }

    public final void setStartTime(long time) {
        addExtraInfo(START_TIME, time + "");
    }

    public final void setEndTime(long time) {
        addExtraInfo(END_TIME, time + "");
    }

    public final long getStartTime() {
        return getExtraInfoAsLong(START_TIME, 0L);
    }

    public final long getEndTime() {
        return getExtraInfoAsLong(END_TIME, 0L);
    }

    public final long getDuration() {
        return getDuration(getStartTime(), getEndTime());
    }

    /*
    * discarded is triggered by JobService, the Scheduler is not awake of that
    * 是否丢弃该任务
    * */
    protected final boolean isDiscarded() {
        final ExecutableState status = executableManager.getOutput(getId()).getState();
        return status == ExecutableState.DISCARDED;
    }

    //是否还需要继续调度job
    protected boolean needRetry() {
        return this.retry <= KylinConfig.getInstanceFromEnv().getJobRetry();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus()).toString();
    }
}
