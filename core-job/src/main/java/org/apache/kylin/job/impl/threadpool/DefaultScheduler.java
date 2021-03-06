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

package org.apache.kylin.job.impl.threadpool;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.job.manager.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * 任务的调度器,用于调度任务
 *
 * ConnectionStateListener表示监听zookeeper上的锁
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private JobLock jobLock;//锁对象
    private ExecutableManager executableManager;//存储job的执行信息在磁盘上

    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;//抓去任务的线程池
    private ExecutorService jobPool;//任务执行的线程池
    private DefaultContext context;//上下文对象

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private volatile boolean initialized = false;//是否已经初始化完成
    private volatile boolean hasStarted = false;//是否已经开启了调度器
    private JobEngineConfig jobEngineConfig;

    private static DefaultScheduler INSTANCE = null;

    public DefaultScheduler() {
        if (INSTANCE != null) {
            throw new IllegalStateException("DefaultScheduler has been initiated.");
        }
    }

    //每隔一定周期抓取一次数据----主要抓取ready的job,然后提交给线程池去执行,同时统计一下每一个状态的job数量
    private class FetcherRunner implements Runnable {

        @Override
        synchronized public void run() {
            try {
                // logger.debug("Job Fetcher is running...");
                Map<String, Executable> runningJobs = context.getRunningJobs();//所有正在执行的job集合,key是Executable的id,value是Executable对象
                if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {//已经超过伐值了
                    //打印日志,说明此时有太多的job在运行了,已经不再抓取了,知道下一次调用
                    logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                    return;
                }

                //分别表示正在运行中    准备中的  其他未知类型  异常的  丢失的 成功的job数量
                int nRunning = 0, nReady = 0, nOthers = 0, nError = 0, nDiscarded = 0, nSUCCEED = 0;
                //只有ready的才会被进行调度
                for (final String id : executableManager.getAllJobIds()) {//循环所有的job
                    if (runningJobs.containsKey(id)) {
                        // logger.debug("Job id:" + id + " is already running");
                        nRunning++;
                        continue;
                    }
                    final Output output = executableManager.getOutput(id);
                    if ((output.getState() != ExecutableState.READY)) {
                        // logger.debug("Job id:" + id + " not runnable");
                        if (output.getState() == ExecutableState.DISCARDED) {
                            nDiscarded++;
                        } else if (output.getState() == ExecutableState.ERROR) {
                            nError++;
                        } else if (output.getState() == ExecutableState.SUCCEED) {
                            nSUCCEED++;
                        } else {
                            nOthers++;
                        }
                        continue;
                    }
                    nReady++;
                    AbstractExecutable executable = executableManager.getJob(id);//获取该job对象,每次都是读取磁盘,太耗费资源了,大并发时候肯定得优化
                    String jobDesc = executable.toString();
                    logger.info(jobDesc + " prepare to schedule");//打印描述内容
                    try {
                        context.addRunningJob(executable);//添加该job到上下文
                        jobPool.execute(new JobRunner(executable));//执行该job--提交给线程池去运行该job
                        logger.info(jobDesc + " scheduled");
                    } catch (Exception ex) {
                        context.removeRunningJob(executable);//从运行的job中移除
                        logger.warn(jobDesc + " fail to schedule", ex);
                    }
                }
                //打印多少个job应该运行,多少个job实际就是在运行队列中,本次抓取了多少个ready线程提交到线程池去后续执行,打印每种job状态下job的数量
                logger.info("Job Fetcher: " + nRunning + " should running, " + runningJobs.size() + " actual running, " + nReady + " ready, " + nSUCCEED + " already succeed, " + nError + " error, " + nDiscarded + " discarded, " + nOthers + " others");
            } catch (Exception e) {
                logger.warn("Job Fetcher caught a exception " + e);
            }
        }
    }

    //表示一个执行的job
    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try {
                executable.execute(context);//去执行该任务,此时是阻塞的,只有当该任务执行完后,才会被返回
                // trigger the next step asap 接下来做什么,继续调度fetcher的run线程方法
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);//当执行完该job后,该线程空闲下来了,因此要求再调度一下,看看是否有新任务
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);//移除该任务
            }
        }
    }

    public static DefaultScheduler getInstance() {
        return INSTANCE;
    }

    //说明zookeeper上的锁有变化
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
            try {
                shutdown();//有变化,因此要shutdown
            } catch (SchedulerException e) {
                throw new RuntimeException("failed to shutdown scheduler", e);
            }
        }
    }

    public synchronized static DefaultScheduler createInstance() {
        destroyInstance();
        INSTANCE = new DefaultScheduler();
        return INSTANCE;
    }

    public synchronized static void destroyInstance() {
        DefaultScheduler tmp = INSTANCE;
        INSTANCE = null;
        if (tmp != null) {
            try {
                tmp.shutdown();
            } catch (SchedulerException e) {
                logger.error("error stop DefaultScheduler", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, final JobLock jobLock) throws SchedulerException {
        this.jobLock = jobLock;
        
        String serverMode = jobEngineConfig.getConfig().getServerMode();//获取模式
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {//只能是job和all两种模式
            logger.info("server mode: " + serverMode + ", no need to run job scheduler");
            return;
        }
        logger.info("Initializing Job Engine ....");

        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        //说明只能有一个节点是job节点,因此需要zookeeper锁去获取,只有获取到的才能是job节点
        if (jobLock.lock() == false) {//没有获取到锁,因此是不能打开job调度的
            throw new IllegalStateException("Cannot start job scheduler due to lack of job lock");
        }

        //存储job的执行信息在磁盘上
        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);//抓去任务的线程池

        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();//最大job数量,用于存放线程池大小

        //任务执行的线程池
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        //将所有runing状态的任务设置成READY状态
        executableManager.resumeAllRunningJobs();

        //线程池每隔多少秒抓取一次任务
        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, 10, ExecutableConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);

        hasStarted = true;//说明任务已经打开
    }

    @Override
    public void shutdown() throws SchedulerException {
        logger.info("Shutingdown Job Engine ....");
        jobLock.unlock();//关闭zookeeper上的锁,让其他节点可以有机会获取该锁,成为job调度节点
        fetcherPool.shutdown();//关闭抓取线程池
        jobPool.shutdown();
    }

    @Override
    public boolean stop(AbstractExecutable executable) throws SchedulerException {
        if (hasStarted) {
            return true;
        } else {
            //TODO should try to stop this executable
            return true;
        }
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

}
