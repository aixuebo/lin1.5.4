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

package org.apache.kylin.storage.hbase.util;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * 使用zookeeper对文件进行锁
 *
 * 该锁用于在分布式环境下,多个节点可以都是job任务节点,因此该锁主要确保一个节点是job的节点,即kylin只有一个节点是jon节点
 */
public class ZookeeperJobLock implements JobLock {
    private Logger logger = LoggerFactory.getLogger(ZookeeperJobLock.class);

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";

    private String scheduleID;
    private InterProcessMutex sharedLock;//锁对象关注在scheduleID节点上
    private CuratorFramework zkClient;

    //获取锁---每一次获取锁都要连接一次zookeeper,这个是非常耗时的,不明白为什么会这么设计,难道很少使用到该锁么?如果很频繁的创建锁对象,那么这里的性能是很有问题的
    //注意:此时创建锁后并没有close关闭zookeeper,如果连接很多,那么都不关闭,那不是很有问题么?
    @Override
    public boolean lock() {
        this.scheduleID = schedulerId();
        String zkConnectString = getZKConnectString();//连接zookeeper的串
        logger.info("zk connection string:" + zkConnectString);
        logger.info("schedulerId:" + scheduleID);
        if (StringUtils.isEmpty(zkConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);//连接zookeeper的尝试连接代理,尝试3次.每次间隔1秒
        this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);//创建新的zookeeper客户端
        this.zkClient.start();
        this.sharedLock = new InterProcessMutex(zkClient, this.scheduleID);
        boolean hasLock = false;
        try {
            hasLock = sharedLock.acquire(3, TimeUnit.SECONDS);//获取锁
        } catch (Exception e) {
            logger.warn("error acquire lock", e);
        }
        if (!hasLock) {//说明获取锁失败
            logger.warn("fail to acquire lock, scheduler has not been started; maybe another kylin process is still running?");
            zkClient.close();
            return false;
        }
        return true;
    }

    @Override
    public void unlock() {
        releaseLock();
    }

    //连接zookeeper的串,ip:port,ip:port,多个连接使用逗号拆分,该zookeeper的连接是从hbase中读取到的
    private String getZKConnectString() {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();//从hbase的配置文件获取zookeepker
        final String serverList = conf.get(HConstants.ZOOKEEPER_QUORUM);
        final String port = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        return org.apache.commons.lang3.StringUtils.join(Iterables.transform(Arrays.asList(serverList.split(",")), new Function<String, String>() {
            @Nullable
            @Override
            public String apply(String input) {
                return input + ":" + port;
            }
        }), ",");
    }

    //释放锁---只有在job调度节点shutdown的时候才会被释放该锁,让其他节点去再次获取成为job的调度节点
    private void releaseLock() {
        try {
            if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
                // client.setData().forPath(ZOOKEEPER_LOCK_PATH, null);
                if (zkClient.checkExists().forPath(scheduleID) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(scheduleID);
                }
            }
        } catch (Exception e) {
            logger.error("error release lock:" + scheduleID);
            throw new RuntimeException(e);
        }
    }

    //锁对应的path节点
    private String schedulerId() {
        return ZOOKEEPER_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
    }
}
