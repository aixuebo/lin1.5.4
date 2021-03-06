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

package org.apache.kylin.rest.metrics;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The entrance of metrics features.
 */
@ThreadSafe
public class QueryMetricsFacade {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsFacade.class);

    private static boolean enabled = false;//是否开启
    private static ConcurrentHashMap<String, QueryMetrics> metricsMap = new ConcurrentHashMap<String, QueryMetrics>();

    //静态方法,初始化该对象
    public static void init() {
        enabled = KylinConfig.getInstanceFromEnv().getQueryMetricsEnabled();
        if (!enabled) //没有开启,则返回
            return;
        
        DefaultMetricsSystem.initialize("Kylin");
    }

    public static void updateMetrics(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!enabled)
            return;
        
        String projectName = sqlRequest.getProject();
        String cubeName = sqlResponse.getCube();

        update(getQueryMetrics("Server_Total", metricsMap), sqlResponse);

        update(getQueryMetrics(projectName, metricsMap), sqlResponse);

        String cubeMetricName = projectName + ",sub=" + cubeName;
        update(getQueryMetrics(cubeMetricName, metricsMap), sqlResponse);
    }

    private static void update(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        try {
            incrQueryCount(queryMetrics, sqlResponse);
            incrCacheHitCount(queryMetrics, sqlResponse);

            if (!sqlResponse.getIsException()) {
                queryMetrics.addQueryLatency(sqlResponse.getDuration());
                queryMetrics.addScanRowCount(sqlResponse.getTotalScanCount());
                queryMetrics.addResultRowCount(sqlResponse.getResults().size());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private static void incrQueryCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (!sqlResponse.isHitExceptionCache() && !sqlResponse.getIsException()) {
            queryMetrics.incrQuerySuccessCount();
        } else {
            queryMetrics.incrQueryFailCount();
        }
        queryMetrics.incrQueryCount();
    }

    private static void incrCacheHitCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (sqlResponse.isStorageCacheUsed()) {
            queryMetrics.addCacheHitCount(1);
        }
    }

    private static QueryMetrics getQueryMetrics(String name, ConcurrentHashMap<String, QueryMetrics> metricsMap) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] intervals = config.getQueryMetricsPercentilesIntervals();//解析数组

        if (metricsMap.containsKey(name)) {
            return metricsMap.get(name);
        } else {
            QueryMetrics queryMetrics = new QueryMetrics(intervals).registerWith(name);
            metricsMap.put(name, queryMetrics);
            return queryMetrics;
        }
    }
}
