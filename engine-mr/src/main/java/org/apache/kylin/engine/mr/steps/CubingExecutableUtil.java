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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.ExecutableContext;

import javax.annotation.Nullable;

public class CubingExecutableUtil {

    /**
     * cube执行过程中的参数信息
     */
    public static final String CUBE_NAME = "cubeName";//设置cube的名字
    public static final String SEGMENT_ID = "segmentId";//设置要执行的segment的uuid
    public static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";
    public static final String STATISTICS_PATH = "statisticsPath";//root/jobid/cube/statistics 统计的结果存储在哪里
    public static final String CUBING_JOB_ID = "cubingJobId";//执行该cube的jobID
    public static final String MERGED_STATISTICS_PATH = "mergedStatisticsPath";
    public static final String INDEX_PATH = "indexPath";//设置二级索引目录

    public static void setStatisticsPath(String path, Map<String, String> params) {
        params.put(STATISTICS_PATH, path);
    }

    public static String getStatisticsPath(Map<String, String> params) {
        return params.get(STATISTICS_PATH);
    }

    public static void setCubeName(String cubeName, Map<String, String> params) {
        params.put(CUBE_NAME, cubeName);
    }

    //获取cube名字
    public static String getCubeName(Map<String, String> params) {
        return params.get(CUBE_NAME);
    }

    public static void setSegmentId(String segmentId, Map<String, String> params) {
        params.put(SEGMENT_ID, segmentId);
    }

    //获取segmentID
    public static String getSegmentId(Map<String, String> params) {
        return params.get(SEGMENT_ID);
    }

    public static void setMergingSegmentIds(List<String> ids, Map<String, String> params) {
        params.put(MERGING_SEGMENT_IDS, StringUtils.join(ids, ","));
    }

    //查找指定cube中的指定segment
    public static CubeSegment findSegment(ExecutableContext context, String cubeName, String segmentId) {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(cubeName);

        if (cube == null) {
            //打印所有的cube名称,通知用户这些cube中没有查找的cube
            String cubeList = StringUtils.join(Iterables.transform(mgr.listAllCubes(), new Function<CubeInstance, String>() {
                @Nullable
                @Override
                public String apply(@Nullable CubeInstance input) {
                    return input.getName();
                }
            }).iterator(), ",");

            throw new IllegalStateException("target cube name: " + cubeName + " cube list: " + cubeList);
        }

        final CubeSegment newSegment = cube.getSegmentById(segmentId);

        if (newSegment == null) {
            //打印所有的segment名称,通知用户该cube中没有查找的segment
            String segmentList = StringUtils.join(Iterables.transform(cube.getSegments(), new Function<CubeSegment, String>() {
                @Nullable
                @Override
                public String apply(@Nullable CubeSegment input) {
                    return input.getUuid();
                }
            }).iterator(), ",");

            throw new IllegalStateException("target segment id: " + segmentId + " segment list: " + segmentList);
        }
        return newSegment;
    }

    //获取要合并的segment集合
    public static List<String> getMergingSegmentIds(Map<String, String> params) {
        final String ids = params.get(MERGING_SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public static void setCubingJobId(String id, Map<String, String> params) {
        params.put(CUBING_JOB_ID, id);
    }

    public static String getCubingJobId(Map<String, String> params) {
        return params.get(CUBING_JOB_ID);
    }

    public static void setMergedStatisticsPath(String path, Map<String, String> params) {
        params.put(MERGED_STATISTICS_PATH, path);
    }

    public static String getMergedStatisticsPath(Map<String, String> params) {
        return params.get(MERGED_STATISTICS_PATH);
    }

    //设置二级索引目录
    public static void setIndexPath(String indexPath, Map<String, String> params) {
        params.put(INDEX_PATH, indexPath);
    }

    public static String getIndexPath(Map<String, String> params) {
        return params.get(INDEX_PATH);
    }

}
