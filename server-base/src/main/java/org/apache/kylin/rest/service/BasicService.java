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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hybrid.HybridManager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public abstract class BasicService {

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public StreamingManager getStreamingManager() {
        return StreamingManager.getInstance(getConfig());
    }

    public KafkaConfigManager getKafkaManager() throws IOException {
        return KafkaConfigManager.getInstance(getConfig());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public HybridManager getHybridManager() {
        return HybridManager.getInstance(getConfig());
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public BadQueryHistoryManager getBadQueryHistoryManager() {
        return BadQueryHistoryManager.getInstance(getConfig());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, final Map<String, Output> allOutputs) {
        return listAllCubingJobs(cubeName, projectName, statusList, -1L, -1L, allOutputs);
    }

    /**
     * 1.查找execute在timeStartInMillis和timeEndInMillis时间范围内的结果集
     * 2.进行过滤,只选择是cubeName和projectName的结果,或者cubeName=null和projectName=null的结果也会被选择
     * 3.在进行过滤,该job的状态必须在参数statusList集合中存在
     */
    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, long timeStartInMillis, long timeEndInMillis, final Map<String, Output> allOutputs) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(getExecutableManager().getAllExecutables(timeStartInMillis, timeEndInMillis)).filter(new Predicate<AbstractExecutable>() {
            @Override
            public boolean apply(AbstractExecutable executable) {
                if (executable instanceof CubingJob) {
                    if (cubeName == null) {
                        return true;
                    }
                    return CubingExecutableUtil.getCubeName(executable.getParams()).equalsIgnoreCase(cubeName);//必须cube名称与参数相同 或者匹配cubeName=null的结果
                } else {
                    return false;
                }
            }
        }).transform(new Function<AbstractExecutable, CubingJob>() {//转换,将 AbstractExecutable对象转换成CubingJob对象
            @Override
            public CubingJob apply(AbstractExecutable executable) {//进行强制转换
                return (CubingJob) executable;
            }
        }).filter(Predicates.and(new Predicate<CubingJob>() {//选择必须projectName也匹配参数的,或者projectName=null的
            @Override
            public boolean apply(CubingJob executable) {
                if (null == projectName || null == getProjectManager().getProject(projectName)) {
                    return true;
                } else {
                    ProjectInstance project = getProjectManager().getProject(projectName);
                    return project.containsRealization(RealizationType.CUBE, CubingExecutableUtil.getCubeName(executable.getParams()));//说明该project包含该cube
                }
            }
        }, new Predicate<CubingJob>() {//选择该job的输出状态在参数的状态集合中
            @Override
            public boolean apply(CubingJob executable) {
                try {
                    Output output = allOutputs.get(executable.getId());
                    ExecutableState state = output.getState();//获取该job对应的状态
                    boolean ret = statusList.contains(state);//该状态是否在参数集合中
                    return ret;
                } catch (Exception e) {
                    throw e;
                }
            }
        })));
        return results;
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList) {
        return listAllCubingJobs(cubeName, projectName, statusList, getExecutableManager().getAllOutputs());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName) {
        return listAllCubingJobs(cubeName, projectName, EnumSet.allOf(ExecutableState.class), getExecutableManager().getAllOutputs());
    }

}
