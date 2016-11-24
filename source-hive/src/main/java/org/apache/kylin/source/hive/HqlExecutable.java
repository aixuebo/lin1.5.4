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

package org.apache.kylin.source.hive;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.datanucleus.store.types.backed.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

/**
 * 执行一组hql命令
 */
public class HqlExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(HqlExecutable.class);

    private static final String HQL = "hql";//该key存储一组hql集合
    private static final String HIVE_CONFIG = "hive-config";//该key存放josn类型的配置信息

    public HqlExecutable() {
        super();
    }

    //真正的工作类
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Map<String, String> configMap = getConfiguration();//获取配置信息
            HiveClient hiveClient = new HiveClient(configMap);

            for (String hql : getHqls()) {//依次执行一组sql
                hiveClient.executeHQL(hql);
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED);//返回成功
        } catch (Exception e) {
            logger.error("error run hive query:" + getHqls(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());//返回失败
        }
    }

    //设置配置信息
    public void setConfiguration(Map<String, String> configMap) {
        if (configMap != null) {
            String configStr = "";
            try {
                configStr = JsonUtil.writeValueAsString(configMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            setParam(HIVE_CONFIG, configStr);
        }
    }

    //获取配置信息
    @SuppressWarnings("unchecked")
    private Map<String, String> getConfiguration() {
        String configStr = getParam(HIVE_CONFIG);
        Map<String, String> result = null;
        if (configStr != null) {
            try {
                result = JsonUtil.readValue(configStr, HashMap.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    //设置一组hql集合
    public void setHqls(List<String> hqls) {
        setParam(HQL, StringUtils.join(hqls, ";"));
    }

    //获取一组hql集合
    private List<String> getHqls() {
        final String hqls = getParam(HQL);
        if (hqls != null) {
            return Lists.newArrayList(StringUtils.split(hqls, ";"));
        } else {
            return Collections.emptyList();
        }
    }

}
