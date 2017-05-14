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

package org.apache.kylin.job.dao;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

/**
 * 简单的描述一个任务AbstractExecutable,即描述一个job
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExecutablePO extends RootPersistentEntity {

    @JsonProperty("name")
    private String name;

    @JsonProperty("tasks")
    private List<ExecutablePO> tasks;//如果该AbstractExecutable是一个ChainedExecutable任务,因此会包含多个子任务,这个集合就是存储子任务的,这集合里面的任务也是每一个任务有一个任务的输出对象的

    @JsonProperty("type")
    private String type;//AbstractExecutable对应的class全路径,该class一定是AbstractExecutable的子类

    @JsonProperty("params")
    private Map<String, String> params = Maps.newHashMap();//任务的执行命令参数集合

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ExecutablePO> getTasks() {
        return tasks;
    }

    public void setTasks(List<ExecutablePO> tasks) {
        this.tasks = tasks;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

}
