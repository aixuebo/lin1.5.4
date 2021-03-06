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

package org.apache.kylin.metadata.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.realization.RealizationType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Project is a concept in Kylin similar to schema in DBMS
 * 表示一个project对象
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ProjectInstance extends RootPersistentEntity {

    public static final String DEFAULT_PROJECT_NAME = "DEFAULT"; //默认不填写project的话,则默认project的name

    //项目名称
    @JsonProperty("name")
    private String name;

    //创建人
    @JsonProperty("owner")
    private String owner;

    //项目存在状态
    @JsonProperty("status")
    private ProjectStatusEnum status;

    //创建时间
    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    //修改时间
    @JsonProperty("last_update_time")
    // FIXME why not RootPersistentEntity.lastModified??
    private String lastUpdateTime;

    //项目描述
    @JsonProperty("description")
    private String description;

    @JsonProperty("tables")
    private Set<String> tables = new TreeSet<String>();//该project下有哪些hive的table

    @JsonProperty("realizations")
    private List<RealizationEntry> realizationEntries;//表示该project下有哪些CubeInstance对象或者HybridInstance对象

    @JsonProperty("models")
    private List<String> models;//该project下有哪些model

    @JsonProperty("ext_filters")
    private Set<String> extFilters = new TreeSet<String>();

    //返回项目存储的全路径
    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String projectName) {
        return ResourceStore.PROJECT_RESOURCE_ROOT + "/" + projectName + ".json";
    }

    public static String getNormalizedProjectName(String project) {
        if (project == null)
            throw new IllegalStateException("Trying to normalized a project name which is null");

        return project.toUpperCase();
    }

    //创建一个项目
    public static ProjectInstance create(String name, String owner, String description, List<RealizationEntry> realizationEntries, List<String> models) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.updateRandomUuid();
        projectInstance.setName(name);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(description);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(System.currentTimeMillis());//创建时间
        if (realizationEntries != null)
            projectInstance.setRealizationEntries(realizationEntries);
        else
            projectInstance.setRealizationEntries(Lists.<RealizationEntry> newArrayList());
        if (models != null)
            projectInstance.setModels(models);
        else
            projectInstance.setModels(new ArrayList<String>());
        return projectInstance;
    }

    // ============================================================================

    public ProjectInstance() {
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setExtFilters(Set<String> extFilters) {
        this.extFilters = extFilters;
    }

    public ProjectStatusEnum getStatus() {
        return status;
    }

    public void setStatus(ProjectStatusEnum status) {
        this.status = status;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //true表示确实是包含该type和realization匹配的实现
    public boolean containsRealization(final RealizationType type, final String realization) {
        return Iterables.any(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    //该project移除一个RealizationType
    public void removeRealization(final RealizationType type, final String realization) {
        Iterables.removeIf(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    public List<RealizationEntry> getRealizationEntries(final RealizationType type) {
        if (type == null)
            return getRealizationEntries();

        return ImmutableList.copyOf(Iterables.filter(realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(@Nullable RealizationEntry input) {
                return input.getType() == type;
            }
        }));
    }

    public int getRealizationCount(final RealizationType type) {

        if (type == null)
            return this.realizationEntries.size();

        return Iterables.size(Iterables.filter(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type;
            }
        }));
    }

    public void addRealizationEntry(final RealizationType type, final String realizationName) {
        RealizationEntry pdm = new RealizationEntry();
        pdm.setType(type);
        pdm.setRealization(realizationName);
        this.realizationEntries.add(pdm);
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public boolean containsTable(String tableName) {
        return tables.contains(tableName.toUpperCase());
    }

    public void removeTable(String tableName) {
        tables.remove(tableName.toUpperCase());
    }

    public void addExtFilter(String extFilterName) {
        this.getExtFilters().add(extFilterName);
    }

    public void removeExtFilter(String filterName) {
        extFilters.remove(filterName);
    }

    public int getTablesCount() {
        return this.getTables().size();
    }

    public void addTable(String tableName) {
        this.getTables().add(tableName.toUpperCase());
    }

    public Set<String> getTables() {
        return tables;
    }

    public Set<String> getExtFilters() {
        return extFilters;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void recordUpdateTime(long timeMillis) {
        this.lastUpdateTime = formatTime(timeMillis);
    }

    public List<RealizationEntry> getRealizationEntries() {
        return realizationEntries;
    }

    public void setRealizationEntries(List<RealizationEntry> entries) {
        this.realizationEntries = entries;
    }

    public List<String> getModels() {
        return models;
    }

    public boolean containsModel(String modelName) {
        return models != null && models.contains(modelName);
    }

    public void setModels(List<String> models) {
        this.models = models;
    }

    //向该project添加该model
    public void addModel(String modelName) {
        if (this.getModels() == null) {
            this.setModels(new ArrayList<String>());
        }
        this.getModels().add(modelName);
    }

    public void removeModel(String modelName) {
        if (this.getModels() != null) {
            this.getModels().remove(modelName);
        }
    }

    public void init() {
        if (name == null)
            name = ProjectInstance.DEFAULT_PROJECT_NAME;

        if (realizationEntries == null) {
            realizationEntries = new ArrayList<RealizationEntry>();
        }

        if (tables == null)
            tables = new TreeSet<String>();

        if (StringUtils.isBlank(this.name))
            throw new IllegalStateException("Project name must not be blank");
    }

    @Override
    public String toString() {
        return "ProjectDesc [name=" + name + "]";
    }

}
