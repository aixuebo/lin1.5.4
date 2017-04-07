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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);
    private static final ConcurrentHashMap<KylinConfig, ProjectManager> CACHE = new ConcurrentHashMap<KylinConfig, ProjectManager>();

    //json序列化该对象
    public static final Serializer<ProjectInstance> PROJECT_SERIALIZER = new JsonSerializer<ProjectInstance>(ProjectInstance.class);

    public static ProjectManager getInstance(KylinConfig config) {
        ProjectManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (ProjectManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new ProjectManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init ProjectManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    private ProjectL2Cache l2Cache;
    // project name => ProjrectInstance
    private CaseInsensitiveStringCache<ProjectInstance> projectMap;//key是project的name,value是对应的project对象

    private ProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing ProjectManager with metadata url " + config);
        this.config = config;
        this.projectMap = new CaseInsensitiveStringCache<ProjectInstance>(config, Broadcaster.TYPE.PROJECT);
        this.l2Cache = new ProjectL2Cache(this);

        reloadAllProjects();
    }

    public void clearL2Cache() {
        l2Cache.clear();
    }

    private void reloadAllProjects() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.PROJECT_RESOURCE_ROOT, ".json");

        logger.debug("Loading Project from folder " + store.getReadableResourcePath(ResourceStore.PROJECT_RESOURCE_ROOT));

        for (String path : paths) {
            reloadProjectLocalAt(path);
        }
        logger.debug("Loaded " + projectMap.size() + " Project(s)");
    }

    public ProjectInstance reloadProjectLocal(String project) throws IOException {
        return reloadProjectLocalAt(ProjectInstance.concatResourcePath(project));
    }

    private ProjectInstance reloadProjectLocalAt(String path) throws IOException {

        ProjectInstance projectInstance = getStore().getResource(path, ProjectInstance.class, PROJECT_SERIALIZER);
        if (projectInstance == null) {
            logger.warn("reload project at path:" + path + " not found, this:" + this.toString());
            return null;
        }

        projectInstance.init();

        projectMap.putLocal(projectInstance.getName(), projectInstance);
        clearL2Cache();

        return projectInstance;
    }

    public List<ProjectInstance> listAllProjects() {
        return new ArrayList<ProjectInstance>(projectMap.values());
    }

    //获取该project对象
    public ProjectInstance getProject(String projectName) {
        projectName = norm(projectName);//对name进行格式化
        return projectMap.get(projectName);
    }

    //创建一个project,有name和创建者还有描述
    public ProjectInstance createProject(String projectName, String owner, String description) throws IOException {
        logger.info("Creating project " + projectName);

        ProjectInstance currentProject = getProject(projectName);
        if (currentProject == null) {
            currentProject = ProjectInstance.create(projectName, owner, description, null, null);
        } else {
            throw new IllegalStateException("The project named " + projectName + "already exists");
        }

        //序列化该对象
        updateProject(currentProject);

        return currentProject;
    }

    //删除一个project的存储以及映射
    public ProjectInstance dropProject(String projectName) throws IOException {
        if (projectName == null)
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(projectName);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + projectName + " does not exist");
        }

        if (projectInstance.getRealizationCount(null) != 0) {
            throw new IllegalStateException("The project named " + projectName + " can not be deleted because there's still realizations in it. Delete them first.");
        }

        logger.info("Dropping project '" + projectInstance.getName() + "'");

        removeProject(projectInstance);
        BadQueryHistoryManager.getInstance(config).removeBadQueryHistory(projectName);//删除该project相关联的bad query,因为该project已经被删除了

        return projectInstance;
    }

    //passive update due to underlying realization update
    public void updateProject(RealizationType type, String realizationName) throws IOException {
        for (ProjectInstance proj : findProjects(type, realizationName)) {
            updateProject(proj);
        }
    }

    //更新一个project,更新新的name和描述信息
    //update project itself
    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc) throws IOException {
        if (!project.getName().equals(newName)) {//说明name都更改了,则删除老的project,增加新的project
            ProjectInstance newProject = this.createProject(newName, project.getOwner(), newDesc);//创建新的project

            //新的project一部分属性使用老的project
            newProject.setCreateTimeUTC(project.getCreateTimeUTC());
            newProject.recordUpdateTime(System.currentTimeMillis());
            newProject.setRealizationEntries(project.getRealizationEntries());
            newProject.setTables(project.getTables());
            newProject.setModels(project.getModels());
            newProject.setExtFilters(project.getExtFilters());

            removeProject(project);
            updateProject(newProject);

            return newProject;
        } else {//做更新操作
            project.setName(newName);
            project.setDescription(newDesc);

            if (project.getUuid() == null)
                project.updateRandomUuid();//设置一个project的唯一ID

            updateProject(project);

            return project;
        }
    }

    //将该project对象序列化成json对象,存储到/project/projectName.json文件中
    private void updateProject(ProjectInstance prj) throws IOException {
        synchronized (prj) {
            getStore().putResource(prj.getResourcePath(), prj, PROJECT_SERIALIZER);
            projectMap.put(norm(prj.getName()), prj); // triggers update broadcast
            clearL2Cache();
        }
    }

    private void removeProject(ProjectInstance proj) throws IOException {
        getStore().deleteResource(proj.getResourcePath());
        projectMap.remove(norm(proj.getName()));
        clearL2Cache();
    }

    //判断该项目是否有该model
    public boolean isModelInProject(String projectName, String modelName) {
        return this.getProject(projectName).containsModel(modelName);
    }

    //更新一个model属于该project
    public ProjectInstance updateModelToProject(String modelName, String newProjectName) throws IOException {
        removeModelFromProjects(modelName);//在所有的project中删除该model
        return addModelToProject(modelName, newProjectName);//向该project添加该model
    }

    //从所有的项目中移除该model
    public void removeModelFromProjects(String modelName) throws IOException {
        for (ProjectInstance projectInstance : findProjects(modelName)) {//查找包含该model的项目集合
            projectInstance.removeModel(modelName);//移除该model
            updateProject(projectInstance);//重新更新元数据,因为已经移除了一个model了
        }
    }

    //向该project添加该model
    private ProjectInstance addModelToProject(String modelName, String project) throws IOException {
        String newProjectName = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance newProject = getProject(newProjectName);
        if (newProject == null) {
            throw new IllegalArgumentException("Project " + newProjectName + " does not exist.");
        }
        newProject.addModel(modelName);
        updateProject(newProject);

        return newProject;
    }

    //移动该RealizationType到newProjectName这个project上
        public ProjectInstance moveRealizationToProject(RealizationType type, String realizationName, String newProjectName, String owner) throws IOException {
        removeRealizationsFromProjects(type, realizationName);//所有匹配规则的project,都要被移除,然后重新序列化
        return addRealizationToProject(type, realizationName, newProjectName, owner);
    }

    //对project添加RealizationType
    private ProjectInstance addRealizationToProject(RealizationType type, String realizationName, String project, String user) throws IOException {
        String newProjectName = norm(project);
        ProjectInstance newProject = getProject(newProjectName);
        if (newProject == null) {
            newProject = this.createProject(newProjectName, user, "This is a project automatically added when adding realization " + realizationName + "(" + type + ")");
        }
        newProject.addRealizationEntry(type, realizationName);
        updateProject(newProject);//更新元数据

        return newProject;
    }

    //所有匹配规则的project,都要被移除,然后重新序列化
    public void removeRealizationsFromProjects(RealizationType type, String realizationName) throws IOException {
        for (ProjectInstance projectInstance : findProjects(type, realizationName)) {//通过RealizationType和realizationName查找匹配的所有的project集合
            projectInstance.removeRealization(type, realizationName);//真正移除
            updateProject(projectInstance);//重新序列化该project
        }
    }

    //向该project添加一组表
    public ProjectInstance addTableDescToProject(String[] tableIdentities, String projectName) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        ProjectInstance projectInstance = getProject(projectName);
        for (String tableId : tableIdentities) {
            TableDesc table = metaMgr.getTableDesc(tableId);
            if (table == null) {
                throw new IllegalStateException("Cannot find table '" + table + "' in metadata manager");
            }
            projectInstance.addTable(table.getIdentity());
        }

        updateProject(projectInstance);//更新元数据
        return projectInstance;
    }

    //删除该project下的一个table
    public void removeTableDescFromProject(String tableIdentities, String projectName) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        ProjectInstance projectInstance = getProject(projectName);
        TableDesc table = metaMgr.getTableDesc(tableIdentities);
        if (table == null) {
            throw new IllegalStateException("Cannot find table '" + table + "' in metadata manager");
        }

        projectInstance.removeTable(table.getIdentity());
        updateProject(projectInstance);
    }

    //添加filter到一个project上
    public ProjectInstance addExtFilterToProject(String[] filters, String projectName) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        ProjectInstance projectInstance = getProject(projectName);
        for (String filterName : filters) {
            ExternalFilterDesc extFilter = metaMgr.getExtFilterDesc(filterName);
            if (extFilter == null) {
                throw new IllegalStateException("Cannot find external filter '" + filterName + "' in metadata manager");
            }
            projectInstance.addExtFilter(filterName);
        }

        updateProject(projectInstance);//更新元数据
        return projectInstance;
    }

    //删除一个filter
    public void removeExtFilterFromProject(String filterName, String projectName) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        ProjectInstance projectInstance = getProject(projectName);
        ExternalFilterDesc filter = metaMgr.getExtFilterDesc(filterName);
        if (filter == null) {
            throw new IllegalStateException("Cannot find external filter '" + filterName + "' in metadata manager");
        }

        projectInstance.removeExtFilter(filterName);
        updateProject(projectInstance);//更新元数据
    }

    //通过RealizationType和realizationName查找匹配的所有的project集合
    public List<ProjectInstance> findProjects(RealizationType type, String realizationName) {
        List<ProjectInstance> result = Lists.newArrayList();
        for (ProjectInstance prj : projectMap.values()) {
            for (RealizationEntry entry : prj.getRealizationEntries()) {
                if (entry.getType().equals(type) && entry.getRealization().equalsIgnoreCase(realizationName)) {
                    result.add(prj);
                    break;
                }
            }
        }
        return result;
    }

    //查找包含该model的项目集合
    private List<ProjectInstance> findProjects(String modelName) {
        List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
        for (ProjectInstance projectInstance : projectMap.values()) {
            if (projectInstance.containsModel(modelName)) {
                projects.add(projectInstance);
            }
        }

        return projects;
    }

    public ExternalFilterDesc getExternalFilterDesc(String project, String extFilter) {
        return l2Cache.getExternalFilterDesc(project, extFilter);
    }

    public Map<String, ExternalFilterDesc> listExternalFilterDescs(String project) {
        return l2Cache.listExternalFilterDesc(project);
    }

    public List<TableDesc> listDefinedTables(String project) throws IOException {
        return l2Cache.listDefinedTables(norm(project));
    }

    public Set<TableDesc> listExposedTables(String project) {
        return l2Cache.listExposedTables(norm(project));
    }

    public Set<ColumnDesc> listExposedColumns(String project, String table) {
        return l2Cache.listExposedColumns(norm(project), table);
    }

    public boolean isExposedTable(String project, String table) {
        return l2Cache.isExposedTable(norm(project), table);
    }

    public boolean isExposedColumn(String project, String table, String col) {
        return l2Cache.isExposedColumn(norm(project), table, col);
    }

    public Set<IRealization> listAllRealizations(String project) {
        return l2Cache.listAllRealizations(norm(project));
    }

    public Set<IRealization> getRealizationsByTable(String project, String tableName) {
        return l2Cache.getRealizationsByTable(norm(project), tableName.toUpperCase());
    }

    public List<IRealization> getOnlineRealizationByFactTable(String project, String factTable) {
        return l2Cache.getOnlineRealizationByFactTable(norm(project), factTable.toUpperCase());
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(norm(project), factTable.toUpperCase(), true);
    }

    public List<MeasureDesc> listEffectiveMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(norm(project), factTable.toUpperCase(), false);
    }

    KylinConfig getConfig() {
        return config;
    }

    ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    //对project的name进行格式化
    private String norm(String project) {
        return project;
    }

}
