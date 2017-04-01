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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.AclPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author jiazhong
 */
@Component("modelMgmtService")
public class ModelService extends BasicService {

    @Autowired
    private AccessService accessService;

    /**
     * 查找project下该modelName对应的DataModelDesc集合
     *
     * 注意
     * 1.如果modelName设置为null或者"",则返回所有匹配的DataModelDesc集合
     * 2.如果projectName为null,则返回所有的DataModelDesc集合,不与project相互匹配
     * @param modelName
     * @param projectName
     * @return
     * @throws IOException
     */
    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<DataModelDesc> listAllModels(final String modelName, final String projectName) throws IOException {
        List<DataModelDesc> models;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            models = getMetadataManager().getModels();//获取全部model集合,与project无关
        } else {
            models = getMetadataManager().getModels(projectName);//获取project下所有的model
        }

        List<DataModelDesc> filterModels = new ArrayList<DataModelDesc>();
        for (DataModelDesc modelDesc : models) {//过滤符合条件的model
            boolean isModelMatch = (null == modelName) || modelName.length() == 0 || modelDesc.getName().toLowerCase().equals(modelName.toLowerCase());

            if (isModelMatch) {
                filterModels.add(modelDesc);
            }
        }

        return filterModels;
    }

    //获取集合中一部分DataModelDesc,即类似用于分页
    public List<DataModelDesc> getModels(final String modelName, final String projectName, final Integer limit, final Integer offset) throws IOException {

        List<DataModelDesc> modelDescs = listAllModels(modelName, projectName);

        if (limit == null || offset == null) {
            return modelDescs;
        }

        if ((modelDescs.size() - offset) < limit) {
            return modelDescs.subList(offset, modelDescs.size());
        }

        return modelDescs.subList(offset, offset + limit);
    }

    //在project下创建一个model
    public DataModelDesc createModelDesc(String projectName, DataModelDesc desc) throws IOException {
        if (getMetadataManager().getDataModelDesc(desc.getName()) != null) {
            throw new InternalErrorException("The model named " + desc.getName() + " already exists");
        }
        DataModelDesc createdDesc = null;
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        createdDesc = getMetadataManager().createDataModelDesc(desc, projectName, owner);//在project下创建model,保存model到磁盘,更新project到磁盘

        //设置权限
        accessService.init(createdDesc, AclPermission.ADMINISTRATION);
        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdDesc, project);
        return createdDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public DataModelDesc updateModelAndDesc(DataModelDesc desc) throws IOException {

        getMetadataManager().updateDataModelDesc(desc);////保存model到磁盘
        return desc;
    }

    //删除model的时候要确定没有cube引用该model才可以被删除
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public void dropModel(DataModelDesc desc) throws IOException {

        //check cube desc exist
        List<CubeDesc> cubeDescs = getCubeDescManager().listAllDesc();//获取所有cube
        for (CubeDesc cubeDesc : cubeDescs) {
            if (cubeDesc.getModelName().equals(desc.getName())) {//确保cube没有引用该model的
                throw new InternalErrorException("Model referenced by cube,drop cubes under model and try again.");
            }
        }

        getMetadataManager().dropModel(desc);//删除该model,并且同步在project中删除该model

        accessService.clean(desc, true);
    }

    //即该table是否在所有的model中被使用,有任意一个model使用了该table,都会返回true
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public boolean isTableInAnyModel(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        tableName = dbTableName[0] + "." + dbTableName[1];
        return getMetadataManager().isTableInAnyModel(tableName);
    }

    //是否project下某一model使用了该表
    //即该table在project中的一个model里面使用了
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public boolean isTableInModel(String tableName, String projectName) throws IOException {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        tableName = dbTableName[0] + "." + dbTableName[1];
        return getMetadataManager().isTableInModel(tableName, projectName);//是否project下某一model使用了该表
    }
}
