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

package org.apache.kylin.query.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;

/**
 * 表示一个project下的数据库
 * 属于calcite项目的接口,获取数据库下表的信息
 */
public class OLAPSchema extends AbstractSchema {

    //    private static final Logger logger = LoggerFactory.getLogger(OLAPSchema.class);

    private KylinConfig config;

    private String projectName;//项目名称
    private String schemaName;//数据库名称

    private String storageUrl;//如何访问hbase

    //数据库的登录url  user  密码
    private String starSchemaUrl;
    private String starSchemaUser;
    private String starSchemaPassword;

    public OLAPSchema(String project, String schemaName) {
        this.projectName = ProjectInstance.getNormalizedProjectName(project);
        this.schemaName = schemaName;
        init();
    }

    private void init() {
        this.config = KylinConfig.getInstanceFromEnv();
        this.storageUrl = config.getStorageUrl();
        this.starSchemaUrl = config.getHiveUrl();
        this.starSchemaUser = config.getHiveUser();
        this.starSchemaPassword = config.getHivePassword();
    }

    /**
     * It is intended to skip caching, because underlying project/tables might change.
     * 为calcite加载表结构
     * @return
     */
    @Override
    protected Map<String, Table> getTableMap() {
        return buildTableMap();
    }

    //获取该product下所有的table集合---数据库名字必须是schemaName
    private Map<String, Table> buildTableMap() {
        Map<String, Table> olapTables = new HashMap<String, Table>();
        Set<TableDesc> projectTables = ProjectManager.getInstance(config).listExposedTables(projectName);//获取该product下所有的table集合

        for (TableDesc tableDesc : projectTables) {
            if (tableDesc.getDatabase().equals(schemaName)) {//找到符合该数据库
                final String tableName = tableDesc.getName();//safe to use tableDesc.getName() here, it is in a DB context now
                final OLAPTable table = new OLAPTable(this, tableDesc);
                olapTables.put(tableName, table);
                //            logger.debug("Project " + projectName + " exposes table " + tableName);
            }
        }

        return olapTables;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getStorageUrl() {
        return storageUrl;
    }

    public boolean hasStarSchemaUrl() {
        return starSchemaUrl != null && !starSchemaUrl.isEmpty();
    }

    public String getStarSchemaUrl() {
        return starSchemaUrl;
    }

    public String getStarSchemaUser() {
        return starSchemaUser;
    }

    public String getStarSchemaPassword() {
        return starSchemaPassword;
    }

    public MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public String getProjectName() {
        return this.projectName;
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(config);
    }

}
