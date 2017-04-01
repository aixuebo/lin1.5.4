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

package org.apache.kylin.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Serves (and caches) metadata for Kylin instance.
 * <p/>
 * Also provides a ResourceStore for general purpose data persistence. 
 * Metadata is serialized as JSON and stored in ResourceStore.
 * 
 * @author yangli9
 * 用于提供所有的元数据信息
 */
public class MetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

    //三种结构如何序列化
    public static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(DataModelDesc.class);
    public static final Serializer<ExternalFilterDesc> EXTERNAL_FILTER_DESC_SERIALIZER = new JsonSerializer<ExternalFilterDesc>(ExternalFilterDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, MetadataManager> CACHE = new ConcurrentHashMap<KylinConfig, MetadataManager>();

    public static MetadataManager getInstance(KylinConfig config) {
        MetadataManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (MetadataManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new MetadataManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }

                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init MetadataManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // table name ==> SourceTable
    private CaseInsensitiveStringCache<TableDesc> srcTableMap;//存储所有的hive中加载的table信息

    // name => value 存放该table的元数据参数信息,key是database.table,value是一个key-value的键值对信息
    private CaseInsensitiveStringCache<Map<String, String>> srcTableExdMap;
    // name => DataModelDesc
    private CaseInsensitiveStringCache<DataModelDesc> dataModelDescMap;//存储所有的model信息
    // name => External Filter Desc
    private CaseInsensitiveStringCache<ExternalFilterDesc> extFilterMap;

    private MetadataManager(KylinConfig config) throws IOException {
        init(config);
    }

    /**
     * Tell MetadataManager that the instance has changed. The cube info will
     * be stored Reload the cube desc and source table A broadcast must be sent
     * out
     * 
     * @return
     * @throws IOException
     * 重新从存储器上加载json文件,加载到内存中
     */
    public void reload() {
        clearCache();
        getInstance(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<DataModelDesc> listDataModels() {
        return Lists.newArrayList(this.dataModelDescMap.values());
    }

    public List<TableDesc> listAllTables() {
        return Lists.newArrayList(srcTableMap.values());
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        return Lists.newArrayList(extFilterMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap() {
        return Collections.unmodifiableMap(srcTableMap.getMap());
    }

    public Map<String, Map<String, String>> listAllTableExdMap() {
        return srcTableExdMap.getMap();
    }

    /**
     * Get ColumnDesc by name, like "table.column"
     * 参数demo:table.column
     * 用于获取某一个列的信息
     */
    public ColumnDesc getColumnDesc(String tableDotColumnName) {
        int cut = tableDotColumnName.lastIndexOf('.');
        if (cut < 0)
            throw new IllegalArgumentException();

        String tableName = tableDotColumnName.substring(0, cut);
        String columnName = tableDotColumnName.substring(cut + 1);

        TableDesc table = getTableDesc(tableName);
        if (table == null)
            return null;

        return table.findColumnByName(columnName);
    }

    /**
     * Get TableDesc by name
     * 通过表名字获取一个表对象
     */
    public TableDesc getTableDesc(String tableName) {
        if (tableName.indexOf(".") < 0)
            tableName = "DEFAULT." + tableName;

        TableDesc result = srcTableMap.get(tableName.toUpperCase());
        return result;
    }

    public ExternalFilterDesc getExtFilterDesc(String filterTableName) {
        ExternalFilterDesc result = extFilterMap.get(filterTableName);
        return result;
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     * 
     * @param tableName 唯一标识符,用database.table表示
     * @return
     * 获取该table的元数据参数信息
     */
    public Map<String, String> getTableDescExd(String tableName) {
        String tableIdentity = tableName;
        Map<String, String> result = new HashMap<String, String>();
        if (srcTableExdMap.containsKey(tableIdentity)) {
            Map<String, String> tmp = srcTableExdMap.get(tableIdentity);
            Iterator<Entry<String, String>> it = tmp.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            result.put(MetadataConstants.TABLE_EXD_STATUS_KEY, "true");//true表示该table有key-value的属性信息存在,false表示不存在
        } else {
            result.put(MetadataConstants.TABLE_EXD_STATUS_KEY, "false");//true表示该table有key-value的属性信息存在,false表示不存在
        }
        return result;
    }

    //保存一个table
    public void saveSourceTable(TableDesc srcTable) throws IOException {
        if (srcTable.getUuid() == null || srcTable.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        //对table进行初始化
        srcTable.init();

        String path = srcTable.getResourcePath();//table的保存路径
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);//用json的方式将table信息存储到path路径下

        srcTableMap.put(srcTable.getIdentity(), srcTable);
    }

    //删除table的元和数据文件
    public void removeSourceTable(String tableIdentity) throws IOException {
        String path = TableDesc.concatResourcePath(tableIdentity);//table的保存路径
        getStore().deleteResource(path);//删除该路径对应的文件
        srcTableMap.remove(tableIdentity);//内存映射删除
    }

    public void saveExternalFilter(ExternalFilterDesc desc) throws IOException {
        if (desc.getUuid() == null) {
            throw new IllegalArgumentException("UUID not set.");
        }
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, EXTERNAL_FILTER_DESC_SERIALIZER);
        desc = reloadExternalFilterAt(path);
        extFilterMap.put(desc.getName(), desc);

    }

    public void removeExternalFilter(String name) throws IOException {
        String path = ExternalFilterDesc.concatResourcePath(name);
        getStore().deleteResource(path);
        extFilterMap.remove(name);

    }

    //重新从存储器上加载json文件,加载到内存中
    private void init(KylinConfig config) throws IOException {
        this.config = config;
        this.srcTableMap = new CaseInsensitiveStringCache<>(config, Broadcaster.TYPE.TABLE);
        this.srcTableExdMap = new CaseInsensitiveStringCache<>(config, Broadcaster.TYPE.TABLE);
        this.dataModelDescMap = new CaseInsensitiveStringCache<>(config, Broadcaster.TYPE.DATA_MODEL);
        this.extFilterMap = new CaseInsensitiveStringCache<>(config, Broadcaster.TYPE.EXTERNAL_FILTER);

        reloadAllSourceTable();
        reloadAllSourceTableExd();
        reloadAllDataModel();
        reloadAllExternalFilter();
    }

    //从存储器上加载table的额外的key-value信息
    private void reloadAllSourceTableExd() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableExdAt(path);//加载一个table的key-value文件到内存中
        }

        logger.debug("Loaded " + srcTableExdMap.size() + " SourceTable EXD(s)");
    }

    //加载一个table的key-value文件到内存中
    @SuppressWarnings("unchecked")
    private Map<String, String> reloadSourceTableExdAt(String path) throws IOException {
        Map<String, String> attrs = Maps.newHashMap();

        ResourceStore store = getStore();
        RawResource res = store.getResource(path);
        if (res == null) {
            logger.warn("Failed to get table exd info from " + path);
            return null;
        }

        InputStream is = res.inputStream;

        try {
            attrs.putAll(JsonUtil.readValue(is, HashMap.class));
        } finally {
            if (is != null)
                is.close();
        }

        // parse table identity from file name
        String file = path;
        if (file.indexOf("/") > -1) {
            file = file.substring(file.lastIndexOf("/") + 1);
        }
        String tableIdentity = file.substring(0, file.length() - MetadataConstants.FILE_SURFIX.length()).toUpperCase();

        srcTableExdMap.putLocal(tableIdentity, attrs);
        return attrs;
    }

    private void reloadAllExternalFilter() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading ExternalFilter from folder " + store.getReadableResourcePath(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT));

        extFilterMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadExternalFilterAt(path);
        }

        logger.debug("Loaded " + extFilterMap.size() + " ExternalFilter(s)");
    }

    private void reloadAllSourceTable() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable from folder " + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        srcTableMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableAt(path);
        }

        logger.debug("Loaded " + srcTableMap.size() + " SourceTable(s)");
    }

    private TableDesc reloadSourceTableAt(String path) throws IOException {
        ResourceStore store = getStore();
        TableDesc t = store.getResource(path, TableDesc.class, TABLE_SERIALIZER);
        if (t == null) {
            return null;
        }
        t.init();

        String tableIdentity = t.getIdentity();

        srcTableMap.putLocal(tableIdentity, t);

        return t;
    }

    private ExternalFilterDesc reloadExternalFilterAt(String path) throws IOException {
        ResourceStore store = getStore();
        ExternalFilterDesc t = store.getResource(path, ExternalFilterDesc.class, EXTERNAL_FILTER_DESC_SERIALIZER);
        if (t == null) {
            return null;
        }
        extFilterMap.putLocal(t.getName(), t);

        return t;
    }

    public void reloadExtFilter(String extFilterName) throws IOException {
        reloadExternalFilterAt(ExternalFilterDesc.concatResourcePath(extFilterName));
    }

    public void reloadSourceTableExt(String tableIdentity) throws IOException {
        reloadSourceTableExdAt(TableDesc.concatExdResourcePath(tableIdentity));
    }

    public void reloadSourceTable(String tableIdentity) throws IOException {
        reloadSourceTableAt(TableDesc.concatResourcePath(tableIdentity));
    }

    public void reloadTableCache(String tableIdentity) throws IOException {
        reloadSourceTableExt(tableIdentity);
        reloadSourceTable(tableIdentity);
    }

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
    }

    public List<DataModelDesc> getModels() {
        return new ArrayList<>(dataModelDescMap.values());
    }

    //获取project下所有的model
    public List<DataModelDesc> getModels(String projectName) throws IOException {
        ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(projectName);
        HashSet<DataModelDesc> ret = new HashSet<>();

        if (projectInstance != null && projectInstance.getModels() != null) {
            for (String modelName : projectInstance.getModels()) {
                DataModelDesc model = getDataModelDesc(modelName);
                if (null != model) {
                    ret.add(model);
                } else {
                    logger.error("Failed to load model" + modelName);
                }
            }
        }

        return new ArrayList<>(ret);
    }

    //是否project下某一model使用了该表
    //即该table在project中的一个model里面使用了
    public boolean isTableInModel(String tableName, String projectName) throws IOException {
        for (DataModelDesc modelDesc : getModels(projectName)) {
            if (modelDesc.getAllTables().contains(tableName.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    //即该table是否在所有的model中被使用,有任意一个model使用了该table,都会返回true
    public boolean isTableInAnyModel(String tableName) {
        for (DataModelDesc modelDesc : getModels()) {
            if (modelDesc.getAllTables().contains(tableName.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    private void reloadAllDataModel() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading DataModel from folder " + store.getReadableResourcePath(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT));

        dataModelDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            try {
                reloadDataModelDescAt(path);
            } catch (IllegalStateException e) {
                logger.error("Error to load DataModel at " + path, e);
                continue;
            }
        }

        logger.debug("Loaded " + dataModelDescMap.size() + " DataModel(s)");
    }

    public DataModelDesc reloadDataModelDesc(String name) {
        return reloadDataModelDescAt(DataModelDesc.concatResourcePath(name));
    }

    private DataModelDesc reloadDataModelDescAt(String path) {
        ResourceStore store = getStore();
        try {
            DataModelDesc dataModelDesc = store.getResource(path, DataModelDesc.class, MODELDESC_SERIALIZER);
            dataModelDesc.init(config, this.getAllTablesMap());
            dataModelDescMap.putLocal(dataModelDesc.getName(), dataModelDesc);
            return dataModelDesc;
        } catch (IOException e) {
            throw new IllegalStateException("Error to load" + path, e);
        }
    }

    // sync on update  删除该model,并且同步在project中删除该model
    public DataModelDesc dropModel(DataModelDesc desc) throws IOException {
        logger.info("Dropping model '" + desc.getName() + "'");
        ResourceStore store = getStore();
        if (desc != null)
            store.deleteResource(desc.getResourcePath());
        // delete model from project
        ProjectManager.getInstance(config).removeModelFromProjects(desc.getName());
        // clean model cache
        this.afterModelDropped(desc);
        return desc;
    }

    private void afterModelDropped(DataModelDesc desc) {
        removeModelCache(desc.getName());
    }

    public void removeModelCache(String modelName) {
        dataModelDescMap.remove(modelName);
    }

    //在project下创建model
    public DataModelDesc createDataModelDesc(DataModelDesc desc, String projectName, String owner) throws IOException {
        String name = desc.getName();
        if (dataModelDescMap.containsKey(name))
            throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");
        ProjectManager.getInstance(config).updateModelToProject(name, projectName);//将model赋予给一个project,并且保存了project到磁盘
        desc.setOwner(owner);
        return saveDataModelDesc(desc);//保存model到磁盘
    }

    //保存model到磁盘
    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        String name = desc.getName();
        if (!dataModelDescMap.containsKey(name)) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }

        return saveDataModelDesc(desc);
    }

    //保存model到磁盘
    private DataModelDesc saveDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        dataModelDesc.init(config, this.getAllTablesMap());

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }

    public void saveTableExd(String tableId, Map<String, String> tableExdProperties) throws IOException {
        if (tableId == null) {
            throw new IllegalArgumentException("tableId couldn't be null");
        }
        TableDesc srcTable = srcTableMap.get(tableId);
        if (srcTable == null) {
            throw new IllegalArgumentException("Couldn't find Source Table with identifier: " + tableId);
        }

        String path = TableDesc.concatExdResourcePath(tableId);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonUtil.writeValueIndent(os, tableExdProperties);
        os.flush();
        InputStream is = new ByteArrayInputStream(os.toByteArray());
        getStore().putResource(path, is, System.currentTimeMillis());
        os.close();
        is.close();

        srcTableExdMap.put(tableId, tableExdProperties);
    }

    public void removeTableExd(String tableIdentity) throws IOException {
        String path = TableDesc.concatExdResourcePath(tableIdentity);
        getStore().deleteResource(path);
        srcTableExdMap.remove(tableIdentity);
    }

    public String appendDBName(String table) {

        if (table.indexOf(".") > 0)
            return table;

        Map<String, TableDesc> map = getAllTablesMap();

        int count = 0;
        String result = null;
        for (TableDesc t : map.values()) {
            if (t.getName().equalsIgnoreCase(table)) {
                result = t.getIdentity();
                count++;
            }
        }

        if (count == 1)
            return result;

        if (count > 1) {
            logger.warn("There are more than 1 table named with '" + table + "' in different database; The program couldn't determine, randomly pick '" + result + "'");
        }
        return result;
    }

}
