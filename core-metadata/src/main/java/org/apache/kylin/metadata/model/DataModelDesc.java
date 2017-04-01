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

package org.apache.kylin.metadata.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 代表一个模型,即事实表和维度表的join组合,
 * 一个model可以被多个cube重复使用
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(DataModelDesc.class);
    public static enum RealizationCapacity {
        SMALL, MEDIUM, LARGE
    }
    
    private KylinConfig config;

    //第一页添加的数据
    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("description")
    private String description;

    //第二页添加事实表以及关联的lookup表
    //lookup表可以对事实表进行扩展属性,事实表引用lookup表的主键
    @JsonProperty("fact_table")
    private String factTable;

    //可以看到，kylin提供了两种join的方式：left join和inner join，本案例采用的是inner join的方式。以及关联条件
    @JsonProperty("lookups")
    private LookupDesc[] lookups;

    //第三页面添加数据
    //factTable表和LookupDesc表数量集合,比如有5个表join,则在这个页面会出现5行内容,每一个表对应一行数据,可以选择该表的一些属性,组装成总维度,即按照这些维度进行group by操作
    @JsonProperty("dimensions")
    private List<ModelDimensionDesc> dimensions;

    //第四个页面 选择度量的列集合
    @JsonProperty("metrics")
    private String[] metrics;//表示页面添加的Measures属性集合,即哪些列是可以作为维度值进行统计分析的

    //第5个页面
    @JsonProperty("filter_condition")
    private String filterCondition;//自定义的where过滤条件

    //如何分区,查询不同日期区间的数据
    @JsonProperty("partition_desc")
    PartitionDesc partitionDesc;

    @JsonProperty("capacity")
    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    private TableDesc factTableDesc;//事实表的元数据对象

    //添加lookup表的元数据对象
    private List<TableDesc> lookupTableDescs = Lists.newArrayList();

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();

    public KylinConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    //返回该model所有相关联的表名集合
    public Collection<String> getAllTables() {
        HashSet<String> ret = Sets.newHashSet();
        ret.add(factTable);
        for (LookupDesc lookupDesc : lookups)
            ret.add(lookupDesc.getTable());
        return ret;
    }

    public String getFactTable() {
        return factTable;
    }

    public TableDesc getFactTableDesc() {
        return factTableDesc;
    }

    public List<TableDesc> getLookupTableDescs() {
        return lookupTableDescs;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable.toUpperCase();
    }

    public LookupDesc[] getLookups() {
        return lookups;
    }

    public void setLookups(LookupDesc[] lookups) {
        this.lookups = lookups;
    }

    //是否是事实表
    public boolean isFactTable(String factTable) {
        return this.factTable.equalsIgnoreCase(factTable);
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public RealizationCapacity getCapacity() {
        return capacity;
    }

    public void setCapacity(RealizationCapacity capacity) {
        this.capacity = capacity;
    }

    /**
     * 通过事实表的外键,找到该外键对应哪个lookup表的主键
     * @param fk 事实表的外键
     * @param joinType join类型
     * @return 返回lookup表关联的列--问题是可以返回好多个lookp表啊,并不唯一,因此有什么意义呢
     */
    public TblColRef findPKByFK(TblColRef fk, String joinType) {
        assert isFactTable(fk.getTable());//确保fk对象是事实表(fact_table)的一个字段

        TblColRef candidate = null;//lookup表关联的列

        for (LookupDesc dim : lookups) {//循环所有关联的从表
            JoinDesc join = dim.getJoin();
            if (join == null)
                continue;

            if (joinType != null && !joinType.equals(join.getType()))//说明join类型不匹配
                continue;

            int find = ArrayUtils.indexOf(join.getForeignKeyColumns(), fk);//找到与主表的id所在外键索引
            if (find >= 0) {
                candidate = join.getPrimaryKeyColumns()[find];//通过该索引,找到lookup表的主键
                if (join.getForeignKeyColumns().length == 1) { // is single
                    // column join?
                    break;
                }
            }
        }
        return candidate;
    }

    //找到table信息
    public TableDesc findTable(String table) {
        if (factTableDesc.getName().equalsIgnoreCase(table) || factTableDesc.getIdentity().equalsIgnoreCase(table))
            return factTableDesc;

        for (TableDesc desc : lookupTableDescs) {
            if (desc.getName().equalsIgnoreCase(table) || desc.getIdentity().equalsIgnoreCase(table))
                return desc;
        }

        throw new IllegalArgumentException("Table not found by " + table);
    }

    // TODO let this replace CubeDesc.buildColumnNameAbbreviation()
    //找到列对应的列对象
    public ColumnDesc findColumn(String column) {
        ColumnDesc colDesc = null;

        int cut = column.lastIndexOf('.');//找到该类所属table
        if (cut > 0) {
            // table specified
            String table = column.substring(0, cut);//找到table名字
            TableDesc tableDesc = findTable(table);
            colDesc = tableDesc.findColumnByName(column.substring(cut + 1));
        } else {//只是有列名,没有列所在table信息,就全部遍历了
            // table not specified, try each table
            colDesc = factTableDesc.findColumnByName(column);//先从事实表查找该列
            if (colDesc == null) {
                for (TableDesc tableDesc : lookupTableDescs) {
                    colDesc = tableDesc.findColumnByName(column);
                    if (colDesc != null)
                        break;
                }
            }
        }

        if (colDesc == null)
            throw new IllegalArgumentException("Column not found by " + column);

        return colDesc;
    }

    //初始化该model
    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.config = config;
        this.factTable = this.factTable.toUpperCase();
        this.factTableDesc = tables.get(this.factTable.toUpperCase());
        if (factTableDesc == null) {
            throw new IllegalStateException("Fact table does not exist:" + this.factTable);
        }

        initJoinColumns(tables);//初始化join的列属性信息
        ModelDimensionDesc.capicalizeStrings(dimensions);//去大写字母
        initPartitionDesc(tables);//初始化分区信息
    }

    //初始化join的列属性信息
    private void initJoinColumns(Map<String, TableDesc> tables) {
        // join columns may or may not present in cube;
        // here we don't modify 'allColumns' and 'dimensionColumns';
        // initDimensionColumns() will do the update
        for (LookupDesc lookup : this.lookups) {//循环所有的lookup表
            lookup.setTable(lookup.getTable().toUpperCase());
            TableDesc dimTable = tables.get(lookup.getTable());
            if (dimTable == null) {
                throw new IllegalStateException("Table " + lookup.getTable() + " does not exist for " + this);
            }
            lookup.setTableDesc(dimTable);
            this.lookupTableDescs.add(dimTable);

            JoinDesc join = lookup.getJoin();
            if (join == null)
                continue;

            StringUtil.toUpperCaseArray(join.getForeignKey(), join.getForeignKey());
            StringUtil.toUpperCaseArray(join.getPrimaryKey(), join.getPrimaryKey());

            // primary key 设置主键信息
            String[] pks = join.getPrimaryKey();//lookup表中列
            TblColRef[] pkCols = new TblColRef[pks.length];//每一个列转换成TblColRef对象
            for (int i = 0; i < pks.length; i++) {
                ColumnDesc col = dimTable.findColumnByName(pks[i]);//从lookup表中找到对应的列对象
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + pks[i] + " in table " + dimTable.getIdentity());
                }
                TblColRef colRef = new TblColRef(col);
                pks[i] = colRef.getName();
                pkCols[i] = colRef;
            }
            join.setPrimaryKeyColumns(pkCols);

            // foreign key 设置外键信息
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {//从事实表中查找列对象
                ColumnDesc col = factTableDesc.findColumnByName(fks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + fks[i] + " in table " + this.getFactTable());
                }
                TblColRef colRef = new TblColRef(col);
                fks[i] = colRef.getName();
                fkCols[i] = colRef;
            }
            join.setForeignKeyColumns(fkCols);

            // Validate join in dimension
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + lookup.getTable() + ")" + Arrays.toString(pks) + " are not consistent with Foreign keys(" + this.getFactTable() + ") " + Arrays.toString(fks));
            }

            //校验列的类型要一致,否则打印警告日志
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    logger.warn("Primary key " + lookup.getTable() + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype() + " are not consistent with Foreign key " + this.getFactTable() + "." + fkCols[i].getName() + "." + fkCols[i].getDatatype());
                }
            }

        }
    }

    //初始化分区信息
    private void initPartitionDesc(Map<String, TableDesc> tables) {
        if (this.partitionDesc != null)
            this.partitionDesc.init(tables);
    }

    /** * Add error info and thrown exception out
     *
     * @param message
     */
    public void addError(String message) {
        addError(message, false);
    }

    /**
     * @param message
     *            error message
     * @param silent
     *            if throw exception
     */
    public void addError(String message, boolean silent) {
        if (!silent) {
            throw new IllegalStateException(message);
        } else {
            this.errors.add(message);
        }
    }

    public List<String> getError() {
        return this.errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataModelDesc modelDesc = (DataModelDesc) o;

        if (!name.equals(modelDesc.name))
            return false;
        if (!getFactTable().equals(modelDesc.getFactTable()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + getFactTable().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataModelDesc [name=" + name + "]";
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
        return ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public List<ModelDimensionDesc> getDimensions() {
        return dimensions;
    }

    public String[] getMetrics() {
        return metrics;
    }

    public void setDimensions(List<ModelDimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public void setMetrics(String[] metrics) {
        this.metrics = metrics;
    }

    public static DataModelDesc getCopyOf(DataModelDesc dataModelDesc) {
        DataModelDesc newDataModelDesc = new DataModelDesc();
        newDataModelDesc.setName(dataModelDesc.getName());
        newDataModelDesc.setDescription(dataModelDesc.getDescription());
        newDataModelDesc.setDimensions(dataModelDesc.getDimensions());
        newDataModelDesc.setFilterCondition(dataModelDesc.getFilterCondition());
        newDataModelDesc.setFactTable(dataModelDesc.getFactTable());
        newDataModelDesc.setLookups(dataModelDesc.getLookups());
        newDataModelDesc.setMetrics(dataModelDesc.getMetrics());
        newDataModelDesc.setPartitionDesc(PartitionDesc.getCopyOf(dataModelDesc.getPartitionDesc()));
        newDataModelDesc.updateRandomUuid();
        return newDataModelDesc;
    }
}
