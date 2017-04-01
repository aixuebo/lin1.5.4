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

package org.apache.kylin.cube.model;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DimensionDesc {

    @JsonProperty("name")
    private String name;//该维度属性名字,一般为库.表.列  或者库.表.derived,或者自定义的名字
    @JsonProperty("table")
    private String table;//该维度属于哪个表的一个列
    @JsonProperty("column")
    private String column;//对应的列名称
    @JsonProperty("derived")
    private String[] derived;//如果是derived的时候,这个是一组列的集合,表示这个维度不是一个单独的列

    //init的时候初始化以下2个变量
    private TableDesc tableDesc;//table对应的表对象
    private JoinDesc join;//找到该model中该表的join关系
    // computed
    private TblColRef[] columnRefs;//fact_table中对应的列映射关系,该映射是on语法使用的列集合

    public void init(CubeDesc cubeDesc, Map<String, TableDesc> tables) {
        if (name != null)
            name = name.toUpperCase();

        if (table != null)
            table = table.toUpperCase();

        tableDesc = tables.get(this.getTable());
        if (tableDesc == null)
            throw new IllegalStateException("Can't find table " + table + " for dimension " + name);

        join = null;
        for (LookupDesc lookup : cubeDesc.getModel().getLookups()) {//找到该model中该表的join关系
            if (lookup.getTable().equalsIgnoreCase(this.getTable())) {
                join = lookup.getJoin();
                break;
            }
        }

        //        if (isHierarchy && this.column.length > 0) {
        //            List<HierarchyDesc> hierarchyList = new ArrayList<HierarchyDesc>(3);
        //            for (int i = 0, n = this.column.length; i < n; i++) {
        //                String aColumn = this.column[i];
        //                HierarchyDesc aHierarchy = new HierarchyDesc();
        //                aHierarchy.setLevel(String.valueOf(i + 1));
        //                aHierarchy.setColumn(aColumn);
        //                hierarchyList.add(aHierarchy);
        //            }
        //
        //            this.hierarchy = hierarchyList.toArray(new HierarchyDesc[hierarchyList.size()]);
        //        }
        //
        //        if (hierarchy != null && hierarchy.length == 0)
        //            hierarchy = null;

        //        if (hierarchy != null) {
        //            for (HierarchyDesc h : hierarchy)
        //                h.setColumn(h.getColumn().toUpperCase());
        //        }

        if (derived != null && derived.length == 0) {
            derived = null;
        }
        //全部大写
        if (derived != null) {
            StringUtil.toUpperCaseArray(derived, derived);
        }
        if (derived != null && join == null) {//derived必须是lookup 表,因此一定有join
            throw new IllegalStateException("Derived can only be defined on lookup table, cube " + cubeDesc + ", " + this);
        }

    }

    //是否该维度是derived方式的维度
    public boolean isDerived() {
        return derived != null;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public JoinDesc getJoin() {
        return join;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TblColRef[] getColumnRefs() {
        return this.columnRefs;
    }

    public void setColumnRefs(TblColRef[] colRefs) {
        this.columnRefs = colRefs;
    }

    public String getColumn() {
        return this.column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String[] getDerived() {
        return derived;
    }

    public void setDerived(String[] derived) {
        this.derived = derived;
    }

    public TableDesc getTableDesc() {
        return this.tableDesc;
    }

    @Override
    public boolean equals(Object o) {
        throw new NotImplementedException();
    }

    @Override
    public int hashCode() {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name).add("table", table).add("column", column).add("derived", Arrays.toString(derived)).add("join", join).toString();
    }
}
