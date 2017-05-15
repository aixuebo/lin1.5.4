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

package org.apache.kylin.dict;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.source.ReadableTable.TableSignature;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 字典的描述对象
 * 一个表的一个字段的值进行字典处理
 * 注意:一个表一个字典可能包含多个DictionaryInfo对象,因为不同segment可能就包含一个该字典DictionaryInfo对象了
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DictionaryInfo extends RootPersistentEntity {

    @JsonProperty("source_table")
    private String sourceTable;//表名
    @JsonProperty("source_column")
    private String sourceColumn;//字段名
    @JsonProperty("source_column_index")
    private int sourceColumnIndex; // 0 based  该字段的索引,从0开始计数
    @JsonProperty("data_type")
    private String dataType;//该字段的类型
    @JsonProperty("input")
    private TableSignature input;//该字段对应的表的签名,比如最后修改时间等
    @JsonProperty("dictionary_class")
    private String dictionaryClass;//该字典使用什么类去存储
    @JsonProperty("cardinality")
    private int cardinality;//该字典容纳了多少条不同的数据值

    transient Dictionary<?> dictionaryObject; //对应的字典对象,即dictionaryClass的实现类---包含了字典的实现class,以及字典存储的全部内容

    public DictionaryInfo() {
    }

    public DictionaryInfo(String sourceTable, String sourceColumn, int sourceColumnIndex, String dataType, TableSignature input) {

        this.updateRandomUuid();

        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.sourceColumnIndex = sourceColumnIndex;
        this.dataType = dataType;
        this.input = input;
    }

    public DictionaryInfo(DictionaryInfo other) {

        this.updateRandomUuid();

        this.sourceTable = other.sourceTable;
        this.sourceColumn = other.sourceColumn;
        this.sourceColumnIndex = other.sourceColumnIndex;
        this.dataType = other.dataType;
        this.input = other.input;
    }

    // ----------------------------------------------------------------------------

    public String getResourcePath() {
        return ResourceStore.DICT_RESOURCE_ROOT + "/" + sourceTable + "/" + sourceColumn + "/" + uuid + ".dict";//每一个表-字段,存在多个字典,因此使用uuid进行标识
    }

    //获取该表--字段对应的所有字典的根目录
    public String getResourceDir() {
        return ResourceStore.DICT_RESOURCE_ROOT + "/" + sourceTable + "/" + sourceColumn;
    }

    // ----------------------------------------------------------------------------

    // to decide if two dictionaries are built on the same table/column,
    // regardless of their signature
    //true表示两个字典对应的是相同的
    public boolean isDictOnSameColumn(DictionaryInfo other) {
        return this.sourceTable.equalsIgnoreCase(other.sourceTable) && this.sourceColumn.equalsIgnoreCase(other.sourceColumn) && this.sourceColumnIndex == other.sourceColumnIndex
                && this.dataType.equalsIgnoreCase(other.dataType) && this.dictionaryClass.equalsIgnoreCase(other.dictionaryClass);
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public int getSourceColumnIndex() {
        return sourceColumnIndex;
    }

    public void setSourceColumnIndex(int sourceColumnIndex) {
        this.sourceColumnIndex = sourceColumnIndex;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public TableSignature getInput() {
        return input;
    }

    public void setInput(TableSignature input) {
        this.input = input;
    }

    public String getDictionaryClass() {
        return dictionaryClass;
    }

    public void setDictionaryClass(String dictionaryClass) {
        this.dictionaryClass = dictionaryClass;
    }

    public Dictionary<?> getDictionaryObject() {
        return dictionaryObject;
    }

    public void setDictionaryObject(Dictionary<?> dictionaryObject) {
        this.dictionaryObject = dictionaryObject;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

}
