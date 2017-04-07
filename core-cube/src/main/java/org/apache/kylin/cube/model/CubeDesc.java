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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 描述一个cube的详细内容
 *
 * 实现IEngineAware,表示提供一个cube的执行引擎
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity implements IEngineAware {
    private static final Logger logger = LoggerFactory.getLogger(CubeDesc.class);

    public static class CannotFilterExtendedColumnException extends RuntimeException {
        public CannotFilterExtendedColumnException(TblColRef tblColRef) {
            super(tblColRef == null ? "null" : tblColRef.getCanonicalName());
        }
    }

    //存在derive字段的维度表才有该对象
    public enum DeriveType {
        LOOKUP,
        PK_FK,//此时说明两个表join的时候on的属性
        EXTENDED_COLUMN //扩展属性
    }

    public static class DeriveInfo {
        public DeriveType type;
        public DimensionDesc dimension;//derve属性属于哪个维度表
        public TblColRef[] columns;//derve属性集合
        public boolean isOneToOne; // only used when ref from derived to host

        DeriveInfo(DeriveType type, DimensionDesc dimension, TblColRef[] columns, boolean isOneToOne) {
            this.type = type;
            this.dimension = dimension;
            this.columns = columns;
            this.isOneToOne = isOneToOne;
        }

        @Override
        public String toString() {
            return "DeriveInfo [type=" + type + ", dimension=" + dimension + ", columns=" + Arrays.toString(columns) + ", isOneToOne=" + isOneToOne + "]";
        }

    }

    private KylinConfigExt config;
    private DataModelDesc model;

    //第一个页面 添加cube的名字以及描述  在哪个model上建立cube  邮件通知事件 以及通知列表
    @JsonProperty("name")
    private String name;
    @JsonProperty("description")
    private String description;
    @JsonProperty("model_name")
    private String modelName;//该cube所属model
    @JsonProperty("notify_list")
    private List<String> notifyList;//邮件通知列表
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Collections.emptyList();//什么情况下会发送邮件通知

    //第二个页面 设置维度
    @JsonProperty("dimensions")
    private List<DimensionDesc> dimensions;

    //第三个页面  设置度量
    @JsonProperty("measures")
    private List<MeasureDesc> measures;

    //第四页 设置merge周期、丢弃周期、分区信息
    /**
     设置增量cube信息，首先需要选择事实表中的某一个时间类型的分区列（貌似只能是按照天进行分区），然后再指定本次构建的cube的时间范围（起始时间点和结束时间点），这一步的结果会作为原始数据查询的where条件，保证本次构建的cube只包含这个闭区间时间内的数据，
     如果事实表没有时间类型的分区别或者没有选择任何分区则表示数据不会动态更新，也就不可以增量的创建cube了。
     可以在该选项中选择分区字段，根据这个字段来获取每次预计算的输入数据区间，Kylin中将每一个区间计算的结果称之为一个Segment，预计算的结果存储在hbase的一个表中。通常情况下这个分区字段对应hive中的分区字段，以天为例子，每次预计算一天的数据。这个过程称之为build。

     除了build这种每个时间区间向前或者向后的新数据计算，还存在两种对已完成计算数据的处理方式。
     第一种称之为Refresh，当某个数据区间的原始数据（hive中）发生变化时，预计算的结果就会出现不一致，因此需要对这个区间的segment进行刷新，即重新计算。
     第二种称之为Merge，由于每一个输入区间对应着一个Segment，结果存储在一个htable中，久而久之就会出现大量的htable，如果一次查询涉及的时间跨度比较久会导致对很多表的扫描，性能下降，因此可以通过将多个segment合并成一个大的segment优化。但是merge不会对现有数据进行任何改变。
     说句题外话，在kylin中可以设置merge的时间区间，默认是7、28，表示每当build了前一天的数据就会自动进行一个merge，将这7天的数据放到一个segment中，当最近28天的数据计算完成之后再次出发merge，以减小扫描的htable数量。
     但是对于经常需要refresh的数据就不能这样设置了，因为一旦合并之后，刷新就需要将整个合并之后的segment进行刷新，这无疑是浪费的。
     注意：kylin不支持删除某一天的数据，如果不希望这一天数据存在，可以在hive中删除并重新refresh这段数据
     */
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;//设置Partition Start Date
    @JsonProperty("partition_date_end")
    private long partitionDateEnd = 3153600000000L;

    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;//自动分区周期,存储毫秒,比如设置7天  28天,则内容是604800000,2419200000

    @JsonProperty("retention_range")
    private long retentionRange = 0;//Retention Threshold值

    //第五页  高级设置 比如设置维度组
    /**
     1、设置Rowkey
     2、设置维度组
     3、设置Cube Size
     在进入到设置RowKey的时候会看到每一个维度的设置（Derived维度看到的是外键列而不是Derived的列），每一个维度可以设置ID（通过拖拽可以改变每一个维度的ID）、Mandatory、Dictionary和Length。
     设置rowkey，这一步的建议是看看就可以了，不要进行修改，除非对kylin内部实现有比较深的理解才能知道怎么去修改。当然这里有一个可以修改的是mandatory dimension，如果一个维度需要在每次查询的时候都出现，那么可以设置这个dimension为mandatory，可以省去很多存储空间，
     另外还可以对所有维度进行划分group，不会组合查询的dimension可以划分在不同的group中，这样也会降低存储空间。

     注意：new aggregation group，是kylin 1.5的新特性；
     老版本中的agg是需要选中所有可能被使用的纬度字段，以供查询；但存在高纬度的查询需求，例如查询某订单编号编号的数据，这时应该仅仅做filter，而不需要为此做cube，但在老版本的agg中，是不允许在group中来制定prune细节。
     而新的特性中，new agg允许设置field是compute（参与cube）还是skip（不参与cube）
     */
    @JsonProperty("aggregation_groups")
    private List<AggregationGroup> aggregationGroups;

    //设置字典
    @JsonProperty("dictionaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<DictionaryDesc> dictionaries;

    //设置rowkey关于字段的顺序
    @JsonProperty("rowkey")
    private RowKeyDesc rowkey;

    //第六页  设置key-value自定义属性
    @JsonProperty("override_kylin_properties")
    private LinkedHashMap<String, String> overrideKylinProps = new LinkedHashMap<String, String>();


    @JsonProperty("null_string")
    private String[] nullStrings;//设定一组字符串表示null,即出现这种字符串了,就表示null

    /**
用于存储度量属性
     "hbase_mapping": {
         "column_family": [
             {
                 "name": "F1",//表示标准的度量
                 "columns": [
                     {
                     "qualifier": "M",
                     "measure_refs": [
                                 "_COUNT_",
                                 "AMOUNT",
                                 "AA",
                                 "BB",
                                 "EE"
                                 ]
                     }
                 ]
             },
             {
                 "name": "F2",//表示count distinct度量
                 "columns": [
                     {
                         "qualifier": "M",
                         "measure_refs": [
                         "CC",
                         "GG"
                         ]
                     }
                 ]
             }
         ]
     },
     */
    @JsonProperty("hbase_mapping")
    private HBaseMappingDesc hbaseMapping;

    @JsonProperty("signature")
    private String signature;//MD5的签名

    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_MR_V1;//执行引擎是spark还是mr
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_HBASE;//存储到哪里


    //用于缓存所有用到的列集合,key是table,value是列的name和列对象组成的集合
    private Map<String, Map<String, TblColRef>> columnMap = new HashMap<String, Map<String, TblColRef>>();
    //该cube的所有的列集合  该cube支持的所有列,包括derived列
    private LinkedHashSet<TblColRef> allColumns = new LinkedHashSet<TblColRef>();
    //该cube的所有的维度列集合  该cube支持的所有列,包括derived列,但是不算度量的列
    private LinkedHashSet<TblColRef> dimensionColumns = new LinkedHashSet<TblColRef>();

    //该映射中的key说明是dervie相关的字段
    private Map<TblColRef, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    private Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

    //用于扩展字段
    private Map<TblColRef, DeriveInfo> extendedColumnToHosts = Maps.newHashMap();

    public boolean isEnableSharding() {
        //in the future may extend to other storage that is shard-able
        return storageType != IStorageAware.ID_HBASE && storageType != IStorageAware.ID_HYBRID;
    }

    public Set<TblColRef> getShardByColumns() {
        return getRowkey().getShardByColumns();
    }

    /**
     * Error messages during resolving json metadata
     * 存储异常信息
     */
    private List<String> errors = new ArrayList<String>();

    /**
     * @return all columns this cube can support, including derived
     * 该cube支持的所有列,包括derived列
     */
    public Set<TblColRef> listAllColumns() {
        return allColumns;
    }

    /**
     * @return dimension columns including derived, BUT NOT measures
     * 该cube支持的所有列,包括derived列,但是不算度量的列
     */
    public Set<TblColRef> listDimensionColumnsIncludingDerived() {
        return dimensionColumns;
    }

    /**
     * @param alsoExcludeExtendedCol  true表示也要包含扩展属性被刨除
     * @return dimension columns excluding derived
     * 刨除derived的列,并且参数为true的时候表示也要刨除扩展的列
     */
    public List<TblColRef> listDimensionColumnsExcludingDerived(boolean alsoExcludeExtendedCol) {
        List<TblColRef> result = new ArrayList<TblColRef>();
        for (TblColRef col : dimensionColumns) {//刨除derived的列
            if (isDerived(col)) {
                continue;
            }

            if (alsoExcludeExtendedCol && isExtendedColumn(col)) {//参数为true的时候表示也要刨除额外的列
                continue;
            }

            result.add(col);
        }
        return result;
    }

    /**
     * @return all functions from each measure.
     * 返回度量中所有的函数对象
     */
    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();
        for (MeasureDesc m : measures) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    //从内存中查找已经存在的列对象
    public TblColRef findColumnRef(String table, String column) {
        Map<String, TblColRef> cols = columnMap.get(table);
        if (cols == null)
            return null;
        else
            return cols.get(column);
    }

    //找到该lookup表对应的一个维度,该维度如果存在,肯定是返回一组derived列集合
    public DimensionDesc findDimensionByTable(String lookupTableName) {
        lookupTableName = lookupTableName.toUpperCase();
        for (DimensionDesc dim : dimensions)
            if (dim.getTable() != null && dim.getTable().equals(lookupTableName))
                return dim;
        return null;
    }

    public boolean hasHostColumn(TblColRef col) {
        return isDerived(col) || isExtendedColumn(col);
    }

    //true表是是dervied字段
    public boolean isDerived(TblColRef col) {
        return derivedToHostMap.containsKey(col);
    }

    //true表示是扩展字段
    public boolean isExtendedColumn(TblColRef col) {
        return extendedColumnToHosts.containsKey(col);
    }

    public DeriveInfo getHostInfo(TblColRef derived) {
        if (isDerived(derived)) {
            return derivedToHostMap.get(derived);
        } else if (isExtendedColumn(derived)) {
            return extendedColumnToHosts.get(derived);
        }
        throw new RuntimeException("Cannot get host info for " + derived);
    }

    public Map<Array<TblColRef>, List<DeriveInfo>> getHostToDerivedInfo(List<TblColRef> rowCols, Collection<TblColRef> wantedCols) {
        Map<Array<TblColRef>, List<DeriveInfo>> result = new HashMap<Array<TblColRef>, List<DeriveInfo>>();
        for (Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedMap.entrySet()) {
            Array<TblColRef> hostCols = entry.getKey();
            boolean hostOnRow = rowCols.containsAll(Arrays.asList(hostCols.data));
            if (!hostOnRow)
                continue;

            List<DeriveInfo> wantedInfo = new ArrayList<DeriveInfo>();
            for (DeriveInfo info : entry.getValue()) {
                //Collections.disjoint true表示两个集合没有公共元素
                if (wantedCols == null || Collections.disjoint(wantedCols, Arrays.asList(info.columns)) == false) // has any wanted columns?
                    wantedInfo.add(info);
            }

            if (wantedInfo.size() > 0)
                result.put(hostCols, wantedInfo);
        }
        return result;
    }

    //路径
    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
        return ResourceStore.CUBE_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    // ============================================================================

    public KylinConfig getConfig() {
        return config;
    }

    private void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public DataModelDesc getModel() {
        return model;
    }

    public void setModel(DataModelDesc model) {
        this.model = model;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    //获取该cube对应的事实表名字
    public String getFactTable() {
        return model.getFactTable();
    }

    //获取该cube对应的事实表对象
    public TableDesc getFactTableDesc() {
        return model.getFactTableDesc();
    }

    //获取该cube对应的关联表集合
    public List<TableDesc> getLookupTableDescs() {
        return model.getLookupTableDescs();
    }

    public String[] getNullStrings() {
        return nullStrings;
    }

    public List<DimensionDesc> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public void setMeasures(List<MeasureDesc> measures) {
        this.measures = measures;
    }

    public List<DictionaryDesc> getDictionaries() {
        return dictionaries;
    }

    void setDictionaries(List<DictionaryDesc> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public RowKeyDesc getRowkey() {
        return rowkey;
    }

    public void setRowkey(RowKeyDesc rowkey) {
        this.rowkey = rowkey;
    }

    public List<AggregationGroup> getAggregationGroups() {
        return aggregationGroups;
    }

    public void setAggregationGroups(List<AggregationGroup> aggregationGroups) {
        this.aggregationGroups = aggregationGroups;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public List<String> getNotifyList() {
        return notifyList;
    }

    public void setNotifyList(List<String> notifyList) {
        this.notifyList = notifyList;
    }

    public List<String> getStatusNeedNotify() {
        return statusNeedNotify;
    }

    public void setStatusNeedNotify(List<String> statusNeedNotify) {
        this.statusNeedNotify = statusNeedNotify;
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    private void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {
        this.overrideKylinProps = overrideKylinProps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CubeDesc cubeDesc = (CubeDesc) o;

        if (!name.equals(cubeDesc.name))
            return false;

        if (!getFactTable().equals(cubeDesc.getFactTable()))
            return false;

        return true;
    }

    public int getBuildLevel() {
        return Collections.max(Collections2.transform(aggregationGroups, new Function<AggregationGroup, Integer>() {
            @Nullable
            @Override
            public Integer apply(AggregationGroup input) {
                return input.getBuildLevel();
            }
        }));
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
        return "CubeDesc [name=" + name + "]";
    }

    /**
     * this method is to prevent malicious metadata change by checking the saved signature
     * with the calculated signature.
     * 该方法可以防止恶意的元数据更改
     * 
     * if you're comparing two cube descs, prefer to use consistentWith()
     * @return
     * 校验签名的正确性
     */
    public boolean checkSignature() {
        if (KylinVersion.getCurrentVersion().isCompatibleWith(new KylinVersion(getVersion())) && !KylinVersion.getCurrentVersion().isSignatureCompatibleWith(new KylinVersion(getVersion()))) {
            logger.info("checkSignature on {} is skipped as the its version is {} (not signature compatible but compatible) ", getName(), getVersion());
            return true;
        }

        if (StringUtils.isBlank(getSignature())) {
            return true;
        }

        String calculated = calculateSignature();//计算签名
        String saved = getSignature();//获取签名
        return calculated.equals(saved);//对比签名是否正确
    }

    public boolean consistentWith(CubeDesc another) {
        if (another == null)
            return false;
        return this.calculateSignature().equals(another.calculateSignature());
    }

    //计算签名
    public String calculateSignature() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|")//
                    .append(JsonUtil.writeValueAsString(this.modelName)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.nullStrings)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.dimensions)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.measures)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.rowkey)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.aggregationGroups)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.hbaseMapping)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.engineType)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.storageType)).append("|");

            String signatureInput = sigString.toString().replaceAll("\\s+", "").toLowerCase();

            byte[] signature = md.digest(signatureInput.getBytes());
            String ret = new String(Base64.encodeBase64(signature));
            return ret;
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }
    }

    //刨除derived的列,以及额外的列,返回剩余列的集合
    public Map<String, TblColRef> buildColumnNameAbbreviation() {
        Map<String, TblColRef> r = new CaseInsensitiveStringMap<TblColRef>();
        for (TblColRef col : listDimensionColumnsExcludingDerived(true)) {
            r.put(col.getName(), col);
        }
        return r;
    }

    /**
     * 1.初始化model对象
     * 2.校验分组内列的数据的有效性
     * 继承关系的列不允许存放在多个分组中
     * joint的列不允许存放在多个分组中
     * 继承的列在同一个分组中不允许少于2个
     * joint的列在同一个分组中不允许少于2个
     * 组合数量不能超过伐值
     * 3.对维度初始化
     * 4.对度量初始化
     */
    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.errors.clear();
        this.config = KylinConfigExt.createInstance(config, overrideKylinProps);//覆盖新的属性

        if (this.modelName == null || this.modelName.length() == 0) {//必须有模型
            this.addError("The cubeDesc '" + this.getName() + "' doesn't have data model specified.");
        }

        // check if aggregation group is valid
        //校验列分组后的有效性
        validate();

        //获取模型对象
        this.model = MetadataManager.getInstance(config).getDataModelDesc(this.modelName);

        if (this.model == null) {//如果出现问题,则添加日志
            this.addError("No data model found with name '" + modelName + "'.");
        }

        //初始化每一个维度
        for (DimensionDesc dim : dimensions) {
            dim.init(this, tables);
        }

        initDimensionColumns();
        initMeasureColumns();

        rowkey.init(this);
        for (AggregationGroup agg : this.aggregationGroups) {
            agg.init(this, rowkey);
        }

        if (hbaseMapping != null) {
            hbaseMapping.init(this);
        }

        initMeasureReferenceToColumnFamily();

        // check all dimension columns are presented on rowkey
        List<TblColRef> dimCols = listDimensionColumnsExcludingDerived(true);
        if (rowkey.getRowKeyColumns().length != dimCols.size()) {
            addError("RowKey columns count (" + rowkey.getRowKeyColumns().length + ") does not match dimension columns count (" + dimCols.size() + "). ");
        }

        initDictionaryDesc();
    }

    //校验
    public void validate() {
        int index = 0;//表示第几个组

        for (AggregationGroup agg : getAggregationGroups()) {
            if (agg.getIncludes() == null) {
                logger.error("Aggregation group " + index + " 'includes' field not set");
                throw new IllegalStateException("Aggregation group " + index + " includes field not set");
            }

            if (agg.getSelectRule() == null) {
                logger.error("Aggregation group " + index + " 'select_rule' field not set");
                throw new IllegalStateException("Aggregation group " + index + " select rule field not set");
            }

            int combination = 1;//总的组合方式数量
            Set<String> includeDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);//所有的维度集合
            getDims(includeDims, agg.getIncludes());//将agg.getIncludes()维度集合添加到includeDims中

            Set<String> mandatoryDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);//必须的列集合
            getDims(mandatoryDims, agg.getSelectRule().mandatory_dims);

            ArrayList<Set<String>> hierarchyDimsList = Lists.newArrayList();//每一组继承列组成一个Set
            Set<String> hierarchyDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);//所有的继承列集合
            getDims(hierarchyDimsList, hierarchyDims, agg.getSelectRule().hierarchy_dims);

            /**
             * 比如年月日组成 4组  年、年月、年月日 、无,因此是hierarchy.size() +1中组合
             */
            for (Set<String> hierarchy : hierarchyDimsList) {
                combination = combination * (hierarchy.size() + 1);
            }

            ArrayList<Set<String>> jointDimsList = Lists.newArrayList();//每一组join组成的一个Set集合
            Set<String> jointDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);//所有的join列
            getDims(jointDimsList, jointDims, agg.getSelectRule().joint_dims);
            if (jointDimsList.size() > 0) {
                combination = combination * (1 << jointDimsList.size());//2的jointDimsList.size()次方组合,即每一个join只能有一种组合
            }

            //必须includeDims包含所有的属性-----这个是一定的,页面选择的时候就已经控制了
            if (!includeDims.containsAll(mandatoryDims) || !includeDims.containsAll(hierarchyDims) || !includeDims.containsAll(jointDims)) {
                logger.error("Aggregation group " + index + " Include dims not containing all the used dims");
                throw new IllegalStateException("Aggregation group " + index + " Include dims not containing all the used dims");
            }

            //计算正常的列
            Set<String> normalDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            normalDims.addAll(includeDims);//先添加全部的列
            //刨除三种类型的列,剩余的就是正常的列
            normalDims.removeAll(mandatoryDims);
            normalDims.removeAll(hierarchyDims);
            normalDims.removeAll(jointDims);

            combination = combination * (1 << normalDims.size());//normalDims.size()次方组合,即每一个正常的列都是可能创造一种组合

            //配置的最多组合数量,即2的多少次方不能超过该值
            if (combination > config.getCubeAggrGroupMaxCombination()) {
                String msg = "Aggregation group " + index + " has too many combinations, use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max.combination' to a bigger value.";
                logger.error("Aggregation group " + index + " has " + combination + " combinations;");
                logger.error(msg);
                throw new IllegalStateException(msg);
            }

            //该方法表示两个参数集合是否有交集,一旦有一个元素是共同存在的,则都返回true
            /**
             * 源代码
             public static boolean containsAny(final Collection coll1, final Collection coll2) {
                 if (coll1.size() < coll2.size()) {
                     for (Iterator it = coll1.iterator(); it.hasNext();) {
                         if (coll2.contains(it.next())) {
                             return true;
                         }
                     }
                 } else {
                     for (Iterator it = coll2.iterator(); it.hasNext();) {
                         if (coll1.contains(it.next())) {
                              return true;
                         }
                     }
                 }
                     return false;
             }
             */
            if (CollectionUtils.containsAny(mandatoryDims, hierarchyDims)) {
                logger.warn("Aggregation group " + index + " mandatory dims overlap with hierarchy dims");
            }
            if (CollectionUtils.containsAny(mandatoryDims, jointDims)) {
                logger.warn("Aggregation group " + index + " mandatory dims overlap with joint dims");
            }

            if (CollectionUtils.containsAny(hierarchyDims, jointDims)) {
                logger.error("Aggregation group " + index + " hierarchy dims overlap with joint dims");
                throw new IllegalStateException("Aggregation group " + index + " hierarchy dims overlap with joint dims");
            }

            //必须至少两个元素-----继承关系肯定是至少有两个维度次才能体现继承的概念
            if (hasSingle(hierarchyDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dims in a hierarchy");
                throw new IllegalStateException("Aggregation group " + index + " require at least 2 dims in a hierarchy");
            }

            //必须至少两个元素
            if (hasSingle(jointDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dims in a joint");
                throw new IllegalStateException("Aggregation group " + index + " require at least 2 dims in a joint");
            }

            //每一个属性列不允许设置到不同分组中
            if (hasOverlap(hierarchyDimsList, hierarchyDims)) {
                logger.error("Aggregation group " + index + " a dim exist in more than one hierarchy");
                throw new IllegalStateException("Aggregation group " + index + " a dim exist in more than one hierarchy");
            }

            //每一个属性列不允许设置到不同分组中
            if (hasOverlap(jointDimsList, jointDims)) {
                logger.error("Aggregation group " + index + " a dim exist in more than one joint");
                throw new IllegalStateException("Aggregation group " + index + " a dim exist in more than one joint");
            }

            index++;
        }
    }

    //向dims中添加维度集合
    private void getDims(Set<String> dims, String[] stringSet) {
        if (stringSet != null) {
            for (String str : stringSet) {
                dims.add(str);
            }
        }
    }

    private void getDims(ArrayList<Set<String>> dimsList, Set<String> dims, String[][] stringSets) {
        if (stringSets != null) {
            for (String[] ss : stringSets) {
                Set<String> temp = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                for (String s : ss) {
                    temp.add(s);
                    dims.add(s);
                }
                dimsList.add(temp);
            }
        }
    }

    //必须至少两个元素
    private boolean hasSingle(ArrayList<Set<String>> dimsList) {
        boolean hasSingle = false;
        for (Set<String> dims : dimsList) {
            if (dims.size() < 2)
                hasSingle = true;
        }
        return hasSingle;
    }

    //每一个属性列不允许设置到不同分组中
    //true表示至少有一个维度,在不同的分类中存在了
    private boolean hasOverlap(ArrayList<Set<String>> dimsList, Set<String> Dims) {
        boolean hasOverlap = false;
        int dimSize = 0;//总共有多少个属性在dimsList中
        for (Set<String> dims : dimsList) {//循环每一组设置
            dimSize += dims.size();
        }
        if (dimSize != Dims.size())
            hasOverlap = true;
        return hasOverlap;
    }

    //初始化维度列
    private void initDimensionColumns() {
        for (DimensionDesc dim : dimensions) {//对每一个维度进行处理
            JoinDesc join = dim.getJoin();

            // init dimension columns
            ArrayList<TblColRef> dimCols = Lists.newArrayList();//属于该列需要的列集合
            String colStrs = dim.getColumn();//列属性名称

            //添加列信息
            if ((colStrs == null && dim.isDerived()) || ("{FK}".equalsIgnoreCase(colStrs))) {//说明没有设置属性列,但是设置了deived了,或者属性列设置为{FK}
                // when column is omitted, special case

                for (TblColRef col : join.getForeignKeyColumns()) {//循环fact_table对应关联的属性集合
                    dimCols.add(initDimensionColRef(col));//initDimensionColRef的目的就是收集该cube所使用了哪些列以及维度使用了哪些列
                }
            } else {//说明是normal形式,即维度就是fact_table中维度
                // normal case

                if (StringUtils.isEmpty(colStrs))
                    throw new IllegalStateException("Dimension column must not be blank " + dim);

                dimCols.add(initDimensionColRef(dim, colStrs));

                //                // fill back column ref in hierarchy
                //                if (dim.isHierarchy()) {
                //                    for (int i = 0; i < dimCols.size(); i++)
                //                        dim.getHierarchy()[i].setColumnRef(dimCols.get(i));
                //                }
            }

            TblColRef[] dimColArray = dimCols.toArray(new TblColRef[dimCols.size()]);
            dim.setColumnRefs(dimColArray);//设置该维度表中所有相关联的column集合

            // init derived columns
            if (dim.isDerived()) {
                String[] derived = dim.getDerived();//衍生维度集合
                String[][] split = splitDerivedColumnAndExtra(derived);//将derived列的内容按照:拆分成两个集合
                String[] derivedNames = split[0];
                String[] derivedExtra = split[1];
                TblColRef[] derivedCols = new TblColRef[derivedNames.length];//引用维度表中的列对象
                for (int i = 0; i < derivedNames.length; i++) {
                    derivedCols[i] = initDimensionColRef(dim, derivedNames[i]);
                }
                initDerivedMap(dimColArray, DeriveType.LOOKUP, dim, derivedCols, derivedExtra);
            }

            // PK-FK derive the other side
            if (join != null) {
                TblColRef[] fk = join.getForeignKeyColumns();//事实表关联键
                TblColRef[] pk = join.getPrimaryKeyColumns();//维度表关联键

                allColumns.addAll(Arrays.asList(fk));
                allColumns.addAll(Arrays.asList(pk));
                for (int i = 0; i < fk.length; i++) {
                    int find = ArrayUtils.indexOf(dimColArray, fk[i]);
                    if (find >= 0) {
                        TblColRef derivedCol = initDimensionColRef(pk[i]);
                        //fact_table的主键 与 lookup表外键相关联
                        initDerivedMap(new TblColRef[] { dimColArray[find] }, DeriveType.PK_FK, dim, new TblColRef[] { derivedCol }, null);
                    }
                }
                /** disable this code as we don't need fk be derived from pk
                 for (int i = 0; i < pk.length; i++) {
                 int find = ArrayUtils.indexOf(hostCols, pk[i]);
                 if (find >= 0) {
                 TblColRef derivedCol = initDimensionColRef(fk[i]);
                 initDerivedMap(hostCols[find], DeriveType.PK_FK, dim, derivedCol);
                 }
                 }
                 */
            }
        }
    }

    //将derived列的内容按照:拆分成两个集合
    private String[][] splitDerivedColumnAndExtra(String[] derived) {
        String[] cols = new String[derived.length];
        String[] extra = new String[derived.length];
        for (int i = 0; i < derived.length; i++) {
            String str = derived[i];
            int cut = str.indexOf(":");
            if (cut >= 0) {
                cols[i] = str.substring(0, cut);
                extra[i] = str.substring(cut + 1).trim();
            } else {
                cols[i] = str;
                extra[i] = "";
            }
        }
        return new String[][] { cols, extra };
    }

    /**
     *
     * @param hostCols 所有需要的列集合
     * @param type derived属性来自于什么表
     * @param dimension 当前是哪个维度
     * @param derivedCols lookup表中derived设置的属性集合
     * @param extra 额外属性
     * 针对deried进行处理
     */
    private void initDerivedMap(TblColRef[] hostCols, DeriveType type, DimensionDesc dimension, TblColRef[] derivedCols, String[] extra) {
        if (hostCols.length == 0 || derivedCols.length == 0)
            throw new IllegalStateException("host/derived columns must not be empty");

        // Although FK derives PK automatically, user unaware of this can declare PK as derived dimension explicitly.
        // In that case, derivedCols[] will contain a FK which is transformed from the PK by initDimensionColRef().
        // Must drop FK from derivedCols[] before continue.
        //删除fact表中的外键
        for (int i = 0; i < derivedCols.length; i++) {
            if (ArrayUtils.contains(hostCols, derivedCols[i])) {
                derivedCols = (TblColRef[]) ArrayUtils.remove(derivedCols, i);
                extra = (String[]) ArrayUtils.remove(extra, i);
                i--;
            }
        }

        Map<TblColRef, DeriveInfo> toHostMap = derivedToHostMap;
        Map<Array<TblColRef>, List<DeriveInfo>> hostToMap = hostToDerivedMap;

        Array<TblColRef> hostColArray = new Array<TblColRef>(hostCols);
        List<DeriveInfo> infoList = hostToMap.get(hostColArray);
        if (infoList == null) {
            hostToMap.put(hostColArray, infoList = new ArrayList<DeriveInfo>());
        }
        infoList.add(new DeriveInfo(type, dimension, derivedCols, false));

        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef derivedCol = derivedCols[i];
            boolean isOneToOne = type == DeriveType.PK_FK || ArrayUtils.contains(hostCols, derivedCol) || (extra != null && extra[i].contains("1-1"));
            toHostMap.put(derivedCol, new DeriveInfo(type, dimension, hostCols, isOneToOne));
        }
    }

    //给定维度对象以及要查询的列名字,获取该列对应的列对象,返回该列对应的fact_table中的列对象
    private TblColRef initDimensionColRef(DimensionDesc dim, String colName) {
        //找到对应的列对象
        TableDesc table = dim.getTableDesc();
        ColumnDesc col = table.findColumnByName(colName);//找到该表对应的列对象
        if (col == null)
            throw new IllegalArgumentException("No column '" + colName + "' found in table " + table);

        TblColRef ref = col.getRef();//获取该列对象

        // always use FK instead PK, FK could be shared by more than one lookup tables
        JoinDesc join = dim.getJoin();
        if (join != null) {
            int idx = ArrayUtils.indexOf(join.getPrimaryKeyColumns(), ref);//该列是否是lookup表的主键
            if (idx >= 0) {
                ref = join.getForeignKeyColumns()[idx];//找到该主键对应fact表的外键
            }
        }
        return initDimensionColRef(ref);
    }

    //添加列的映射关系,即该cube的所有列集合 以及 维度列集合
    private TblColRef initDimensionColRef(TblColRef ref) {
        TblColRef existing = findColumnRef(ref.getTable(), ref.getName());//找到列对象
        if (existing != null) {//说明已经缓存过了
            return existing;
        }

        //将该列添加到cube的列集合中 以及 cube的维度列集合中
        allColumns.add(ref);
        dimensionColumns.add(ref);

        Map<String, TblColRef> cols = columnMap.get(ref.getTable());
        if (cols == null) {
            columnMap.put(ref.getTable(), cols = new HashMap<String, TblColRef>());
        }
        cols.put(ref.getName(), ref);//添加该列和列对象
        return ref;
    }

    //初始化度量内容
    private void initMeasureColumns() {
        if (measures == null || measures.isEmpty()) {
            return;
        }

        TableDesc factTable = getFactTableDesc();//事实表
        List<TableDesc> lookupTables = getLookupTableDescs();//关联表集合
        for (MeasureDesc m : measures) {
            m.setName(m.getName().toUpperCase());

            if (m.getDependentMeasureRef() != null) {
                m.setDependentMeasureRef(m.getDependentMeasureRef().toUpperCase());
            }

            FunctionDesc func = m.getFunction();
            //将事实表和关联表都作为参数,初始化函数
            func.init(factTable, lookupTables);
            allColumns.addAll(func.getParameter().getColRefs());//函数中应用到哪个字段了

            if (ExtendedColumnMeasureType.FUNC_RAW.equalsIgnoreCase(m.getFunction().getExpression())) {
                FunctionDesc functionDesc = m.getFunction();

                List<TblColRef> hosts = ExtendedColumnMeasureType.getExtendedColumnHosts(functionDesc);
                TblColRef extendedColumn = ExtendedColumnMeasureType.getExtendedColumn(functionDesc);
                initExtendedColumnMap(hosts.toArray(new TblColRef[hosts.size()]), extendedColumn);
            }
        }
    }

    //用于扩展属性
    private void initExtendedColumnMap(TblColRef[] hostCols, TblColRef extendedColumn) {
        extendedColumnToHosts.put(extendedColumn, new DeriveInfo(DeriveType.EXTENDED_COLUMN, null, hostCols, false));
    }

    private void initMeasureReferenceToColumnFamily() {
        if (measures == null || measures.size() == 0)
            return;

        //所有的度量的name与度量对象的映射
        Map<String, MeasureDesc> measureLookup = new HashMap<String, MeasureDesc>();
        for (MeasureDesc m : measures)
            measureLookup.put(m.getName(), m);

        //度量的name与度量的序号的映射
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < measures.size(); i++)
            measureIndexLookup.put(measures.get(i).getName(), i);


        for (HBaseColumnFamilyDesc cf : getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                String[] colMeasureRefs = c.getMeasureRefs();
                MeasureDesc[] measureDescs = new MeasureDesc[colMeasureRefs.length];
                int[] measureIndex = new int[colMeasureRefs.length];
                for (int i = 0; i < colMeasureRefs.length; i++) {
                    measureDescs[i] = measureLookup.get(colMeasureRefs[i]);
                    measureIndex[i] = measureIndexLookup.get(colMeasureRefs[i]);
                }
                c.setMeasures(measureDescs);
                c.setMeasureIndex(measureIndex);
                c.setColumnFamilyName(cf.getName());
            }
        }
    }

    private void initDictionaryDesc() {
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                dictDesc.init(this);
                allColumns.add(dictDesc.getColumnRef());
                if (dictDesc.getResuseColumnRef() != null) {
                    allColumns.add(dictDesc.getResuseColumnRef());
                }
            }
        }
    }

    //在rowkey中找到第几个列
    public TblColRef getColumnByBitIndex(int bitIndex) {
        RowKeyColDesc[] rowKeyColumns = this.getRowkey().getRowKeyColumns();
        return rowKeyColumns[rowKeyColumns.length - 1 - bitIndex].getColRef();
    }

    //查看度量函数中是否有消耗内存的函数,比如HyperLogLog or TopN
    public boolean hasMemoryHungryMeasures() {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().getMeasureType().isMemoryHungry()) {
                return true;
            }
        }
        return false;
    }

    public long getRetentionRange() {
        return retentionRange;
    }

    public void setRetentionRange(long retentionRange) {
        this.retentionRange = retentionRange;
    }

    public long[] getAutoMergeTimeRanges() {
        return autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(long[] autoMergeTimeRanges) {
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    /**
     * Add error info and thrown exception out
     *
     * @param message
     */
    public void addError(String message) {
        addError(message, false);
    }

    /**
     * @param message error message
     * @param silent  if throw exception 是否无声,即是否抛异常
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

    public HBaseMappingDesc getHbaseMapping() {
        return hbaseMapping;
    }

    public void setHbaseMapping(HBaseMappingDesc hbaseMapping) {
        this.hbaseMapping = hbaseMapping;
    }

    public void setNullStrings(String[] nullStrings) {
        this.nullStrings = nullStrings;
    }

    public boolean supportsLimitPushDown() {
        return getStorageType() != IStorageAware.ID_HBASE && getStorageType() != IStorageAware.ID_HYBRID;
    }

    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType = storageType;
    }

    @Override
    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        this.engineType = engineType;
    }

    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public long getPartitionDateEnd() {
        return partitionDateEnd;
    }

    public void setPartitionDateEnd(long partitionDateEnd) {
        this.partitionDateEnd = partitionDateEnd;
    }

    /** Get columns that have dictionary
     * 获取所有有字段的列
     **/
    public Set<TblColRef> getAllColumnsHaveDictionary() {
        Set<TblColRef> result = Sets.newLinkedHashSet();

        // dictionaries in dimensions
        for (RowKeyColDesc rowKeyColDesc : rowkey.getRowKeyColumns()) {
            TblColRef colRef = rowKeyColDesc.getColRef();
            if (rowkey.isUseDictionary(colRef)) {//判断该列是否是dict
                result.add(colRef);
            }
        }

        // dictionaries in measures
        for (MeasureDesc measure : measures) {
            MeasureType<?> aggrType = measure.getFunction().getMeasureType();
            result.addAll(aggrType.getColumnsNeedDictionary(measure.getFunction()));
        }

        // any additional dictionaries
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                TblColRef col = dictDesc.getColumnRef();
                result.add(col);
            }
        }

        return result;
    }

    /** Get columns that need dictionary built on it. Note a column could reuse dictionary of another column.
     * 过滤掉使用其他列的字典的列
     * 即获取需要构建字典的列的集合
     **/
    public Set<TblColRef> getAllColumnsNeedDictionaryBuilt() {
        Set<TblColRef> result = getAllColumnsHaveDictionary();//获取所有有字段的列

        // remove columns that reuse other's dictionary
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                if (dictDesc.getResuseColumnRef() != null) {//使用别的字段的字典
                    result.remove(dictDesc.getColumnRef());//从集合中移除
                    result.add(dictDesc.getResuseColumnRef());
                }
            }
        }

        return result;
    }

    /** A column may reuse dictionary of another column, find the dict column, return same col if there's no reuse column
     **/
    public TblColRef getDictionaryReuseColumn(TblColRef col) {
        if (dictionaries == null) {
            return col;
        }
        for (DictionaryDesc dictDesc : dictionaries) {//循环所有的字典
            if (dictDesc.getColumnRef().equals(col) && dictDesc.getResuseColumnRef() != null) {//字典对应的列是参数列,并且该列引用其他列的字典
                return dictDesc.getResuseColumnRef();
            }
        }
        return col;
    }

    /** Get a column which can be used in distributing the source table */
    public TblColRef getDistributedByColumn() {
        Set<TblColRef> shardBy = getShardByColumns();
        if (shardBy != null && shardBy.size() > 0) {
            return shardBy.iterator().next();
        }

        return null;
    }

    //返回参数列对应的bilderClass
    public String getDictionaryBuilderClass(TblColRef col) {
        if (dictionaries == null)
            return null;

        for (DictionaryDesc desc : dictionaries) {
            if (desc.getBuilderClass() != null) {
                // column that reuses other's dict need not be built, thus should not reach here
                if (col.equals(desc.getColumnRef())) {
                    return desc.getBuilderClass();
                }
            }
        }
        return null;
    }

    public static CubeDesc getCopyOf(CubeDesc cubeDesc) {
        CubeDesc newCubeDesc = new CubeDesc();
        newCubeDesc.setName(cubeDesc.getName());
        newCubeDesc.setModelName(cubeDesc.getModelName());
        newCubeDesc.setDescription(cubeDesc.getDescription());
        newCubeDesc.setNullStrings(cubeDesc.getNullStrings());
        newCubeDesc.setDimensions(cubeDesc.getDimensions());
        newCubeDesc.setMeasures(cubeDesc.getMeasures());
        newCubeDesc.setDictionaries(cubeDesc.getDictionaries());
        newCubeDesc.setRowkey(cubeDesc.getRowkey());
        newCubeDesc.setHbaseMapping(cubeDesc.getHbaseMapping());
        newCubeDesc.setSignature(cubeDesc.getSignature());
        newCubeDesc.setNotifyList(cubeDesc.getNotifyList());
        newCubeDesc.setStatusNeedNotify(cubeDesc.getStatusNeedNotify());
        newCubeDesc.setAutoMergeTimeRanges(cubeDesc.getAutoMergeTimeRanges());
        newCubeDesc.setPartitionDateStart(cubeDesc.getPartitionDateStart());
        newCubeDesc.setPartitionDateEnd(cubeDesc.getPartitionDateEnd());
        newCubeDesc.setRetentionRange(cubeDesc.getRetentionRange());
        newCubeDesc.setEngineType(cubeDesc.getEngineType());
        newCubeDesc.setStorageType(cubeDesc.getStorageType());
        newCubeDesc.setAggregationGroups(cubeDesc.getAggregationGroups());
        newCubeDesc.setOverrideKylinProps(cubeDesc.getOverrideKylinProps());
        newCubeDesc.setConfig((KylinConfigExt) cubeDesc.getConfig());
        newCubeDesc.updateRandomUuid();
        return newCubeDesc;
    }

}
