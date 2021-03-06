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

package org.apache.kylin.cube;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

//先比较开始位置,在比较结束位置
//该对象存储在CubeInstance文件中
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeSegment implements Comparable<CubeSegment>, IBuildable, ISegment {

    @JsonBackReference
    private CubeInstance cubeInstance;//该segment所属cube
    @JsonProperty("uuid")
    private String uuid;//segment对应的uuid
    @JsonProperty("name")
    private String name;//格式是FULL_BUILD  或者startOffset + "_" + endOffset  或者yyyyMMddHHmmss_yyyyMMddHHmmss
    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier; // HTable name hbase对应的表名

    //该segment对应的时间区间
    @JsonProperty("date_range_start")
    private long dateRangeStart;
    @JsonProperty("date_range_end")
    private long dateRangeEnd;

    //该segment对应的offset区间
    @JsonProperty("source_offset_start")
    private long sourceOffsetStart;
    @JsonProperty("source_offset_end")
    private long sourceOffsetEnd;

    //segment大小相关信息参见org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep,是该job结束后更新的这些关于segment的元数据信息
    @JsonProperty("status")
    private SegmentStatusEnum status;//状态
    @JsonProperty("size_kb")
    private long sizeKB;//cube的大小,单位M
    @JsonProperty("input_records")
    private long inputRecords;//原始文件行数
    @JsonProperty("input_records_size")
    private long inputRecordsSize;//原始文件大小
    @JsonProperty("last_build_time")
    private long lastBuildTime;//设置该cube最后一次执行的时候时间戳
    @JsonProperty("last_build_job_id")
    private String lastBuildJobID;//设置该cube最后一次执行的时候的jobID
    @JsonProperty("create_time_utc")
    private long createTimeUTC;//创建时间
    @JsonProperty("cuboid_shard_nums")
    private Map<Long, Short> cuboidShardNums = Maps.newHashMap();
    @JsonProperty("total_shards") //it is only valid when all cuboids are squshed into some shards. like the HBASE_STORAGE case, otherwise it'll stay 0
    private int totalShards = 0;
    @JsonProperty("blackout_cuboids")
    private List<Long> blackoutCuboids = Lists.newArrayList();

    @JsonProperty("binary_signature")
    private String binarySignature; // a hash of cube schema and dictionary ID, used for sanity check

    //key是字段table/colume,value是该列对应的字典存放路径
    @JsonProperty("dictionaries")
    private ConcurrentHashMap<String, String> dictionaries; // table/column ==> dictionary resource path 存储cube的字典

    //key是lookup表的table名字,value是该表对应的快照的路径---因为lookup表由于dervied和extends的原因,会引用lookup表的数据内容,因此需要有一个映射快照的需求
    @JsonProperty("snapshots")
    private ConcurrentHashMap<String, String> snapshots; // table name ==> snapshot resource path 存储cube的快照

    @JsonProperty("index_path")
    private String indexPath;//设置二级索引目录

    @JsonProperty("rowkey_stats")
    private List<Object[]> rowkeyStats = Lists.newArrayList();//存储字典相关的信息,比如col.getName(), dict.getSize(), dict.getSizeOfId()即容纳多少个不同的值,以及每一个字典对应的编号占用多少个字节

    @JsonProperty("additionalInfo")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private HashMap<String, String> additionalInfo = new LinkedHashMap<String, String>();

    private volatile Map<Long, Short> cuboidBaseShards = Maps.newHashMap();//cuboid id ==> base(starting) shard for this cuboid

    public CubeDesc getCubeDesc() {
        return getCubeInstance().getDescriptor();
    }

    /**
     * @param startDate
     * @param endDate
     * @return if(startDate == 0 && endDate == 0), returns "FULL_BUILD", else
     * returns "yyyyMMddHHmmss_yyyyMMddHHmmss"
     * 为segment产生一个name,格式是FULL_BUILD  或者startOffset + "_" + endOffset  或者yyyyMMddHHmmss_yyyyMMddHHmmss
     */
    public static String makeSegmentName(long startDate, long endDate, long startOffset, long endOffset) {
        if (startOffset != 0 || endOffset != 0) {
            if (startOffset == 0 && (endOffset == 0 || endOffset == Long.MAX_VALUE)) {
                return "FULL_BUILD";
            }

            return startOffset + "_" + endOffset;
        }

        // using time
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(startDate) + "_" + dateFormat.format(endDate);
    }

    // ============================================================================

    public KylinConfig getConfig() {
        return cubeInstance.getConfig();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String id) {
        this.uuid = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    @Override
    public DataModelDesc getModel() {
        return this.getCubeDesc().getModel();
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public long getSizeKB() {
        return sizeKB;
    }

    public void setSizeKB(long sizeKB) {
        this.sizeKB = sizeKB;
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public long getInputRecordsSize() {
        return inputRecordsSize;
    }

    public void setInputRecordsSize(long inputRecordsSize) {
        this.inputRecordsSize = inputRecordsSize;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public String getLastBuildJobID() {
        return lastBuildJobID;
    }

    public void setLastBuildJobID(String lastBuildJobID) {
        this.lastBuildJobID = lastBuildJobID;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public String getBinarySignature() {
        return binarySignature;
    }

    public void setBinarySignature(String binarySignature) {
        this.binarySignature = binarySignature;
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public void setCubeInstance(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
    }

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public List<Object[]> getRowkeyStats() {
        return rowkeyStats;
    }

    public Map<String, String> getDictionaries() {
        if (dictionaries == null)
            dictionaries = new ConcurrentHashMap<String, String>();
        return dictionaries;
    }

    public Map<String, String> getSnapshots() {
        if (snapshots == null)
            snapshots = new ConcurrentHashMap<String, String>();
        return snapshots;
    }

    public String getSnapshotResPath(String table) {
        return getSnapshots().get(table);
    }

    public void putSnapshotResPath(String table, String snapshotResPath) {
        getSnapshots().put(table, snapshotResPath);
    }

    public Collection<String> getDictionaryPaths() {
        return getDictionaries().values();
    }

    public Collection<String> getSnapshotPaths() {
        return getSnapshots().values();
    }

    public String getDictResPath(TblColRef col) {
        return getDictionaries().get(dictKey(col));
    }

    //设置某一个列对应的字典存放路径
    public void putDictResPath(TblColRef col, String dictResPath) {
        getDictionaries().put(dictKey(col), dictResPath);
    }

    private String dictKey(TblColRef col) {
        return col.getTable() + "/" + col.getName();
    }

    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    public Map<TblColRef, Dictionary<String>> buildDictionaryMap() {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : getCubeDesc().getAllColumnsHaveDictionary()) {
            result.put(col, (Dictionary<String>) getDictionary(col));
        }
        return result;
    }

    public Dictionary<String> getDictionary(TblColRef col) {
        TblColRef reuseCol = getCubeDesc().getDictionaryReuseColumn(col);
        CubeManager cubeMgr = CubeManager.getInstance(this.getCubeInstance().getConfig());
        return cubeMgr.getDictionary(this, reuseCol);
    }

    public CubeDimEncMap getDimensionEncodingMap() {
        return new CubeDimEncMap(this);
    }

    //true表示使用的是offset
    public boolean isSourceOffsetsOn() {
        return sourceOffsetStart != 0 || sourceOffsetEnd != 0;
    }

    // date range is used in place of source offsets when offsets are missing
    //开始时间或者offset
    public long getSourceOffsetStart() {
        return isSourceOffsetsOn() ? sourceOffsetStart : dateRangeStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    // date range is used in place of source offsets when offsets are missing
    //获取segment的结束时间或者offset
    public long getSourceOffsetEnd() {
        return isSourceOffsetsOn() ? sourceOffsetEnd : dateRangeEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    //true表示有重叠,false表示没有交集
    public boolean dateRangeOverlaps(CubeSegment seg) {
        return dateRangeStart < seg.dateRangeEnd && seg.dateRangeStart < dateRangeEnd;
    }

    //参数CubeSegment属于本类范围内,包含=关系
    public boolean dateRangeContains(CubeSegment seg) {
        return dateRangeStart <= seg.dateRangeStart && seg.dateRangeEnd <= dateRangeEnd;
    }

    // date range is used in place of source offsets when offsets are missing
    //true表示有重叠,false表示没有交集
    public boolean sourceOffsetOverlaps(CubeSegment seg) {
        if (isSourceOffsetsOn())
            return sourceOffsetStart < seg.sourceOffsetEnd && seg.sourceOffsetStart < sourceOffsetEnd;
        else
            return dateRangeOverlaps(seg);
    }

    // date range is used in place of source offsets when offsets are missing
    //参数CubeSegment属于本类范围内,包含关系
    //比如原始是[1,200],但是seg为[150,180]
    public boolean sourceOffsetContains(CubeSegment seg) {
        if (isSourceOffsetsOn())
            return sourceOffsetStart <= seg.sourceOffsetStart && seg.sourceOffsetEnd <= sourceOffsetEnd;
        else
            return dateRangeContains(seg);
    }

    //校验segment合法性----必须是分区的,否则不需要segment,校验开始时间不能大于结束时间
    public void validate() {
        if (cubeInstance.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {//model是分区的
            if (!isSourceOffsetsOn() && dateRangeStart >= dateRangeEnd) //判断start > end 时候是非法的,则报错
                throw new IllegalStateException("Invalid segment, dateRangeStart(" + dateRangeStart + ") must be smaller than dateRangeEnd(" + dateRangeEnd + ") in segment " + this);
            if (isSourceOffsetsOn() && sourceOffsetStart >= sourceOffsetEnd)
                throw new IllegalStateException("Invalid segment, sourceOffsetStart(" + sourceOffsetStart + ") must be smaller than sourceOffsetEnd(" + sourceOffsetEnd + ") in segment " + this);
        }
    }

    //先比较开始位置,在比较结束位置
    @Override
    public int compareTo(CubeSegment other) {
        long comp = this.getSourceOffsetStart() - other.getSourceOffsetStart();
        if (comp != 0)
            return comp < 0 ? -1 : 1;

        comp = this.getSourceOffsetEnd() - other.getSourceOffsetEnd();
        if (comp != 0)
            return comp < 0 ? -1 : 1;
        else
            return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cubeInstance == null) ? 0 : cubeInstance.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CubeSegment other = (CubeSegment) obj;
        if (cubeInstance == null) {
            if (other.cubeInstance != null)
                return false;
        } else if (!cubeInstance.equals(other.cubeInstance))
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (status != other.status)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return cubeInstance.getName() + "[" + name + "]";
    }

    public void setDictionaries(ConcurrentHashMap<String, String> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public void setSnapshots(ConcurrentHashMap<String, String> snapshots) {
        this.snapshots = snapshots;
    }

    public String getStatisticsResourcePath() {
        return getStatisticsResourcePath(this.getCubeInstance().getName(), this.getUuid());
    }

    //为每一个cube的segment输出统计信息  /cube_statistics/cubeName/cubeSegmentId.seq
    public static String getStatisticsResourcePath(String cubeName, String cubeSegmentId) {
        return ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeName + "/" + cubeSegmentId + ".seq";
    }

    @Override
    public int getSourceType() {
        return cubeInstance.getSourceType();
    }

    @Override
    public int getEngineType() {
        return cubeInstance.getEngineType();
    }

    @Override
    public int getStorageType() {
        return cubeInstance.getStorageType();
    }

    //是否可以分片,默认是false
    public boolean isEnableSharding() {
        return getCubeDesc().isEnableSharding();
    }

    //设置分片的列集合
    public Set<TblColRef> getShardByColumns() {
        return getCubeDesc().getShardByColumns();
    }

    //sharding需要的字节长度+cuboid需要的字节长度
    public int getRowKeyPreambleSize() {
        return isEnableSharding() ? RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN : RowConstants.ROWKEY_CUBOIDID_LEN;
    }

    /**
     * get the number of shards where each cuboid will distribute
     *
     * @return
     */
    public Short getCuboidShardNum(Long cuboidId) {
        Short ret = this.cuboidShardNums.get(cuboidId);
        if (ret == null) {
            return 1;
        } else {
            return ret;
        }
    }

    public void setCuboidShardNums(Map<Long, Short> newCuboidShards) {
        this.cuboidShardNums = newCuboidShards;
    }

    public int getTotalShards(long cuboidId) {
        if (totalShards > 0) {
            //shard squashed case
            //logger.info("total shards for {} is {}", cuboidId, totalShards);
            return totalShards;
        } else {
            int ret = getCuboidShardNum(cuboidId);
            //logger.info("total shards for {} is {}", cuboidId, ret);
            return ret;
        }
    }

    public void setTotalShards(int totalShards) {
        this.totalShards = totalShards;
    }

    public short getCuboidBaseShard(Long cuboidId) {
        if (totalShards > 0) {
            //shard squashed case

            Short ret = cuboidBaseShards.get(cuboidId);
            if (ret == null) {
                ret = ShardingHash.getShard(cuboidId, totalShards);
                cuboidBaseShards.put(cuboidId, ret);
            }

            //logger.info("base for cuboid {} is {}", cuboidId, ret);
            return ret;
        } else {
            //logger.info("base for cuboid {} is {}", cuboidId, 0);
            return 0;
        }
    }

    public List<Long> getBlackoutCuboids() {
        return this.blackoutCuboids;
    }

    public IRealization getRealization() {
        return cubeInstance;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    public HashMap<String, String> getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(HashMap<String, String> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }
}
