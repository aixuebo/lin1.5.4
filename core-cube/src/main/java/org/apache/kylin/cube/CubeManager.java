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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * @author yangli9
 */
public class CubeManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";//为该segment创建hbase的表名字时候从这些字符串中获取字符

    private static int HBASE_TABLE_LENGTH = 10;//为segment设置一个hbase的表名字的时候,生成的表名字长度
    public static final Serializer<CubeInstance> CUBE_SERIALIZER = new JsonSerializer<CubeInstance>(CubeInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(CubeManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, CubeManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeManager>();

    public static CubeManager getInstance(KylinConfig config) {
        CubeManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (CubeManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new CubeManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                    for (KylinConfig kylinConfig : CACHE.keySet()) {
                        logger.warn("type: " + kylinConfig.getClass() + " reference: " + System.identityHashCode(kylinConfig.base()));
                    }
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // cube name ==> CubeInstance
    private CaseInsensitiveStringCache<CubeInstance> cubeMap;//内存中存储的CubeInstance对象
    // "table/column" ==> lookup table
    //    private SingleValueCache<String, LookupStringTable> lookupTables = new SingleValueCache<String, LookupStringTable>(Broadcaster.TYPE.METADATA);

    // for generation hbase table name of a new segment
    //key是cube的name,value是一个集合,是存储该cube的所有segment的hbase数据库表名字
    private Multimap<String, String> usedStorageLocation = HashMultimap.create();

    private CubeManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with config " + config);
        this.config = config;
        this.cubeMap = new CaseInsensitiveStringCache<CubeInstance>(config, Broadcaster.TYPE.CUBE);
        loadAllCubeInstance();
    }

    public List<CubeInstance> listAllCubes() {
        return new ArrayList<CubeInstance>(cubeMap.values());
    }

    //根据cube名字查找一个具体的cube
    public CubeInstance getCube(String cubeName) {
        cubeName = cubeName.toUpperCase();
        return cubeMap.get(cubeName);
    }

    //找到uuid对应的cube实例
    public CubeInstance getCubeByUuid(String uuid) {
        Collection<CubeInstance> copy = new ArrayList<CubeInstance>(cubeMap.values());
        for (CubeInstance cube : copy) {
            if (uuid.equals(cube.getUuid()))
                return cube;
        }
        return null;
    }

    /**
     * Get related Cubes by cubedesc name. By default, the desc name will be
     * translated into upper case.
     *
     * @param descName CubeDesc name
     * @return
     * 找到cube的descName与参数相同的cube集合
     */
    public List<CubeInstance> getCubesByDesc(String descName) {

        descName = descName.toUpperCase();
        List<CubeInstance> list = listAllCubes();//获取所有的cube
        List<CubeInstance> result = new ArrayList<CubeInstance>();
        Iterator<CubeInstance> it = list.iterator();//循环所有的cube
        while (it.hasNext()) {
            CubeInstance ci = it.next();
            if (descName.equalsIgnoreCase(ci.getDescName())) {//描述相同的则就是要找到的cube集合
                result.add(ci);
            }
        }
        return result;
    }

    /**
     * 为某一个cube的CubeSegment的某一列构建字典
     * @param cubeSeg 要处理的segment对象
     * @param col 要处理的某一个字段
     * @param factTableValueProvider 如何读取该字段所有的属性值
     */
    public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, DistinctColumnValuesProvider factTableValueProvider) throws IOException {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        //该列必须是字典列
        if (!cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col))//false表示col不是该cube需要的字典的列,因此直接返回
            return null;

        DictionaryManager dictMgr = getDictionaryManager();
        String builderClass = cubeDesc.getDictionaryBuilderClass(col);//返回构建字典的class对象
        DictionaryInfo dictInfo = dictMgr.buildDictionary(cubeDesc.getModel(), col, factTableValueProvider, builderClass);//构建一个字典表对象

        if (dictInfo != null) {
            Dictionary<?> dict = dictInfo.getDictionaryObject();//具体的字典对象
            cubeSeg.putDictResPath(col, dictInfo.getResourcePath());//存放该列和字典存放路径的映射
            cubeSeg.getRowkeyStats().add(new Object[] { col.getName(), dict.getSize(), dict.getSizeOfId() });

            CubeUpdate cubeBuilder = new CubeUpdate(cubeSeg.getCubeInstance());
            cubeBuilder.setToUpdateSegs(cubeSeg);
            updateCube(cubeBuilder);//更新新的ccube内容
        }
        return dictInfo;
    }

    /**
     * return null if no dictionary for given column
     * 返回该列对应的字典对象
     */
    @SuppressWarnings("unchecked")
    public Dictionary<String> getDictionary(CubeSegment cubeSeg, TblColRef col) {
        DictionaryInfo info = null;
        try {
            DictionaryManager dictMgr = getDictionaryManager();
            String dictResPath = cubeSeg.getDictResPath(col);//获取该列对应的字典存放路径
            if (dictResPath == null)
                return null;

            info = dictMgr.getDictionaryInfo(dictResPath);
            if (info == null)
                throw new IllegalStateException("No dictionary found by " + dictResPath + ", invalid cube state; cube segment" + cubeSeg + ", col " + col);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get dictionary for cube segment" + cubeSeg + ", col" + col, e);
        }

        return (Dictionary<String>) info.getDictionaryObject();
    }

    //为lookupTable表做快照
    public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        SnapshotManager snapshotMgr = getSnapshotManager();

        TableDesc tableDesc = new TableDesc(metaMgr.getTableDesc(lookupTable));//获取该lookup表元数据对象
        if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(tableDesc.getTableType())) {//说明该表是视图
            String tableName = tableDesc.getMaterializedName();
            tableDesc.setDatabase(config.getHiveDatabaseForIntermediateTable());
            tableDesc.setName(tableName);
        }

        //读取hive的表数据内容---即如何读取该lookup表内容
        ReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
        SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc);//为该hive表构建快照

        cubeSeg.putSnapshotResPath(lookupTable, snapshot.getResourcePath());//保存该lookup表,以及该lookup表内数据对应的存储路径
        CubeUpdate cubeBuilder = new CubeUpdate(cubeSeg.getCubeInstance());
        cubeBuilder.setToUpdateSegs(cubeSeg);
        updateCube(cubeBuilder);//更新cube

        return snapshot;
    }

    //删除该CubeInstance,deleteDesc是true,表示要删除CubeDesc
    // sync on update
    public CubeInstance dropCube(String cubeName, boolean deleteDesc) throws IOException {
        logger.info("Dropping cube '" + cubeName + "'");
        // load projects before remove cube from project

        // delete cube instance and cube desc
        CubeInstance cube = getCube(cubeName);

        if (deleteDesc && cube.getDescriptor() != null) {
            CubeDescManager.getInstance(config).removeCubeDesc(cube.getDescriptor());//删除一个cube描述对象,即删除CubeDesc
        }

        // remove cube and update cache
        getStore().deleteResource(cube.getResourcePath());//删除CubeInstance
        cubeMap.remove(cube.getName());

        // delete cube from project
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.CUBE, cubeName);

        if (listener != null)
            listener.afterCubeDelete(cube);

        return cube;
    }

    //创建一个CubeInstance对象
    // sync on update
    public CubeInstance createCube(String cubeName, String projectName, CubeDesc desc, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cubeName + "' from desc '" + desc.getName() + "'");

        // save cube resource
        CubeInstance cube = CubeInstance.create(cubeName, desc);
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);//更新该cube
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cubeName, projectName, owner);

        if (listener != null)
            listener.afterCubeCreate(cube);

        return cube;
    }

    //创建一个CubeInstance对象
    public CubeInstance createCube(CubeInstance cube, String projectName, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cube.getName() + "' from instance object. '");

        // save cube resource
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cube.getName(), projectName, owner);

        if (listener != null)
            listener.afterCubeCreate(cube);

        return cube;
    }

    //更新一个cube
    public CubeInstance updateCube(CubeUpdate update) throws IOException {
        CubeInstance cube = updateCubeWithRetry(update, 0);

        if (listener != null)
            listener.afterCubeUpdate(cube);

        return cube;
    }

    /**
     * 更新cube的数据
     * @param update 更新cube的请求
     * @param retry 尝试更新次数,每次增加1
     */
    private CubeInstance updateCubeWithRetry(CubeUpdate update, int retry) throws IOException {
        if (update == null || update.getCubeInstance() == null)
            throw new IllegalStateException();

        CubeInstance cube = update.getCubeInstance();
        logger.info("Updating cube instance '" + cube.getName() + "'");

        List<CubeSegment> newSegs = Lists.newArrayList(cube.getSegments());//所有的segment集合

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));//追加segment

        List<String> toRemoveResources = Lists.newArrayList();//要删除的segment路径
        if (update.getToRemoveSegs() != null) {//删除的segment
            Iterator<CubeSegment> iterator = newSegs.iterator();//循环每一个segment
            while (iterator.hasNext()) {
                CubeSegment currentSeg = iterator.next();
                boolean found = false;//true表示发现了要删除的segment
                for (CubeSegment toRemoveSeg : update.getToRemoveSegs()) {
                    if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                        iterator.remove();//删除该segment
                        toRemoveResources.add(toRemoveSeg.getStatisticsResourcePath());
                        found = true;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + currentSeg.getName() + "' doesn't exist for remove.");
                }
            }
        }

        if (update.getToUpdateSegs() != null) {
            for (CubeSegment segment : update.getToUpdateSegs()) {
                boolean found = false;//true表示发现了要更新的segment
                for (int i = 0; i < newSegs.size(); i++) {
                    if (newSegs.get(i).getUuid().equals(segment.getUuid())) {
                        newSegs.set(i, segment);//更新,即老的替换新的segment数据
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + segment.getName() + "' doesn't exist for update.");
                }
            }
        }

        Collections.sort(newSegs);//排序
        CubeValidator.validate(newSegs);
        cube.setSegments(newSegs);//设置新的

        if (update.getStatus() != null) {
            cube.setStatus(update.getStatus());
        }

        if (update.getOwner() != null) {
            cube.setOwner(update.getOwner());
        }

        if (update.getCost() > 0) {
            cube.setCost(update.getCost());
        }

        try {
            getStore().putResource(cube.getResourcePath(), cube, CUBE_SERIALIZER);//保存数据
        } catch (IllegalStateException ise) {
            logger.warn("Write conflict to update cube " + cube.getName() + " at try " + retry + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            cube = reloadCubeLocal(cube.getName());//重新加载cube对象
            update.setCubeInstance(cube);
            retry++;
            cube = updateCubeWithRetry(update, retry);//重新更新
        }

        //删除hdfs数据
        if (toRemoveResources.size() > 0) {
            for (String resource : toRemoveResources) {
                try {
                    getStore().deleteResource(resource);
                } catch (IOException ioe) {
                    logger.error("Failed to delete resource " + toRemoveResources.toString());
                }
            }
        }

        cubeMap.put(cube.getName(), cube);

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(cube.getConfig()).clearL2Cache();

        return cube;
    }

    //追加一个segment
    // append a full build segment
    public CubeSegment appendSegment(CubeInstance cube) throws IOException {
        return appendSegment(cube, 0, 0, 0, 0);
    }

    //追加一个segment
    public CubeSegment appendSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) throws IOException {
        return appendSegment(cube, startDate, endDate, startOffset, endOffset, true);
    }

    //追加一个segment
    public CubeSegment appendSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset, boolean strictChecking) throws IOException {

        if (strictChecking)
            checkNoBuildingSegment(cube);//校验该cube是否有正在buidler的,如果有则抛异常

        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {//是分区的表
            // try figure out a reasonable start if missing
            if (startDate == 0 && startOffset == 0) {//说明没有设置开始时间,因此会自动初始化开始时间为上一次的最后一个时间
                boolean isOffsetsOn = endOffset != 0;//说明设置了offset
                if (isOffsetsOn) {
                    startOffset = calculateStartOffsetForAppendSegment(cube);
                    if (startOffset == Long.MAX_VALUE) {
                        throw new IllegalStateException("There is already one pending for building segment, please submit request later.");
                    }
                } else {//说明是按照分区时间获取的.因此获取开始时间
                    startDate = calculateStartDateForAppendSegment(cube);
                }
            }
        } else {
            startDate = 0;
            endDate = Long.MAX_VALUE;
            startOffset = 0;
            endOffset = 0;
        }

        //创建一个CubeSegment对象
        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);
        validateNewSegments(cube, newSegment);//为该cube校验该segment

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);//新增一个该segment
        updateCube(cubeBuilder);//更新cube
        return newSegment;
    }

    //重新build过去的一段数据
    public CubeSegment refreshSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) throws IOException {
        checkNoBuildingSegment(cube);//校验该cube是否有正在buidler的,如果有则抛异常

        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);//创建一个新的segment

        //因为开始和结束只要存在,重新builder的时候,会重新对segment排序,因为该重新builder的范围可能跨域了老的segment的若干个,因此会将老的segment若干个都删除掉,保留新的
        Pair<Boolean, Boolean> pair = CubeValidator.fitInSegments(cube.getSegments(), newSegment);//确保该segment是存在的,因此才可以进行重新build,说明存在的意义是,只要重新builder的开始和结束存在,则就可以重新builder
        if (pair.getFirst() == false || pair.getSecond() == false) //必须开始位置和结束位置都存在,否则没办法重新build
            throw new IllegalArgumentException("The new refreshing segment " + newSegment + " does not match any existing segment in cube " + cube);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);//添加新的segment变更
        updateCube(cubeBuilder);//更新cube

        return newSegment;
    }

    /**
     * 合并多个segment
     * @param force  true表示强制进行merge合并,false表示不强制,如果有一个segment的大小是0,说明是空的,则抛异常,不能进行merge合并操作
     */
    public CubeSegment mergeSegments(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset, boolean force) throws IOException {
        if (cube.getSegments().isEmpty())
            throw new IllegalArgumentException("Cube " + cube + " has no segments");
        if (startDate >= endDate && startOffset >= endOffset)
            throw new IllegalArgumentException("Invalid merge range");

        checkNoBuildingSegment(cube);//不允许有正在builder的segment
        checkCubeIsPartitioned(cube);//必须有partition分区,否则是不需要merge合并的

        boolean isOffsetsOn = cube.getSegments().get(0).isSourceOffsetsOn();//是否使用了offset流式

        if (isOffsetsOn) {//处理流式
            // offset cube, merge by date range?
            if (startOffset == endOffset) {
                Pair<CubeSegment, CubeSegment> pair = findMergeOffsetsByDateRange(cube.getSegments(SegmentStatusEnum.READY), startDate, endDate, Long.MAX_VALUE);
                if (pair == null)
                    throw new IllegalArgumentException("Find no segments to merge by date range " + startDate + "-" + endDate + " for cube " + cube);
                startOffset = pair.getFirst().getSourceOffsetStart();
                endOffset = pair.getSecond().getSourceOffsetEnd();
            }
            startDate = 0;
            endDate = 0;
        } else {//处理分区方式
            // date range cube, make sure range is on dates
            if (startDate == endDate) {//暂时没想明白什么时候会走这个流程,反正当时间相同的时候,他将会走offset流程
                startDate = startOffset;
                endDate = endOffset;
            }
            //因为是时间区间的,因此将offset区间设置为0
            startOffset = 0;
            endOffset = 0;
        }

        //比如虽然新的merge的segment开始位置和结束位置分别是2017-01-01 2017-12-31,但是我真实的segment只有2017-01-01 2017-11-31,则最后的segment的结果就仅仅是2017-01-01到2017-11-31
        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);//创建新的segment

        //参数是merge时候创建新的segment,该方法返回已经存在的segment中在merge范围内的,即merge的segment真实合并的是哪些segment
        List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException("Range " + newSegment.getSourceOffsetStart() + "-" + newSegment.getSourceOffsetEnd() + " must contain at least 2 segments, but there is " + mergingSegments.size());

        CubeSegment first = mergingSegments.get(0);
        CubeSegment last = mergingSegments.get(mergingSegments.size() - 1);
        //设置真实的开始位置和结束位置
        if (newSegment.isSourceOffsetsOn()) {
            newSegment.setDateRangeStart(minDateRangeStart(mergingSegments));
            newSegment.setDateRangeEnd(maxDateRangeEnd(mergingSegments));
            newSegment.setSourceOffsetStart(first.getSourceOffsetStart());
            newSegment.setSourceOffsetEnd(last.getSourceOffsetEnd());
        } else {
            newSegment.setDateRangeStart(first.getSourceOffsetStart());
            newSegment.setDateRangeEnd(last.getSourceOffsetEnd());
        }

        //对不强制merge合并的操作,要进行校验
        if (force == false) {
            List<String> emptySegment = Lists.newArrayList();//找到空的segment集合
            for (CubeSegment seg : mergingSegments) {
                if (seg.getSizeKB() == 0) {
                    emptySegment.add(seg.getName());
                }
            }

            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException("Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: " + emptySegment);
            }
        }

        //校验segment的合法性
        validateNewSegments(cube, newSegment);

        //更新操作
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);//更新cube

        return newSegment;
    }

    /**
     * 在segment集合中,与startDate,endDate有交集的,则被帅选出来,选择第一个和最后一个满足交集的segment
     * 即找到准备合并的segment集合
     * @param segments
     * @param startDate 第一个segment的开始时间
     * @param endDate 第一个segment的开始时间 + skipSegDateRangeCap的时间
     * @param skipSegDateRangeCap 合并的周期
     * @return
     */
    private Pair<CubeSegment, CubeSegment> findMergeOffsetsByDateRange(List<CubeSegment> segments, long startDate, long endDate, long skipSegDateRangeCap) {
        // must be offset cube
        LinkedList<CubeSegment> result = Lists.newLinkedList();
        for (CubeSegment seg : segments) {//循环所有的segment

            // include if date range overlaps
            if (startDate < seg.getDateRangeEnd() && seg.getDateRangeStart() < endDate) {//这里面返回true,说明有交集

                //虽然有交集,但是也要进一步filter过滤

                // reject too big segment,即过滤掉segment区间本身就大于合并周期的
                if (seg.getDateRangeEnd() - seg.getDateRangeStart() > skipSegDateRangeCap) //说明这个segment的范围已经超过了合并周期了,比如范围是30天,已经超过28天合并,因此直接break
                    break;

                // reject holes 过滤掉中间有空隙的,即有空隙的不参与合并
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())//说明上下两个segment的开始位置和结束位置没有连接上
                    break;

                result.add(seg);
            }
        }

        if (result.size() <= 1)
            return null;
        else
            return Pair.newPair(result.getFirst(), result.getLast());
    }

    //找到最小的segment
    private long minDateRangeStart(List<CubeSegment> mergingSegments) {
        long min = Long.MAX_VALUE;
        for (CubeSegment seg : mergingSegments)
            min = Math.min(min, seg.getDateRangeStart());
        return min;
    }

    //找到最大的segment
    private long maxDateRangeEnd(List<CubeSegment> mergingSegments) {
        long max = Long.MIN_VALUE;
        for (CubeSegment seg : mergingSegments)
            max = Math.max(max, seg.getDateRangeEnd());
        return max;
    }


    //获取当前最后一个segment的end位置
    private long calculateStartOffsetForAppendSegment(CubeInstance cube) {
        List<CubeSegment> existing = cube.getSegments();
        if (existing.isEmpty()) {
            return 0;
        } else {
            return existing.get(existing.size() - 1).getSourceOffsetEnd();
        }
    }


    //获取当前最后一个segment的date的end位置
    private long calculateStartDateForAppendSegment(CubeInstance cube) {
        List<CubeSegment> existing = cube.getSegments();
        if (existing.isEmpty()) {
            return cube.getDescriptor().getPartitionDateStart();
        } else {
            return existing.get(existing.size() - 1).getDateRangeEnd();
        }
    }

    //严格校验,确保此时没有正在buildering的segment
    private void checkNoBuildingSegment(CubeInstance cube) {
        if (cube.getBuildingSegments().size() > 0) {
            throw new IllegalStateException("There is already a building segment!");
        }
    }

    //确保该cube必须有分区,没有分区则抛异常
    private void checkCubeIsPartitioned(CubeInstance cube) {
        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned() == false) {
            throw new IllegalStateException("there is no partition date column specified, only full build is supported");
        }
    }

    /**
     * After cube update, reload cube related cache
     * 重新加载cube对象
     * @param cubeName
     */
    public CubeInstance reloadCubeLocal(String cubeName) {
        return reloadCubeLocalAt(CubeInstance.concatResourcePath(cubeName));
    }

    public void removeCubeLocal(String cubeName) {
        usedStorageLocation.removeAll(cubeName.toUpperCase());
        cubeMap.removeLocal(cubeName);
    }

    //如何读取该segment下某一个维度表内的快照数据
    public LookupStringTable getLookupTable(CubeSegment cubeSegment, DimensionDesc dim) {

        String tableName = dim.getTable();
        String[] pkCols = dim.getJoin().getPrimaryKey();//lookup表主键
        String snapshotResPath = cubeSegment.getSnapshotResPath(tableName);//获取该segment在builder的时候对应该表的快照路径
        if (snapshotResPath == null)
            throw new IllegalStateException("No snaphot for table '" + tableName + "' found on cube segment" + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);//说明没有快照

        try {
            SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);//返回快照对象
            TableDesc tableDesc = getMetadataManager().getTableDesc(tableName);//返回对应的表对象
            return new LookupStringTable(tableDesc, pkCols, snapshot);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    private CubeSegment newSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) {
        CubeSegment segment = new CubeSegment();
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(CubeSegment.makeSegmentName(startDate, endDate, startOffset, endOffset));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setSourceOffsetStart(startOffset);
        segment.setSourceOffsetEnd(endOffset);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());//产生存储该segment数据的hbase表名

        segment.setCubeInstance(cube);

        segment.validate();
        return segment;
    }

    //产生一个不重复的tableName,名字组成是KYLIN_+10个随机字符
    private String generateStorageLocation() {
        String namePrefix = IRealizationConstants.CubeHbaseStorageLocationPrefix;
        String tableName = "";
        Random ran = new Random();
        do {
            StringBuffer sb = new StringBuffer();
            sb.append(namePrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                sb.append(ALPHA_NUM.charAt(ran.nextInt(ALPHA_NUM.length())));//产生一个随机字符
            }
            tableName = sb.toString();
        } while (this.usedStorageLocation.containsValue(tableName));//存在则重新生成一个

        return tableName;
    }

    //产生需要合并的开始位置和结束位置
    public Pair<Long, Long> autoMergeCubeSegments(CubeInstance cube) throws IOException {
        //true表示 需要自动merge分区,是在cube中设置的merge分区天数
        if (!cube.needAutoMerge()) {
            logger.debug("Cube " + cube.getName() + " doesn't need auto merge");
            return null;
        }

        //只能选择是ready的,如果有非ready的,则返回null,并且打印debug日志
        if (cube.getBuildingSegments().size() > 0) {
            logger.debug("Cube " + cube.getName() + " has bulding segment, will not trigger merge at this moment");
            return null;
        }

        List<CubeSegment> ready = cube.getSegments(SegmentStatusEnum.READY);

        long[] timeRanges = cube.getDescriptor().getAutoMergeTimeRanges();//自动merge分区的时间信息
        Arrays.sort(timeRanges);

        for (int i = timeRanges.length - 1; i >= 0; i--) {//只要一个满足了,就return停止了
            long toMergeRange = timeRanges[i];//每一个规定的自动merge分区时间

            for (int s = 0; s < ready.size(); s++) {//循环全部的segment
                CubeSegment seg = ready.get(s);
                Pair<CubeSegment, CubeSegment> p = findMergeOffsetsByDateRange(ready.subList(s, ready.size()),seg.getDateRangeStart(), seg.getDateRangeStart() + toMergeRange, toMergeRange);//找到在startDate到endDate之间的需要被合并的segment的第一个和最后一个segment
                if (p != null && p.getSecond().getDateRangeEnd() - p.getFirst().getDateRangeStart() >= toMergeRange) //说明区间已经超过了合并时间周期了,因此进行合并
                    return Pair.newPair(p.getFirst().getSourceOffsetStart(), p.getSecond().getSourceOffsetEnd());//合并获取开始位置和结束位置
            }
        }

        return null;
    }

    //相关信息参见org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep,是该job结束后更新的这些关于segment的元数据信息
    public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment... newSegments) throws IOException {
        List<CubeSegment> tobe = calculateToBeSegments(cube);

        for (CubeSegment seg : newSegments) {
            if (tobe.contains(seg) == false)
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " is expected but not in the tobe " + tobe);

            if (StringUtils.isBlank(seg.getStorageLocationIdentifier()))
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " missing StorageLocationIdentifier");

            if (StringUtils.isBlank(seg.getLastBuildJobID()))
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " missing LastBuildJobID");

            seg.setStatus(SegmentStatusEnum.READY);
        }

        for (CubeSegment seg : tobe) {
            if (isReady(seg) == false)
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " should be READY but is not");
        }

        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        for (CubeSegment segment : cube.getSegments()) {
            if (!tobe.contains(segment))
                toRemoveSegs.add(segment);
        }

        logger.info("Promoting cube " + cube + ", new segments " + Arrays.toString(newSegments) + ", to remove segments " + toRemoveSegs);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()])).setToUpdateSegs(newSegments).setStatus(RealizationStatusEnum.READY);
        updateCube(cubeBuilder);
    }

    //为新增的segment进行校验,确保没问题
    public void validateNewSegments(CubeInstance cube, CubeSegment... newSegments) {
        List<CubeSegment> tobe = calculateToBeSegments(cube, newSegments);//返回新增加的segment后,合法的segment集合
        List<CubeSegment> newList = Arrays.asList(newSegments);
        if (tobe.containsAll(newList) == false) {//确保新增加的segment一定都是合法的,否则抛异常
            throw new IllegalStateException("For cube " + cube + ", the new segments " + newList + " do not fit in its current " + cube.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    /**
     * Smartly figure out the TOBE segments once all new segments are built.
     * 对cube原有的segment和新增加的segment进行合并.删除不符合规则的segment,返回最终的segment
     *
     * 不符合规则的条件是:
     * - Ensures no gap, no overlap 确保没有间隙，也没有交叠
     * - Favors new segments over the old 保留new状态的,比ready状态的优先级高
     * - Favors big segments over the small 范围大的segment要替换掉范围小的segment,即当有包含关系的时候,删除一个范围小的segment
     */
    private List<CubeSegment> calculateToBeSegments(CubeInstance cube, CubeSegment... newSegments) {

        List<CubeSegment> tobe = Lists.newArrayList(cube.getSegments());//cube此时所有的segment
        if (newSegments != null)
            tobe.addAll(Arrays.asList(newSegments));//添加新的segment
        if (tobe.size() == 0)
            return tobe;

        // sort by source offset 排序
        Collections.sort(tobe);

        CubeSegment firstSeg = tobe.get(0);//获取第一个
        firstSeg.validate();//校验segment合法性----必须是分区的,否则不需要segment,校验开始时间不能大于结束时间

        for (int i = 0, j = 1; j < tobe.size();) {//从第2个segment开始循环,即j=1开始,i上j的上一个segment
            CubeSegment is = tobe.get(i);//获取上一个segment
            CubeSegment js = tobe.get(j);//获取本次应该校验的segment
            js.validate();//校验本次处理的segment

            //以下两个校验应该是不会发生的
            // check i is either ready or new
            if (!isNew(is) && !isReady(is)) {//说明状态不对,continue,从集合中移除该segment
                tobe.remove(i);
                continue;
            }

            // check j is either ready or new 说明状态不对,continue,从集合中移除该segment
            if (!isNew(js) && !isReady(js)) {
                tobe.remove(j);
                continue;
            }

            if (is.getSourceOffsetStart() == js.getSourceOffsetStart()) {//两个segment的开始位置相同,此时应该业务上控制,也不会发生,但是会发生在refreshSegment阶段,产生新的segment跨域了老的segment若干个,因此会出现这种情况
                //一旦进来后,最终都会continue,但是最终会删除一个segment
                // if i, j competes
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {//都是状态合法的,此时就要删除一个,选择区间最小的删除掉,因为开始时间是相同的,则看结束时间即可
                    // if both new or ready, favor the bigger segment
                    if (is.getSourceOffsetEnd() <= js.getSourceOffsetEnd()) {
                        tobe.remove(i);
                    } else {
                        tobe.remove(j);
                    }
                } else if (isNew(is)) {//删除new新的,反正保留一个即可---此时new优先,如果new则保留,删除非new的
                    // otherwise, favor the new segment
                    tobe.remove(j);
                } else {
                    tobe.remove(i);
                }
                continue;
            }

            // if i, j in sequence
            if (is.getSourceOffsetEnd() <= js.getSourceOffsetStart()) {//说明合法,后一个开始位置就应该比前一个结束位置大,因此执行下一个segment
                i++;
                j++;
                continue;
            }

            // seems j not fitting此时说明后一个不比前一个区间大,因此要删除后一个,因此此时说明有交叠overlap
            tobe.remove(j);
        }

        return tobe;
    }

    private boolean isReady(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    private boolean isNew(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.NEW || seg.getStatus() == SegmentStatusEnum.READY_PENDING;
    }

    //加载所有的cube
    private void loadAllCubeInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT, ".json");

        logger.info("Loading Cube from folder " + store.getReadableResourcePath(ResourceStore.CUBE_RESOURCE_ROOT));

        int succeed = 0;
        int fail = 0;
        for (String path : paths) {
            CubeInstance cube = reloadCubeLocalAt(path);
            if (cube == null) {
                fail++;
            } else {
                succeed++;
            }
        }

        logger.info("Loaded " + succeed + " cubes, fail on " + fail + " cubes");
    }

    //加载一个cube
    private synchronized CubeInstance reloadCubeLocalAt(String path) {
        ResourceStore store = getStore();

        CubeInstance cubeInstance;
        try {
            cubeInstance = store.getResource(path, CubeInstance.class, CUBE_SERIALIZER);

            CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(cubeInstance.getDescName());
            if (cubeDesc == null)
                throw new IllegalStateException("CubeInstance desc not found '" + cubeInstance.getDescName() + "', at " + path);

            cubeInstance.setConfig((KylinConfigExt) cubeDesc.getConfig());

            if (StringUtils.isBlank(cubeInstance.getName()))
                throw new IllegalStateException("CubeInstance name must not be blank, at " + path);

            if (cubeInstance.getDescriptor() == null)
                throw new IllegalStateException("CubeInstance desc not found '" + cubeInstance.getDescName() + "', at " + path);

            final String cubeName = cubeInstance.getName();
            cubeMap.putLocal(cubeName, cubeInstance);//更新本节点信息

            for (CubeSegment segment : cubeInstance.getSegments()) {
                usedStorageLocation.put(cubeName.toUpperCase(), segment.getStorageLocationIdentifier());
            }

            logger.debug("Reloaded new cube: " + cubeName + " with reference being" + cubeInstance + " having " + cubeInstance.getSegments().size() + " segments:" + StringUtils.join(Collections2.transform(cubeInstance.getSegments(), new Function<CubeSegment, String>() {
                @Nullable
                @Override
                public String apply(CubeSegment input) {
                    return input.getStorageLocationIdentifier();
                }
            }), ","));

            return cubeInstance;
        } catch (Exception e) {
            logger.error("Error during load cube instance, skipping : " + path, e);
            return null;
        }
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private DictionaryManager getDictionaryManager() {
        return DictionaryManager.getInstance(config);
    }

    private SnapshotManager getSnapshotManager() {
        return SnapshotManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.CUBE;
    }

    @Override
    public IRealization getRealization(String name) {
        return getCube(name);
    }

    // ============================================================================

    public interface CubeChangeListener {
        void afterCubeCreate(CubeInstance cube);//监听cube被创建的之后

        void afterCubeUpdate(CubeInstance cube);//监听cube有更新的之后

        void afterCubeDelete(CubeInstance cube);//监听cube被删除的之后
    }

    private CubeChangeListener listener;

    public void setCubeChangeListener(CubeChangeListener listener) {
        this.listener = listener;
    }

    /**
     * Get the columns which need build the dictionary from fact table. (the column exists on fact and is not fk)
     * @param cubeDesc
     * @return
     * @throws IOException
     * 字典的列属于fact表中的集合
     */
    public List<TblColRef> getAllDictColumnsOnFact(CubeDesc cubeDesc) throws IOException {
        List<TblColRef> factDictCols = new ArrayList<TblColRef>();//字典的列属于fact表中的集合
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (TblColRef col : cubeDesc.getAllColumnsNeedDictionaryBuilt()) {//获取需要构建字典的列的集合,然后循环

            String scanTable = dictMgr.decideSourceData(cubeDesc.getModel(), col).getTable();//该属性字段对应的表
            if (cubeDesc.getModel().isFactTable(scanTable)) {//是否是事实表
                factDictCols.add(col);
            }
        }
        return factDictCols;
    }
}
