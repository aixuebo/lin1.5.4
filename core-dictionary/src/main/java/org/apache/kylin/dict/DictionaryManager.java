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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableSignature;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

public class DictionaryManager {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryManager.class);

    private static final DictionaryInfo NONE_INDICATOR = new DictionaryInfo();

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, DictionaryManager> CACHE = new ConcurrentHashMap<KylinConfig, DictionaryManager>();

    public static DictionaryManager getInstance(KylinConfig config) {
        DictionaryManager r = CACHE.get(config);
        if (r == null) {
            synchronized (DictionaryManager.class) {
                r = CACHE.get(config);
                if (r == null) {
                    r = new DictionaryManager(config);
                    CACHE.put(config, r);
                    if (CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return r;
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    //key是path,value是该path对应的字典对象
    private LoadingCache<String, DictionaryInfo> dictCache; // resource

    // path ==>
    // DictionaryInfo

    private DictionaryManager(KylinConfig config) {
        this.config = config;
        this.dictCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, DictionaryInfo>() {
            @Override
            public void onRemoval(RemovalNotification<String, DictionaryInfo> notification) {
                DictionaryManager.logger.info("Dict with resource path " + notification.getKey() + " is removed due to " + notification.getCause());
            }
        }).maximumSize(config.getCachedDictMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, DictionaryInfo>() {
                    @Override
                    public DictionaryInfo load(String key) throws Exception {
                        DictionaryInfo dictInfo = DictionaryManager.this.load(key, true);
                        if (dictInfo == null) {
                            return NONE_INDICATOR;
                        } else {
                            return dictInfo;
                        }
                    }
                });
    }

    //通过路径加载一个字典对象
    public Dictionary<?> getDictionary(String resourcePath) throws IOException {
        DictionaryInfo dictInfo = getDictionaryInfo(resourcePath);
        return dictInfo == null ? null : dictInfo.getDictionaryObject();
    }

    //加载存在的一个字典对象
    public DictionaryInfo getDictionaryInfo(final String resourcePath) throws IOException {
        try {
            DictionaryInfo result = dictCache.get(resourcePath);
            if (result == NONE_INDICATOR) {
                return null;
            } else {
                return result;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Save the dictionary as it is.
     * More often you should consider using its alternative trySaveNewDict to save dict space
     * 为一个字典描述对象插入新的字典对象
     */
    public DictionaryInfo forceSave(Dictionary<?> newDict, DictionaryInfo newDictInfo) throws IOException {
        initDictInfo(newDict, newDictInfo);
        logger.info("force to save dict directly");
        return saveNewDict(newDictInfo);
    }

    /**
     * @return may return another dict that is a super set of the input
     * @throws IOException
     * 为一个字典描述对象插入新的字典对象
     *
     * 新的方式是有则不需要保存,直接返回老的即可,如果没有则要保存新的
     * 同时新的方式又增加了是否全局合并成一个字典的情况
     */
    public DictionaryInfo trySaveNewDict(Dictionary<?> newDict, DictionaryInfo newDictInfo) throws IOException {

        initDictInfo(newDict, newDictInfo);

        if (config.isGrowingDictEnabled()) {//true表示字典会自动合并成一个大字典
            DictionaryInfo largestDictInfo = findLargestDictInfo(newDictInfo);//找到磁盘上存储字典内容最多的字典对象
            if (largestDictInfo != null) {
                largestDictInfo = getDictionaryInfo(largestDictInfo.getResourcePath());//字典对应的内存对象
                Dictionary<?> largestDictObject = largestDictInfo.getDictionaryObject();//对应的字典对象
                if (largestDictObject.contains(newDict)) {//包含这些字典了,直接返回即可
                    logger.info("dictionary content " + newDict + ", is contained by  dictionary at " + largestDictInfo.getResourcePath());
                    return largestDictInfo;
                } else if (newDict.contains(largestDictObject)) {//或者新的字典更大,则保存新的字典
                    logger.info("dictionary content " + newDict + " is by far the largest, save it");
                    return saveNewDict(newDictInfo);
                } else {//否则进行合并字典
                    logger.info("merge dict and save...");
                    return mergeDictionary(Lists.newArrayList(newDictInfo, largestDictInfo));
                }
            } else {
                logger.info("first dict of this column, save it directly");
                return saveNewDict(newDictInfo);//第一次保存该tabke 该列的数据,因此直接保存即可
            }
        } else {//说明字典不需要合并成大字典
            logger.info("Growing dict is not enabled");
            String dupDict = checkDupByContent(newDictInfo, newDict);//说明存在的该字典了
            if (dupDict != null) {
                logger.info("Identical dictionary content, reuse existing dictionary at " + dupDict);
                return getDictionaryInfo(dupDict);
            }

            return saveNewDict(newDictInfo);
        }
    }

    //校验是否有两个相同的字典对象
    private String checkDupByContent(DictionaryInfo dictInfo, Dictionary<?> dict) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        NavigableSet<String> existings = store.listResources(dictInfo.getResourceDir());//查询所有的字典集合
        if (existings == null)
            return null;

        //校验字典集合是否已经太多了
        logger.info("{} existing dictionaries of the same column", existings.size());
        if (existings.size() > 100) {//说明该table 该列对应的字典文件太多了
            logger.warn("Too many dictionaries under {}, dict count: {}", dictInfo.getResourceDir(), existings.size());
        }

        //看看是否有相同的字典对象
        for (String existing : existings) {
            DictionaryInfo existingInfo = getDictionaryInfo(existing);
            if (existingInfo != null && dict.equals(existingInfo.getDictionaryObject())) {//字典的内容都相同,说明已经存在了,不需要保存新的,因此直接返回内存中存在的即可
                return existing;
            }
        }

        return null;
    }

    //向描述信息对象中插入字典相关的信息
    private void initDictInfo(Dictionary<?> newDict, DictionaryInfo newDictInfo) {
        newDictInfo.setCardinality(newDict.getSize());//该字典容纳了多少条数据
        newDictInfo.setDictionaryObject(newDict);//设置新的字典对象  对应的字典对象---包含了字典的实现class,以及字典存储的全部内容
        newDictInfo.setDictionaryClass(newDict.getClass().getName());
    }

    //将字典内容保存到磁盘上
    private DictionaryInfo saveNewDict(DictionaryInfo newDictInfo) throws IOException {

        save(newDictInfo);
        dictCache.put(newDictInfo.getResourcePath(), newDictInfo);//添加缓存

        return newDictInfo;
    }

    //对多个字典进行合并
    public DictionaryInfo mergeDictionary(List<DictionaryInfo> dicts) throws IOException {

        if (dicts.size() == 0)
            return null;

        if (dicts.size() == 1)
            return dicts.get(0);

        DictionaryInfo firstDictInfo = null;//第一个字典
        int totalSize = 0;//总字典大小
        for (DictionaryInfo info : dicts) {//循环每一个字典对象
            // check
            if (firstDictInfo == null) {
                firstDictInfo = info;
            } else {
                if (!firstDictInfo.isDictOnSameColumn(info)) {//说明字典不相同,按道理应该是非法的,因此警告
                    // don't throw exception, just output warning as legacy cube segment may build dict on PK
                    logger.warn("Merging dictionaries are not structurally equal : " + firstDictInfo.getResourcePath() + " and " + info.getResourcePath());
                }
            }
            totalSize += info.getInput().getSize();//hive表对应的总大小
        }

        if (firstDictInfo == null) {
            throw new IllegalArgumentException("DictionaryManager.mergeDictionary input cannot be null");
        }

        //identical
        DictionaryInfo newDictInfo = new DictionaryInfo(firstDictInfo);
        TableSignature signature = newDictInfo.getInput();
        signature.setSize(totalSize);
        signature.setLastModifiedTime(System.currentTimeMillis());
        signature.setPath("merged_with_no_original_path");

        //        String dupDict = checkDupByInfo(newDictInfo);
        //        if (dupDict != null) {
        //            logger.info("Identical dictionary input " + newDictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
        //            return getDictionaryInfo(dupDict);
        //        }

        //check for cases where merging dicts are actually same
        boolean identicalSourceDicts = true;;//true表示字典的内容都相同,因此不需要合并
        for (int i = 1; i < dicts.size(); ++i) {
            if (!dicts.get(0).getDictionaryObject().equals(dicts.get(i).getDictionaryObject())) {
                identicalSourceDicts = false;
                break;
            }
        }

        if (identicalSourceDicts) {//不需要合并
            logger.info("Use one of the merging dictionaries directly");
            return dicts.get(0);
        } else {
            Dictionary<?> newDict = DictionaryGenerator.mergeDictionaries(DataType.getType(newDictInfo.getDataType()), dicts);//合并
            return trySaveNewDict(newDict, newDictInfo);
        }
    }

    public DictionaryInfo buildDictionary(DataModelDesc model, TblColRef col, DistinctColumnValuesProvider factTableValueProvider) throws IOException {
        return buildDictionary(model, col, factTableValueProvider, null);
    }

    /**
     * 为该模型的某一列构建字典
     * @param model 模型
     * @param col 模型的某一个列
     * @param factTableValueProvider 如何读取该字段对应的字段值
     * @param builderClass 构建字典的class类,可以为null
     */
    public DictionaryInfo buildDictionary(DataModelDesc model, TblColRef col, DistinctColumnValuesProvider factTableValueProvider, String builderClass) throws IOException {

        logger.info("building dictionary for " + col);

        TblColRef srcCol = decideSourceData(model, col);//如果col是fact表中字段,要转换成lookup表中字段
        String srcTable = srcCol.getTable();//列对应表
        String srcColName = srcCol.getName();//列名
        int srcColIdx = srcCol.getColumnDesc().getZeroBasedIndex();//该列的下标

        ReadableTable inpTable;
        if (model.isFactTable(srcTable)) {//是否是事实表
            inpTable = factTableValueProvider.getDistinctValuesFor(srcCol);//去读取该列对应的内容
        } else {//不是事实表
            MetadataManager metadataManager = MetadataManager.getInstance(config);
            TableDesc tableDesc = new TableDesc(metadataManager.getTableDesc(srcTable));//lookup表对象
            if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(tableDesc.getTableType())) {//是视图
                TableDesc materializedTbl = new TableDesc();
                materializedTbl.setDatabase(config.getHiveDatabaseForIntermediateTable());
                materializedTbl.setName(tableDesc.getMaterializedName());
                inpTable = SourceFactory.createReadableTable(materializedTbl);
            } else {
                inpTable = SourceFactory.createReadableTable(tableDesc);
            }
        }

        TableSignature inputSig = inpTable.getSignature();//表的元数据
        if (inputSig == null) // table does not exists
            return null;

        DictionaryInfo dictInfo = new DictionaryInfo(srcTable, srcColName, srcColIdx, col.getDatatype(), inputSig);

        String dupDict = checkDupByInfo(dictInfo);//校验字典是否重复了,返回路径
        if (dupDict != null) {//说明已经存在
            logger.info("Identical dictionary input " + dictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
            return getDictionaryInfo(dupDict);//加载存在的一个字典对象
        }

        logger.info("Building dictionary object " + JsonUtil.writeValueAsString(dictInfo));

        Dictionary<String> dictionary;
        IDictionaryValueEnumerator columnValueEnumerator = null;//读取该列数据
        try {
            columnValueEnumerator = new TableColumnValueEnumerator(inpTable.getReader(), dictInfo.getSourceColumnIndex());
            if (builderClass == null)
                dictionary = DictionaryGenerator.buildDictionary(DataType.getType(dictInfo.getDataType()), columnValueEnumerator);//根据字段类型,返回一个适合的字典对象
            else
                dictionary = DictionaryGenerator.buildDictionary((IDictionaryBuilder) ClassUtil.newInstance(builderClass), dictInfo, columnValueEnumerator);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create dictionary on " + col, ex);
        } finally {
            if (columnValueEnumerator != null)
                columnValueEnumerator.close();
        }

        return trySaveNewDict(dictionary, dictInfo);
    }

    /**
     * Decide a dictionary's source data, leverage PK-FK relationship.
     * 如果col是fact表中字段,且又是lookup表主键,则要转换成lookup表中字段
     * 否则直接输出原始的col字段
     */
    public TblColRef decideSourceData(DataModelDesc model, TblColRef col) throws IOException {
        // Note FK on fact table is supported by scan the related PK on lookup table
        // FK on fact table and join type is inner, use PK from lookup instead
        if (model.isFactTable(col.getTable())) {//该列是事实表的列
            TblColRef pkCol = model.findPKByFK(col, "inner");//通过fact表外键,找到关联的lookup表主键
            if (pkCol != null)
                col = pkCol; // scan the counterparty PK on lookup table instead
        }
        return col;
    }

    //校验字典是否重复了,返回路径
    private String checkDupByInfo(DictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = MetadataManager.getInstance(config).getStore();
        final List<DictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(), DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);//加载已经存在的字典集合

        TableSignature input = dictInfo.getInput();//每一个字典都对应一个原始数据的签名,签名没变化,说明字典存在

        for (DictionaryInfo dictionaryInfo : allResources) {
            if (input.equals(dictionaryInfo.getInput())) {
                return dictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    //获取字典内词汇量最多的字典对象
    private DictionaryInfo findLargestDictInfo(DictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = MetadataManager.getInstance(config).getStore();
        final List<DictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(), DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);//仅仅加载该table和该列的字典描述信息

        DictionaryInfo largestDict = null;
        for (DictionaryInfo dictionaryInfo : allResources) {//循环所有的内容
            if (largestDict == null) {
                largestDict = dictionaryInfo;
                continue;
            }

            if (largestDict.getCardinality() < dictionaryInfo.getCardinality()) {
                largestDict = dictionaryInfo;
            }
        }
        return largestDict;
    }

    //删除某一个字典文件
    public void removeDictionary(String resourcePath) throws IOException {
        logger.info("Remvoing dict: " + resourcePath);
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        store.deleteResource(resourcePath);
        dictCache.invalidate(resourcePath);//缓存中删除该路径
    }

    //删除该table对应的字段列对应的所有字典信息----因为一个表--一个字段不同时期建立不同的字典,因此是会存在多个字典对象的
    public void removeDictionaries(String srcTable, String srcCol) throws IOException {
        DictionaryInfo info = new DictionaryInfo();
        info.setSourceTable(srcTable);
        info.setSourceColumn(srcCol);

        ResourceStore store = MetadataManager.getInstance(config).getStore();
        NavigableSet<String> existings = store.listResources(info.getResourceDir());//获取全部字典信息存储的path
        if (existings == null)
            return;

        //以此删除该字典内容
        for (String existing : existings)
            removeDictionary(existing);
    }

    //保存一个字典对象---即保存到HBase上
    void save(DictionaryInfo dict) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        String path = dict.getResourcePath();
        logger.info("Saving dictionary at " + path);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        DictionaryInfoSerializer.FULL_SERIALIZER.serialize(dict, dout);//全部序列化字典的描述信息对象
        dout.close();
        buf.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf.toByteArray());
        store.putResource(path, inputStream, System.currentTimeMillis());
        inputStream.close();
    }

    //加载磁盘上的一个字典对象
    //参数loadDictObj表示是否加载字典对象,因为字典内容比较多,加载会很慢
    DictionaryInfo load(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();

        logger.info("DictionaryManager(" + System.identityHashCode(this) + ") loading DictionaryInfo(loadDictObj:" + loadDictObj + ") at " + resourcePath);
        DictionaryInfo info = store.getResource(resourcePath, DictionaryInfo.class, loadDictObj ? DictionaryInfoSerializer.FULL_SERIALIZER : DictionaryInfoSerializer.INFO_SERIALIZER);

        //        if (loadDictObj)
        //            logger.debug("Loaded dictionary at " + resourcePath);

        return info;
    }

}
