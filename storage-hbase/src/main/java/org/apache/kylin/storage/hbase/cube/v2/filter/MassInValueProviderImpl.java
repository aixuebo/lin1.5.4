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

package org.apache.kylin.storage.hbase.cube.v2.filter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.filter.UDF.MassInValueProvider;
import org.apache.kylin.metadata.filter.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;

public class MassInValueProviderImpl implements MassInValueProvider {
    public static final Logger logger = LoggerFactory.getLogger(MassInValueProviderImpl.class);

    //path作为key,value是该path的最后修改时间戳以及对应的具体的值,因此就可以起到缓存的作用了
    private final static Cache<String, Pair<Long, Set<ByteArray>>> hdfs_caches = CacheBuilder.newBuilder().maximumSize(3).removalListener(new RemovalListener<Object, Object>() {
        @Override
        public void onRemoval(RemovalNotification<Object, Object> notification) {
            logger.debug(String.valueOf(notification.getCause()));
        }
    }).build();

    private Set<ByteArray> ret = Sets.newHashSet();

    /**
     *
     * @param filterTableType 存储在hbase的table上,还是存储在hdfs上
     * @param filterResourceIdentifier 具体的habse的tableName 还是hdfs上path
     * @param encoding
     */
    public MassInValueProviderImpl(Functions.FilterTableType filterTableType, String filterResourceIdentifier, DimensionEncoding encoding) {

        if (filterTableType == Functions.FilterTableType.HDFS) {

            logger.info("Start to load HDFS filter table from " + filterResourceIdentifier);
            Stopwatch stopwatch = new Stopwatch().start();

            FileSystem fileSystem = null;
            try {
                synchronized (hdfs_caches) {

                    // directly create hbase configuration here due to no KYLIN_CONF definition.
                    fileSystem = FileSystem.get(HBaseConfiguration.create());

                    long modificationTime = fileSystem.getFileStatus(new Path(filterResourceIdentifier)).getModificationTime();//该路径的最后修改时间
                    Pair<Long, Set<ByteArray>> cached = hdfs_caches.getIfPresent(filterResourceIdentifier);//说明缓存存在
                    if (cached != null && cached.getFirst().equals(modificationTime)) {//时间戳也对得上
                        ret = cached.getSecond();//从缓存获取数据
                        logger.info("Load HDFS from cache using " + stopwatch.elapsedMillis() + " millis");
                        return;
                    }

                    //具体读取数据
                    InputStream inputStream = fileSystem.open(new Path(filterResourceIdentifier));
                    List<String> lines = IOUtils.readLines(inputStream, Charset.defaultCharset());

                    logger.info("Load HDFS finished after " + stopwatch.elapsedMillis() + " millis");

                    for (String line : lines) {
                        if (StringUtils.isEmpty(line)) {
                            continue;
                        }

                        try {
                            ByteArray byteArray = ByteArray.allocate(encoding.getLengthOfEncoding());
                            encoding.encode(line.getBytes(), line.getBytes().length, byteArray.array(), 0);
                            ret.add(byteArray);
                        } catch (Exception e) {
                            logger.warn("Error when encoding the filter line " + line);
                        }
                    }

                    hdfs_caches.put(filterResourceIdentifier, Pair.newPair(modificationTime, ret));

                    logger.info("Mass In values constructed after " + stopwatch.elapsedMillis() + " millis, containing " + ret.size() + " entries");
                }

            } catch (IOException e) {
                throw new RuntimeException("error when loading the mass in values", e);
            }
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }
    }

    @Override
    public Set<?> getMassInValues() {
        return ret;
    }
}
