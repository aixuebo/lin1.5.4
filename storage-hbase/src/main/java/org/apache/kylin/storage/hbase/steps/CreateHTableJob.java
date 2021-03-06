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

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CuboidShardUtil;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 创建hbase的一个table,使用规划好的rowkey进行拆分该table的regionSplit
 *
 * 读取rowkey的分布范围--创建hbase的table表
 */
public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);

    CubeInstance cube = null;
    CubeDesc cubeDesc = null;
    String segmentID = null;
    KylinConfig kylinConfig;
    Path partitionFilePath;//rowkey划分存储的partition的路径  读取rowkey的分布范围---存储rowkey范围,按照该范围进行region拆分

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_PARTITION_FILE_PATH);//读取rowkey的分布范围
        options.addOption(OPTION_STATISTICS_ENABLED);//是否统计
        parseOptions(options, args);

        partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));
        boolean statsEnabled = Boolean.parseBoolean(getOptionValue(OPTION_STATISTICS_ENABLED));//是否统计

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        cube = cubeMgr.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        kylinConfig = cube.getConfig();
        segmentID = getOptionValue(OPTION_SEGMENT_ID);
        CubeSegment cubeSegment = cube.getSegmentById(segmentID);//获取本次要插入的segment对象

        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();//获取hbase的配置信息

        try {
            byte[][] splitKeys;//二维数组,第一维度表示每一个resgion范围,第二个维度表示每一个region分区时候的rowkey
            if (statsEnabled) {//如果统计
                final Map<Long, Double> cuboidSizeMap = new CubeStatsReader(cubeSegment, kylinConfig).getCuboidSizeMap();//计算每一个cuboid占用多少字节,单位是M
                splitKeys = getRegionSplitsFromCuboidStatistics(cuboidSizeMap, kylinConfig, cubeSegment, partitionFilePath.getParent());
            } else {//不统计
                splitKeys = getRegionSplits(conf, partitionFilePath);
            }

            CubeHTableUtil.createHTable(cubeSegment, splitKeys);//为segment创建hbase的表--分配每一个region范围,分配协处理器
            return 0;
        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            logger.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    //获取多少个region分区,每一个分区对应的是rowkey的字节数组
    @SuppressWarnings("deprecation")
    public byte[][] getRegionSplits(Configuration conf, Path path) throws Exception {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path) == false) {
            System.err.println("Path " + path + " not found, no region split, HTable will be one region");
            return null;
        }

        List<byte[]> rowkeyList = new ArrayList<byte[]>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);//读取hdfs上的数据
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);//rowkey
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);//index,key在切割点集合中的下标
            while (reader.next(key, value)) {//每次读取一行数据
                rowkeyList.add(((Text) key).copyBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.closeStream(reader);
        }

        logger.info((rowkeyList.size() + 1) + " regions");
        logger.info(rowkeyList.size() + " splits");
        for (byte[] split : rowkeyList) {
            logger.info(StringUtils.byteToHexString(split));
        }

        byte[][] retValue = rowkeyList.toArray(new byte[rowkeyList.size()][]);
        return retValue.length == 0 ? null : retValue;
    }

    //one region for one shard
    private static byte[][] getSplitsByRegionCount(int regionCount) {
        byte[][] result = new byte[regionCount - 1][];
        for (int i = 1; i < regionCount; ++i) {
            byte[] split = new byte[RowConstants.ROWKEY_SHARDID_LEN];
            BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
            result[i - 1] = split;
        }
        return result;
    }

    /**
     *
     * @param cubeSizeMap 计算每一个cuboid占用多少字节,单位是M
     * @param kylinConfig
     * @param cubeSegment
     * @param hfileSplitsOutputFolder
     * @return
     * @throws IOException 返回值是每一个region存储的cuboid的边界
     */
    public static byte[][] getRegionSplitsFromCuboidStatistics(final Map<Long, Double> cubeSizeMap, final KylinConfig kylinConfig, final CubeSegment cubeSegment, final Path hfileSplitsOutputFolder) throws IOException {

        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        float cut = cubeDesc.getConfig().getKylinHBaseRegionCut();//单位G,每一个region存储多大数据

        logger.info("Cut for HBase region is " + cut + "GB");

        double totalSizeInM = 0;//所有的cuboid占用的总字节
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }

        List<Long> allCuboids = Lists.newArrayList();//存储所有的cuboid集合
        allCuboids.addAll(cubeSizeMap.keySet());
        Collections.sort(allCuboids);//排序

        int nRegion = Math.round((float) (totalSizeInM / (cut * 1024L)));//region数量,理论上totalSizeInM>cut,因此表示可以拆分成多少个region
        //校验范围在最大值和最小值之间
        nRegion = Math.max(kylinConfig.getHBaseRegionCountMin(), nRegion);
        nRegion = Math.min(kylinConfig.getHBaseRegionCountMax(), nRegion);

        if (cubeSegment.isEnableSharding()) {//&& (nRegion > 1)) {存在sharding分片
            //use prime nRegions to help random sharding
            int original = nRegion;
            if (nRegion == 0) {
                nRegion = 1;
            }

            if (nRegion > Short.MAX_VALUE) {
                logger.info("Too many regions! reduce to " + Short.MAX_VALUE);
                nRegion = Short.MAX_VALUE;
            }

            if (nRegion != original) {//说明region数量有调整了
                logger.info("Region count is adjusted from " + original + " to " + nRegion + " to help random sharding");
            }
        }

        int mbPerRegion = (int) (totalSizeInM / nRegion);//每一个region最终多大
        mbPerRegion = Math.max(1, mbPerRegion);

        logger.info("Total size " + totalSizeInM + "M (estimated)");
        logger.info("Expecting " + nRegion + " regions.");
        logger.info("Expecting " + mbPerRegion + " MB per region.");

        if (cubeSegment.isEnableSharding()) {
            //each cuboid will be split into different number of shards
            HashMap<Long, Short> cuboidShards = Maps.newHashMap();

            //each shard/region may be split into multiple hfiles; array index: region ID, Map: key: cuboidID, value cuboid size in the region
            List<HashMap<Long, Double>> innerRegionSplits = Lists.newArrayList();
            for (int i = 0; i < nRegion; i++) {
                innerRegionSplits.add(new HashMap<Long, Double>());
            }
            
            double[] regionSizes = new double[nRegion];
            for (long cuboidId : allCuboids) {//循环所有的cuboid
                double estimatedSize = cubeSizeMap.get(cuboidId);//该cuboid对应的size,单位是M
                double magic = 23;
                int shardNum = (int) (estimatedSize * magic / mbPerRegion + 1);
                if (shardNum < 1) {
                    shardNum = 1;
                }

                if (shardNum > nRegion) {
                    logger.info(String.format("Cuboid %d 's estimated size %.2f MB will generate %d regions, reduce to %d", cuboidId, estimatedSize, shardNum, nRegion));
                    shardNum = nRegion;
                } else {
                    logger.info(String.format("Cuboid %d 's estimated size %.2f MB will generate %d regions", cuboidId, estimatedSize, shardNum));
                }

                cuboidShards.put(cuboidId, (short) shardNum);
                short startShard = ShardingHash.getShard(cuboidId, nRegion);
                for (short i = startShard; i < startShard + shardNum; ++i) {
                    short j = (short) (i % nRegion);
                    regionSizes[j] = regionSizes[j] + estimatedSize / shardNum;
                    innerRegionSplits.get(j).put(cuboidId, estimatedSize / shardNum);
                }
            }

            for (int i = 0; i < nRegion; ++i) {
                logger.info(String.format("Region %d's estimated size is %.2f MB, accounting for %.2f percent", i, regionSizes[i], 100.0 * regionSizes[i] / totalSizeInM));
            }

            CuboidShardUtil.saveCuboidShards(cubeSegment, cuboidShards, nRegion);
            saveHFileSplits(innerRegionSplits, mbPerRegion, hfileSplitsOutputFolder, kylinConfig);
            return getSplitsByRegionCount(nRegion);

        } else {
            List<Long> regionSplit = Lists.newArrayList();//拆分点的cuboid

            long size = 0;//此时region的大小
            int regionIndex = 0;
            int cuboidCount = 0;//此时region中包含的cuboid的数量
            for (int i = 0; i < allCuboids.size(); i++) {//循环所有的cuboid
                long cuboidId = allCuboids.get(i);
                if (size >= mbPerRegion || (size + cubeSizeMap.get(cuboidId)) >= mbPerRegion * 1.2) {//切换一个region
                    // if the size already bigger than threshold, or it will exceed by 20%, cut for next region
                    regionSplit.add(cuboidId);
                    logger.info("Region " + regionIndex + " will be " + size + " MB, contains cuboids < " + cuboidId + " (" + cuboidCount + ") cuboids");//打印每一个region的最终大小,单位M,该segment包含多少个cuboid
                    size = 0;
                    cuboidCount = 0;
                    regionIndex++;
                }
                size += cubeSizeMap.get(cuboidId);
                cuboidCount++;
            }

            byte[][] result = new byte[regionSplit.size()][];
            for (int i = 0; i < regionSplit.size(); i++) {
                result[i] = Bytes.toBytes(regionSplit.get(i));
            }

            return result;
        }
    }

    /**
     *
     * @param innerRegionSplits
     * @param mbPerRegion 每一个region多少M
     * @param outputFolder 存储路径
     * @param kylinConfig
     * @throws IOException
     */
    protected static void saveHFileSplits(final List<HashMap<Long, Double>> innerRegionSplits, int mbPerRegion, final Path outputFolder, final KylinConfig kylinConfig) throws IOException {

        if (outputFolder == null) {
            logger.warn("outputFolder for hfile split file is null, skip inner region split");
            return;
        }

        // note read-write separation, respect HBase FS here
        Configuration hbaseConf = HBaseConnection.getCurrentHBaseConfiguration();
        FileSystem fs = FileSystem.get(hbaseConf);
        if (fs.exists(outputFolder) == false) {//创建该文件路径
            fs.mkdirs(outputFolder);
        }

        final float hfileSizeGB = kylinConfig.getHBaseHFileSizeGB();//单位GB,hbase的Hfile文件大小
        float hfileSizeMB = hfileSizeGB * 1024;//转换成M
        if (hfileSizeMB > mbPerRegion) {
            hfileSizeMB = mbPerRegion;
        }

        // keep the tweak for sandbox test
        if (hfileSizeMB > 0.0 && kylinConfig.isDevEnv()) {//说明是开发环境
            hfileSizeMB = mbPerRegion / 2;
        }

        int compactionThreshold = Integer.valueOf(hbaseConf.get("hbase.hstore.compactionThreshold", "3"));//压缩率
        logger.info("hbase.hstore.compactionThreshold is " + compactionThreshold);
        if (hfileSizeMB > 0.0 && hfileSizeMB * compactionThreshold < mbPerRegion) {
            hfileSizeMB = mbPerRegion / compactionThreshold;
        }

        if (hfileSizeMB <= 0) {
            hfileSizeMB = mbPerRegion;
        }
        logger.info("hfileSizeMB:" + hfileSizeMB);
        final Path hfilePartitionFile = new Path(outputFolder, "part-r-00000_hfile");
        short regionCount = (short) innerRegionSplits.size();

        List<byte[]> splits = Lists.newArrayList();
        for (int i = 0; i < regionCount; i++) {
            if (i > 0) {
                // skip 0
                byte[] split = new byte[RowConstants.ROWKEY_SHARDID_LEN];
                BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
                splits.add(split); // split by region;
            }

            HashMap<Long, Double> cuboidSize = innerRegionSplits.get(i);
            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidSize.keySet());
            Collections.sort(allCuboids);

            double accumulatedSize = 0;
            int j = 0;
            for (Long cuboid : allCuboids) {
                if (accumulatedSize >= hfileSizeMB) {
                    logger.info(String.format("Region %d's hfile %d size is %.2f mb", i, j, accumulatedSize));
                    byte[] split = new byte[RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN];
                    BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
                    System.arraycopy(Bytes.toBytes(cuboid), 0, split, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
                    splits.add(split);
                    accumulatedSize = 0;
                    j++;
                }
                accumulatedSize += cuboidSize.get(cuboid);
            }

        }

        SequenceFile.Writer hfilePartitionWriter = SequenceFile.createWriter(hbaseConf, SequenceFile.Writer.file(hfilePartitionFile), SequenceFile.Writer.keyClass(ImmutableBytesWritable.class), SequenceFile.Writer.valueClass(NullWritable.class));

        for (int i = 0; i < splits.size(); i++) {
            hfilePartitionWriter.append(new ImmutableBytesWritable(splits.get(i)), NullWritable.get());
        }
        hfilePartitionWriter.close();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateHTableJob(), args);
        System.exit(exitCode);
    }
}
