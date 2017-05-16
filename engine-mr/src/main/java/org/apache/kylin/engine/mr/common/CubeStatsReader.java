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

package org.apache.kylin.engine.mr.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.SumHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This should be in cube module. It's here in engine-mr because currently stats
 * are saved as sequence files thus a hadoop dependency.
 * 这个是一个MR任务,因为当前任务的状态被保存在hadoop依赖的sequence文件中了
 */
public class CubeStatsReader {

    private static final Logger logger = LoggerFactory.getLogger(CubeStatsReader.class);

    final CubeSegment seg;
    final int samplingPercentage;
    final double mapperOverlapRatioOfFirstBuild; // only makes sense for the first build, is meaningless after merge
    final Map<Long, HyperLogLogPlusCounter> cuboidRowEstimatesHLL;

    //一个segment一个统计文件
    public CubeStatsReader(CubeSegment cubeSegment, KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        String statsKey = cubeSegment.getStatisticsResourcePath();//统计文件存储位置  /cube_statistics/cubeName/cubeSegmentId.seq
        File tmpSeqFile = writeTmpSeqFile(store.getResource(statsKey).inputStream);//将文件写入到本地文件系统中
        Reader reader = null;

        try {
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();

            //读取下载的本地文件
            Path path = new Path(HadoopUtil.fixWindowsPath("file://" + tmpSeqFile.getAbsolutePath()));
            Option seqInput = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(hadoopConf, seqInput);

            int percentage = 100;//抽样的百分比
            double mapperOverlapRatio = 0;//map的重复率
            //每一个cuboid对应一个对象
            Map<Long, HyperLogLogPlusCounter> counterMap = Maps.newHashMap();

            //获取key和value的class类
            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), hadoopConf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), hadoopConf);
            while (reader.next(key, value)) {//不断的读取文件的数据
                if (key.get() == 0L) {
                    percentage = Bytes.toInt(value.getBytes());
                } else if (key.get() == -1) {
                    mapperOverlapRatio = Bytes.toDouble(value.getBytes());
                } else {
                    HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(kylinConfig.getCubeStatsHLLPrecision());
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());
                    counterMap.put(key.get(), hll);//key就是cuboid
                }
            }

            this.seg = cubeSegment;
            this.samplingPercentage = percentage;
            this.mapperOverlapRatioOfFirstBuild = mapperOverlapRatio;
            this.cuboidRowEstimatesHLL = counterMap;

        } finally {
            IOUtils.closeStream(reader);
            tmpSeqFile.delete();
        }
    }

    //将文件写入到本地文件系统中
    private File writeTmpSeqFile(InputStream inputStream) throws IOException {
        File tempFile = File.createTempFile("kylin_stats_tmp", ".seq");
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(tempFile);
            org.apache.commons.io.IOUtils.copy(inputStream, out);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(out);
        }
        return tempFile;
    }

    //返回每一个cuboid 有多少个不同的数据值
    public Map<Long, Long> getCuboidRowEstimatesHLL() {
        return getCuboidRowCountMapFromSampling(cuboidRowEstimatesHLL, samplingPercentage);
    }

    // return map of Cuboid ID => MB
    //计算每一个cuboid占用多少字节,单位是M
    public Map<Long, Double> getCuboidSizeMap() {
        return getCuboidSizeMapFromRowCount(seg, getCuboidRowEstimatesHLL());
    }

    public double getMapperOverlapRatioOfFirstBuild() {
        return mapperOverlapRatioOfFirstBuild;
    }

    //返回每一个cuboid 有多少个不同的数据值
    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HyperLogLogPlusCounter> hllcMap, int samplingPercentage) {
        Map<Long, Long> cuboidRowCountMap = Maps.newHashMap();
        for (Map.Entry<Long, HyperLogLogPlusCounter> entry : hllcMap.entrySet()) {
            // No need to adjust according sampling percentage. Assumption is that data set is far
            // more than cardinality. Even a percentage of the data should already see all cardinalities.
            cuboidRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
        }
        return cuboidRowCountMap;
    }

    /**
     * @param cubeSegment
     * @param rowCountMap 返回每一个cuboid 有多少个不同的数据值
     */
    public static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();//每一个列对应的编码后的固定长度
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final List<TblColRef> columnList = baseCuboid.getColumns();
        final CubeDimEncMap dimEncMap = cubeSegment.getDimensionEncodingMap();

        for (int i = 0; i < columnList.size(); i++) {
            rowkeyColumnSize.add(dimEncMap.get(columnList.get(i)).getLengthOfEncoding());
        }

        //key是每一个cuboid,value是该cuboid占用大小,单位是M
        Map<Long, Double> sizeMap = Maps.newHashMap();
        for (Map.Entry<Long, Long> entry : rowCountMap.entrySet()) {//计算多少个不同元素占用字节
            sizeMap.put(entry.getKey(), estimateCuboidStorageSize(cubeSegment, entry.getKey(), entry.getValue(), baseCuboidId, rowkeyColumnSize));
        }
        return sizeMap;
    }

    /**
     * Estimate the cuboid's size
     * @param cubeSegment
     * @param cuboidId
     * @param rowCount 该cuboid有多少个不同的数值
     * @param baseCuboidId
     * @param rowKeyColumnLength //每一个列对应编码后的长度
     * @return 计算多少个不同元素占用字节  单位是M
     */
    private static double estimateCuboidStorageSize(CubeSegment cubeSegment, long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

        /**
         * 因为每个cuboid存储到hbase占用的空间取决于:rowkey+value,
         * 1.rowkey由sharding需要的字节长度+cuboid需要的字节长度 + 真正的rowkey的编码后的值组成
         * 2.value由度量的结果组成
         *
         */
        int bytesLength = cubeSegment.getRowKeyPreambleSize();
        KylinConfig kylinConf = cubeSegment.getConfig();

        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < parentCuboidIdActualLength; i++) {//表示该cuboid有多少个1,即有多少个字段
            if ((mask & cuboidId) > 0) {
                bytesLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));//表示每一个字段需要编码后的字节长度
            }
            mask = mask >> 1;
        }

        // add the measure length 添加度量需要的长度
        int space = 0;
        boolean isMemoryHungry = false;
        for (MeasureDesc measureDesc : cubeSegment.getCubeDesc().getMeasures()) {//循环所有的度量
            if (measureDesc.getFunction().getMeasureType().isMemoryHungry()) {
                isMemoryHungry = true;
            }
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            space += returnType.getStorageBytesEstimate();
        }
        bytesLength += space;

        //每一个cuboid的固定长度*总内容数量,即最终的占用大小,单位是M
        double ret = 1.0 * bytesLength * rowCount / (1024L * 1024L);
        if (isMemoryHungry) {
            double cuboidSizeMemHungryRatio = kylinConf.getJobCuboidSizeMemHungryRatio();//默认值0.05
            logger.info("Cube is memory hungry, storage size estimation multiply " + cuboidSizeMemHungryRatio);
            ret *= cuboidSizeMemHungryRatio;//ret = ret * cuboidSizeMemHungryRatio,不理解为什么后来变得更小了
        } else {
            double cuboidSizeRatio = kylinConf.getJobCuboidSizeRatio();//默认值0.25
            logger.info("Cube is not memory hungry, storage size estimation multiply " + cuboidSizeRatio);
            ret *= cuboidSizeRatio;
        }
        logger.info("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength + " bytes." + " Total size is " + ret + "M.");
        return ret;
    }

    private void print(PrintWriter out) {
        Map<Long, Long> cuboidRows = getCuboidRowEstimatesHLL();//每一个cuboid有多少个不同元素
        Map<Long, Double> cuboidSizes = getCuboidSizeMap();//每一个cuboid占用字节数
        List<Long> cuboids = new ArrayList<Long>(cuboidRows.keySet());
        Collections.sort(cuboids);

        out.println("============================================================================");
        out.println("Statistics of " + seg);
        out.println();
        out.println("Cube statistics hll precision: " + cuboidRowEstimatesHLL.values().iterator().next().getPrecision());
        out.println("Total cuboids: " + cuboidRows.size());//一共产生多少个cuboid
        out.println("Total estimated rows: " + SumHelper.sumLong(cuboidRows.values()));//总不同的数量
        out.println("Total estimated size(MB): " + SumHelper.sumDouble(cuboidSizes.values()));//总大小
        out.println("Sampling percentage:  " + samplingPercentage);
        out.println("Mapper overlap ratio: " + mapperOverlapRatioOfFirstBuild);
        printKVInfo(out);    //打印每一个属性对应编码后的字节长度
        printCuboidInfoTreeEntry(cuboidRows, cuboidSizes, out);//递归打印所有cuboid的内容
        out.println("----------------------------------------------------------------------------");
    }

    private void printCuboidInfoTreeEntry(Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, PrintWriter out) {
        CubeDesc cubeDesc = seg.getCubeDesc();
        CuboidScheduler scheduler = new CuboidScheduler(cubeDesc);
        long baseCuboid = Cuboid.getBaseCuboidId(cubeDesc);
        int dimensionCount = Long.bitCount(baseCuboid);
        printCuboidInfoTree(-1L, baseCuboid, scheduler, cuboidRows, cuboidSizes, dimensionCount, 0, out);
    }

    //打印每一个属性对应编码后的字节长度
    private void printKVInfo(PrintWriter writer) {
        Cuboid cuboid = Cuboid.getBaseCuboid(seg.getCubeDesc());
        RowKeyEncoder encoder = new RowKeyEncoder(seg, cuboid);
        for (TblColRef col : cuboid.getColumns()) {
            writer.println("Length of dimension " + col + " is " + encoder.getColumnLength(col));
        }
    }

    private static void printCuboidInfoTree(long parent, long cuboidID, final CuboidScheduler scheduler, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        printOneCuboidInfo(parent, cuboidID, cuboidRows, cuboidSizes, dimensionCount, depth, out);

        List<Long> children = scheduler.getSpanningCuboid(cuboidID);//获取该cuboidID下所有子cuboidID
        Collections.sort(children);

        for (Long child : children) {
            printCuboidInfoTree(cuboidID, child, scheduler, cuboidRows, cuboidSizes, dimensionCount, depth + 1, out);
        }
    }

    /**
     *
     * @param parent 父cubouid
     * @param cuboidID 当前处理的cuboid
     * @param cuboidRows 每一个cuboid有多少个不同的数据
     * @param cuboidSizes 每一个cuboid占用空间,单位M
     * @param dimensionCount basecuboid有多少个1,即有多少维
     * @param depth 第几层
     * @param out
     */
    private static void printOneCuboidInfo(long parent, long cuboidID, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < depth; i++) {//多少深度,就打印多少个空格
            sb.append("    ");
        }
        String cuboidName = Cuboid.getDisplayName(cuboidID, dimensionCount);//其实就是打印对应的二进制,让其知道cuboid是哪些位置是1
        sb.append("|---- Cuboid ").append(cuboidName);

        long rowCount = cuboidRows.get(cuboidID);
        double size = cuboidSizes.get(cuboidID);
        sb.append(", est row: ").append(rowCount).append(", est MB: ").append(formatDouble(size));//打印多少行不同的数据,以及占用多少M空间

        if (parent != -1) {//有父类,则打印收缩百分比,即当前节点不同值的数量/父节点不同值的数量
            sb.append(", shrink: ").append(formatDouble(100.0 * cuboidRows.get(cuboidID) / cuboidRows.get(parent))).append("%");
        }

        out.println(sb.toString());
    }

    private static String formatDouble(double input) {
        return new DecimalFormat("#.##").format(input);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("CubeStatsReader is used to read cube statistic saved in metadata store");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCube(args[0]);
        List<CubeSegment> segments = cube.getSegments(SegmentStatusEnum.READY);

        PrintWriter out = new PrintWriter(System.out);
        for (CubeSegment seg : segments) {
            new CubeStatsReader(seg, config).print(out);
        }
        out.flush();
    }

}
