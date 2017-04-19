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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;

/**
 * 输出cuboid的统计信息
 */
public class CuboidStatsUtil {

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HyperLogLogPlusCounter> cuboidHLLMap, int samplingPercentage) throws IOException {
        writeCuboidStatistics(conf, outputPath, cuboidHLLMap, samplingPercentage, 0);
    }

    /**
     *
     * @param conf
     * @param outputPath 统计输出的目录
     * @param cuboidHLLMap 每一个cubodi对应的HyperLogLogPlusCounter对象
     * @param samplingPercentage 抽样的百分比---即每隔多少个元素则抽样一个数据
     * @param mapperOverlapRatio 重叠比例
     */
    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HyperLogLogPlusCounter> cuboidHLLMap, int samplingPercentage, double mapperOverlapRatio) throws IOException {
        Path seqFilePath = new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);//创建序列化文件

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(cuboidHLLMap.keySet());//所有cuboid集合
        Collections.sort(allCuboids);

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);//创建缓冲区
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(BytesWritable.class));
        try {
            // mapper overlap ratio at key -1
            writer.append(new LongWritable(-1), new BytesWritable(Bytes.toBytes(mapperOverlapRatio)));//添加重叠比例 key是-1

            // sampling percentage at key 0
            writer.append(new LongWritable(0L), new BytesWritable(Bytes.toBytes(samplingPercentage)));//添加百分比  key是0

            for (long i : allCuboids) {//添加每一个cuboid对应的对象
                valueBuf.clear();
                cuboidHLLMap.get(i).writeRegisters(valueBuf);//将内容写出到输出流中
                valueBuf.flip();
                writer.append(new LongWritable(i), new BytesWritable(valueBuf.array(), valueBuf.limit()));
            }
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

}
