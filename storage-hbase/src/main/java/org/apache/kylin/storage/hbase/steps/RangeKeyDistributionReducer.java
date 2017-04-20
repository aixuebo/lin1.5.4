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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 * 用于对所有的rowkey进行划分范围
 */
public class RangeKeyDistributionReducer extends KylinReducer<Text, LongWritable, Text, LongWritable> {

    public static final long ONE_GIGA_BYTES = 1024L * 1024L * 1024L;//1G
    private static final Logger logger = LoggerFactory.getLogger(RangeKeyDistributionReducer.class);

    private LongWritable outputValue = new LongWritable(0);

    private int minRegionCount = 1;
    private int maxRegionCount = 500;
    private float cut = 10.0f;
    private int hfileSizeGB = 1;
    private long bytesRead = 0;
    private List<Text> gbPoints = new ArrayList<Text>();//切分点,切分点的内容是rowkey,1G一个切分点
    private String output = null;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        if (context.getConfiguration().get(BatchConstants.CFG_OUTPUT_PATH) != null) {
            output = context.getConfiguration().get(BatchConstants.CFG_OUTPUT_PATH);
        }

        if (context.getConfiguration().get(BatchConstants.CFG_HFILE_SIZE_GB) != null) {
            hfileSizeGB = Float.valueOf(context.getConfiguration().get(BatchConstants.CFG_HFILE_SIZE_GB)).intValue();
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_SPLIT_SIZE) != null) {
            cut = Float.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_SPLIT_SIZE));
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MIN) != null) {
            minRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MIN));
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MAX) != null) {
            maxRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MAX));
        }

        logger.info("Chosen cut for htable is " + cut + ", max region count=" + maxRegionCount + ", min region count=" + minRegionCount + ", hfile size=" + hfileSizeGB);

        // add empty key at position 0
        gbPoints.add(new Text());//先初始化一个空的切分点,表示0
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable v : values) {
            bytesRead += v.get();
        }

        if (bytesRead >= ONE_GIGA_BYTES) {//达到一G就设置一个切分点
            gbPoints.add(new Text(key));//切分点就是rowkey
            bytesRead = 0; // reset bytesRead
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int nRegion = Math.round((float) gbPoints.size() / cut); //最终切分成多少个region
        //校验region切分的数量一定在最大值和最小值之间
        nRegion = Math.max(minRegionCount, nRegion);
        nRegion = Math.min(maxRegionCount, nRegion);

        int gbPerRegion = gbPoints.size() / nRegion;//表示每一个region里面有多少个切分点,即有多少G数据
        gbPerRegion = Math.max(1, gbPerRegion);//最小是1G

        if (hfileSizeGB <= 0) {
            hfileSizeGB = gbPerRegion;
        }
        int hfilePerRegion = (int) (gbPerRegion / hfileSizeGB);//表示一个region里面需要多少个hfile文件
        hfilePerRegion = Math.max(1, hfilePerRegion);//hfile文件至少也是1个

        System.out.println(nRegion + " regions");//打印多少个region
        System.out.println(gbPerRegion + " GB per region");//每一个region多少G数据
        System.out.println(hfilePerRegion + " hfile per region");//每一个region有多少个hfile文件


        Path hfilePartitionFile = new Path(output + "/part-r-00000_hfile");//创建hfile文件---存储每一个切分点的key
        SequenceFile.Writer hfilePartitionWriter = new SequenceFile.Writer(hfilePartitionFile.getFileSystem(context.getConfiguration()), context.getConfiguration(), hfilePartitionFile, ImmutableBytesWritable.class, NullWritable.class);//文件内容key是ImmutableBytesWritable,value是NullWritable
        int hfileCountInOneRegion = 0;//表示一个region里面已经存放多少个hfile文件了
        for (int i = hfileSizeGB; i < gbPoints.size(); i += hfileSizeGB) {
            hfilePartitionWriter.append(new ImmutableBytesWritable(gbPoints.get(i).getBytes()), NullWritable.get());//存放分割点的rowkey
            if (++hfileCountInOneRegion >= hfilePerRegion) {//表示该切换region了
                Text key = gbPoints.get(i);
                outputValue.set(i);
                System.out.println(StringUtils.byteToHexString(key.getBytes()) + "\t" + outputValue.get());
                context.write(key, outputValue);//key是切换region时候的rowkey,value是第几个切分点-----配合hfilePartitionFile文件可以读取到value第几个代表的是哪个切分点

                hfileCountInOneRegion = 0;
            }
        }
        hfilePartitionWriter.close();
    }
}
