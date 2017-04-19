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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author George Song (ysong1)
 * 每一次的输入都是上一次cuboid的输出----切上一次的维度肯定比这一次的维度高
 */
public class NDCuboidMapper extends KylinMapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(NDCuboidMapper.class);

    private Text outputKey = new Text();
    private String cubeName;
    private String segmentID;
    private CubeSegment cubeSegment;
    private CubeDesc cubeDesc;
    private CuboidScheduler cuboidScheduler;

    private int handleCounter;//一个处理了多少条数据
    private int skipCounter;//没有子cuboid,因此跳过的记录数

    private byte[] newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];
    private ByteArray newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeSegment = cube.getSegmentById(segmentID);
        cubeDesc = cube.getDescriptor();

        // initialize CubiodScheduler
        cuboidScheduler = new CuboidScheduler(cubeDesc);

        rowKeySplitter = new RowKeySplitter(cubeSegment, 65, 256);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(cubeSegment);
    }

    /**
     * @param parentCuboid 父cuboid
     * @param childCuboid 子cuboid
     * @param splitBuffers 数据内容
     */
    private int buildKey(Cuboid parentCuboid, Cuboid childCuboid, SplittedBytes[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(childCuboid);

        int offset = 0;//在newKeyBodyBuf中添加到哪个位置了

        // rowkey columns
        long mask = Long.highestOneBit(parentCuboid.getId());//父Cuboid的最高位
        long parentCuboidId = parentCuboid.getId();
        long childCuboidId = childCuboid.getId();

        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(parentCuboid.getId());// 表示Long.SIZE - 前面有多少个0被省略了,即有多少个位置可以去循环
        int index = rowKeySplitter.getBodySplitOffset(); // skip shard and cuboidId
        for (int i = 0; i < parentCuboidIdActualLength; i++) {//循环每一个位置
            if ((mask & parentCuboidId) > 0) {// if the this bit position equals 1 ,说明父节点是有该列的位置的
                if ((mask & childCuboidId) > 0) {// if the child cuboid has this column,说明子节点是有该列的位置的
                    System.arraycopy(splitBuffers[index].value, 0, newKeyBodyBuf, offset, splitBuffers[index].length);//将value值写入到newKeyBodyBuf字节数组中
                    offset += splitBuffers[index].length;//追加字段值的长度
                }
                index++;
            }
            mask = mask >> 1;//依次切换一个位置
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();//字节长度
        while (newKeyBuf.array().length < fullKeySize) {//扩容
            newKeyBuf.set(new byte[newKeyBuf.length() * 2]);
        }
        newKeyBuf.set(0, fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, offset), newKeyBuf);//将编码到newKeyBuf中

        return fullKeySize;
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidId = rowKeySplitter.split(key.getBytes());//读取上一次的rowkey结果,拆分出cuboid
        Cuboid parentCuboid = Cuboid.findById(cubeDesc, cuboidId);//找到父对象

        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);//获取父对象下面所有的子节点

        // if still empty or null  没有子节点,因此跳过该记录
        if (myChildren == null || myChildren.size() == 0) {
            context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Skipped records").increment(1L);
            skipCounter++;
            if (skipCounter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Skipped " + skipCounter + " records!");
            }
            return;
        }

        context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Processed records").increment(1L);//处理多少条数据,即有子节点的数据

        handleCounter++;
        if (handleCounter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + handleCounter + " records!");
        }

        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findById(cubeDesc, child);//找到子节点cuboid
            int fullKeySize = buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());
            outputKey.set(newKeyBuf.array(), 0, fullKeySize);
            context.write(outputKey, value);//value不变化,因为度量肯定是不需要变化的,度量就是累加
        }

    }
}
