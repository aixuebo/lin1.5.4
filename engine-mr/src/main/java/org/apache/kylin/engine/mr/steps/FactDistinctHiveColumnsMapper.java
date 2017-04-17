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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * 1.用于将rowkey中每一个字典列需要的字段值收集起来,一个reduce一个字段的不重复的字典值
 * 2.统计rowkey的各种组合情况,即每一个cuboid有多少个不同的元素
 */
public class FactDistinctHiveColumnsMapper<KEYIN> extends FactDistinctColumnsMapperBase<KEYIN, Object> {

    protected boolean collectStatistics = false;//是否进行统计
    private int samplingPercentage;//统计抽样的百分比
    private int rowCount = 0;//该map处理的记录数,每隔100清空一次


    protected CuboidScheduler cuboidScheduler = null;
    protected int nRowKey;//rowkey的长度,即需要多少个字段作为rowkey

    private Long[] cuboidIds;//所有的cuboid集合
    private Integer[][] allCuboidsBitSet = null;//每一个元素对应的集合,比如cudoid有3个1,则该子节点数组的长度就是3,分别表示哪三个rowkey的位置是1

    private HyperLogLogPlusCounter[] allCuboidsHLL = null;//每一个cuboid节点对应一个HyperLogLogPlusCounter对象,用于统计该cuboid有多少个不同的rowkey的hash值,不优化的话有2^n次方个cuboid节点

    private HashFunction hf = null; //hash函数

    private ByteArray[] row_hashcodes = null;//每一个列对应一个字节数组,存储该列对应的值的hash后的字节数组
    private ByteBuffer keyBuffer;//存储key的缓冲池

    private static final Text EMPTY_TEXT = new Text();//空值
    public static final byte MARK_FOR_HLL = (byte) 0xFF;//11111111 表示一个字节的位

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        keyBuffer = ByteBuffer.allocate(4096);
        collectStatistics = Boolean.parseBoolean(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_ENABLED));//是否进行统计
        if (collectStatistics) {
            samplingPercentage = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));
            cuboidScheduler = new CuboidScheduler(cubeDesc);
            nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

            List<Long> cuboidIdList = Lists.newArrayList();//存储每一个cuboid
            List<Integer[]> allCuboidsBitSetList = Lists.newArrayList();//每一个元素是一个集合,该集合表示该cuboid中是哪几个元素是1
            addCuboidBitSet(baseCuboidId, allCuboidsBitSetList, cuboidIdList);

            allCuboidsBitSet = allCuboidsBitSetList.toArray(new Integer[cuboidIdList.size()][]);
            cuboidIds = cuboidIdList.toArray(new Long[cuboidIdList.size()]);

            allCuboidsHLL = new HyperLogLogPlusCounter[cuboidIds.length];
            for (int i = 0; i < cuboidIds.length; i++) {
                allCuboidsHLL[i] = new HyperLogLogPlusCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision());
            }

            hf = Hashing.murmur3_32(); //初始化hash算法

            //rowkey中每一个列对应一个字节数组
            row_hashcodes = new ByteArray[nRowKey];
            for (int i = 0; i < nRowKey; i++) {
                row_hashcodes[i] = new ByteArray();
            }
        }
    }

    //从baseCubo开始,不断递归,寻找所有的cuboidId集合
    private void addCuboidBitSet(long cuboidId, List<Integer[]> allCuboidsBitSet, List<Long> allCuboids) {

        allCuboids.add(cuboidId);

        //表示第几个字段是1
        Integer[] indice = new Integer[Long.bitCount(cuboidId)];//Long.bitCount(cuboidId) 表示有多少个位置是1

        long mask = Long.highestOneBit(baseCuboidId);//最高位1,剩下的都是0,此时对应的是什么
        int position = 0;
        for (int i = 0; i < nRowKey; i++) {//循环每一个字段
            if ((mask & cuboidId) > 0) {//判断最高位是否是1
                indice[position] = i;
                position++;
            }
            mask = mask >> 1;//每一次都是少1位,比如以前是11位,下一次就是10位,并且依然是最高位是1,剩下的都是0
        }

        allCuboidsBitSet.add(indice);//添加子节点集合
        Collection<Long> children = cuboidScheduler.getSpanningCuboid(cuboidId);
        for (Long childId : children) {
            addCuboidBitSet(childId, allCuboidsBitSet, allCuboids);
        }
    }

    //将每一个列的index序号和列对应的内容作为输出的key
    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        String[] row = flatTableInputFormat.parseMapperInput(record);//读取每一行数据,转换成列集合
        try {
            for (int i = 0; i < factDictCols.size(); i++) {//循环fact表中字段集合
                String fieldValue = row[dictionaryColumnIndex[i]];//fact表中列所在的index索引---获取该fact表中对应的字段值
                if (fieldValue == null)
                    continue;

                keyBuffer.clear();
                //存储第几列,以及对应的值
                keyBuffer.put(Bytes.toBytes(i)[3]); // one byte is enough  将int序号转换成字节数组,但是因为序号很少,一个字节就足够了,因此只获取第一个字节,即只存储了一个字节
                keyBuffer.put(Bytes.toBytes(fieldValue));//存储字节数组内容
                outputKey.set(keyBuffer.array(), 0, keyBuffer.position());
                context.write(outputKey, EMPTY_TEXT);
            }
        } catch (Exception ex) {
            handleErrorRecord(row, ex);
        }

        if (collectStatistics && rowCount < samplingPercentage) {//抽样百分比,比如是5,则每100条数据,都要前5条数据
            putRowKeyToHLL(row);
        }

        if (rowCount++ == 100)
            rowCount = 0;
    }

    private void putRowKeyToHLL(String[] row) {

        //generate hash for each row key column
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[intermediateTableDesc.getRowKeyColumnIndexes()[i]];//选择该列的值
            if (colValue != null) {
                row_hashcodes[i].set(hc.putString(colValue).hash().asBytes());//将该列对应的值进行hash,然后hash后的字节数组存储起来
            } else {
                row_hashcodes[i].set(hc.putInt(0).hash().asBytes());//列对应的值是null的时候,存储0的hash即可
            }
        }

        // user the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {//循环有多少个cuboid节点
            Hasher hc = hf.newHasher();//每一个节点产生一个新的hash算法
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {//计算该cuboid下面有哪些维度是1
                /**
                 * allCuboidsBitSet[i]表示获取该cuboid
                 * allCuboidsBitSet[i][position] 表示获取该cuboid在rowkey上准备设置列值作为维度的列号,即如果存在3个列,则position范围是0 1 2,对应的值是每一个字段是在rowkey上第几个字段-----即该方法返回第几个字段
                 * row_hashcodes[allCuboidsBitSet[i][position] 表示去获取该rowkey上的字段对应的具体值
                 * hc.putBytes 表示将若个个列的具体值存储到字节数组中
                 */
                hc.putBytes(row_hashcodes[allCuboidsBitSet[i][position]].array());
            }

            allCuboidsHLL[i].add(hc.hash().asBytes());//组装好的rowkey对应的hash值,不同的hash值,说明rowkey的内容是不同的
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (collectStatistics) {//需要统计
            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);
            // output each cuboid's hll to reducer, key is 0 - cuboidId
            HyperLogLogPlusCounter hll;
            for (int i = 0; i < cuboidIds.length; i++) {//循环每一个cuboid点
                hll = allCuboidsHLL[i];//每一个cuboid对应一个统计点,统计该cuboid有多少个不同的rowkey的hash值

                keyBuffer.clear();
                keyBuffer.put(MARK_FOR_HLL); // one byte 插入一个字节11111111 表示是一个cuboid的头
                keyBuffer.putLong(cuboidIds[i]);//插入cuboid
                outputKey.set(keyBuffer.array(), 0, keyBuffer.position());//cuboid头+cuboid内容---作为key

                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                outputValue.set(hllBuf.array(), 0, hllBuf.position());//cuboid对应的hash统计内容---作为value
                context.write(outputKey, outputValue);
            }
        }
    }
}
