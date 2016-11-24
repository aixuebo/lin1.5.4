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

package org.apache.kylin.source.hive.cardinality;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;

/**
 * @author Jack
 * 对hive的table表中每一个列进行估算distinct(value),
 * 输入列序号,统计值
 * 输出每一个列序号作为key,value是该列distinct(value)的估值
 */
public class ColumnCardinalityReducer extends KylinReducer<IntWritable, BytesWritable, IntWritable, LongWritable> {

    public static final int ONE = 1;

    //key是第几个列,value是该列对应的统计对象,该对象是估算对象
    private Map<Integer, HyperLogLogPlusCounter> hllcMap = new HashMap<Integer, HyperLogLogPlusCounter>();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
    }

    @Override
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        int skey = key.get();//获取该列信息
        for (BytesWritable v : values) {//每一个map对该列的统计结果
            ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());//统计结果转换成HyperLogLogPlusCounter对象
            HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter();
            hll.readRegisters(buffer);
            getHllc(skey).merge(hll);//合并统计内容
            hll.clear();
        }
    }

    private HyperLogLogPlusCounter getHllc(Integer key) {
        if (!hllcMap.containsKey(key)) {
            hllcMap.put(key, new HyperLogLogPlusCounter());
        }
        return hllcMap.get(key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Integer> keys = new ArrayList<Integer>();//所有的列序号集合
        Iterator<Integer> it = hllcMap.keySet().iterator();
        while (it.hasNext()) {
            keys.add(it.next());
        }
        Collections.sort(keys);//对列序号排序
        it = keys.iterator();
        while (it.hasNext()) {
            int key = it.next();
            HyperLogLogPlusCounter hllc = hllcMap.get(key);
            ByteBuffer buf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);
            buf.clear();
            hllc.writeRegisters(buf);
            buf.flip();
            context.write(new IntWritable(key), new LongWritable(hllc.getCountEstimate()));//输出每一个列序号作为key,value是该列distinct(value)的估值
            // context.write(new Text("ErrorRate_" + key), new
            // LongWritable((long)hllc.getErrorRate()));
        }

    }
}
