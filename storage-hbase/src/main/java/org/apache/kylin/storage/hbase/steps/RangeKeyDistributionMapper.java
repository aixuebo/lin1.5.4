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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.engine.mr.KylinMapper;

/**
 * @author ysong1
 * 每个1M字节的时候统计一下rowkey以及对应的字节数
 * 用于对所有的rowkey进行划分范围
 */
public class RangeKeyDistributionMapper extends KylinMapper<Text, Text, Text, LongWritable> {

    private static final long ONE_MEGA_BYTES = 1L * 1024L * 1024L;//1M

    private LongWritable outputValue = new LongWritable(0);

    private long bytesRead = 0;//已经读取了多少个字节

    private Text lastKey;//最后一个key

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        lastKey = key;

        int bytesLength = key.getLength() + value.getLength();//该记录占用多少字节
        bytesRead += bytesLength;//累加读取的字节数

        if (bytesRead >= ONE_MEGA_BYTES) {//超过伐值
            outputValue.set(bytesRead);
            context.write(key, outputValue);//设置准确的字节数

            // reset bytesRead
            bytesRead = 0;//归0
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (lastKey != null) {
            outputValue.set(bytesRead);
            context.write(lastKey, outputValue);//输出最后一个key对应的字节数
        }
    }

}
