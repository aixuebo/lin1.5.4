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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureDecoder;

import com.google.common.collect.Lists;

/**
 * @author George Song (ysong1)
 * 读取rowkey--所有度量的值作为输入源
 * 输出到hbase中,rowkey不变,只是将所有的度量值,拆分成若干个列族,存放到不同的列里面
 */
public class CubeHFileMapper extends KylinMapper<Text, Text, ImmutableBytesWritable, KeyValue> {

    ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

    String cubeName;
    CubeDesc cubeDesc;

    MeasureDecoder inputCodec;//度量的解码器
    Object[] inputMeasures;//调用解码器后,每一个度量返回一个反序列化后的值,每一个值都是数组的一个元素

    List<KeyValueCreator> keyValueCreators;//每一个列族对应一个该对象----因为每一个列族是分配了不同的度量值的

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(config);
        cubeDesc = cubeMgr.getCube(cubeName).getDescriptor();

        inputCodec = new MeasureDecoder(cubeDesc.getMeasures());
        inputMeasures = new Object[cubeDesc.getMeasures().size()];
        keyValueCreators = Lists.newArrayList();

        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
    }

    /**
     *
     * @param key  rowkey
     * @param value 所有的度量集合
     */
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        outputKey.set(key.getBytes(), 0, key.getLength());//rowkey
        KeyValue outputValue;

        int n = keyValueCreators.size();
        if (n == 1 && keyValueCreators.get(0).isFullCopy) { // shortcut for simple full copy

            outputValue = keyValueCreators.get(0).create(key, value.getBytes(), 0, value.getLength());
            context.write(outputKey, outputValue);

        } else { // normal (complex) case that distributes measures to multiple HBase columns

            inputCodec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), inputMeasures);//对度量的结果反序列化---存储到inputMeasures中

            for (int i = 0; i < n; i++) {
                outputValue = keyValueCreators.get(i).create(key, inputMeasures);
                context.write(outputKey, outputValue);//用于存储 属于该列族下的某一列对应的具体度量值,因为输出文件是hbase的输出文件,因此这种方式符合hbase文件格式
            }
        }
    }

}
