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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class InMemCuboidReducer extends KylinReducer<ByteArrayWritable, ByteArrayWritable, Object, Object> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private BufferedMeasureEncoder codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    private Text outputKey;
    private Text outputValue;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();

        List<MeasureDesc> measuresDescs = cubeDesc.getMeasures();
        codec = new BufferedMeasureEncoder(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);
        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];

        outputKey = new Text();
        outputValue = new Text();
    }

    @Override
    public void reduce(ByteArrayWritable key, Iterable<ByteArrayWritable> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (ByteArrayWritable value : values) {//对相同的数据进行聚类
            codec.decode(value.asBuffer(), input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);//收集聚类后的结果

        // output key
        outputKey.set(key.array(), key.offset(), key.length());

        // output value
        ByteBuffer valueBuf = codec.encode(result);//对最终聚合后的结果进行buffer处理
        outputValue.set(valueBuf.array(), 0, valueBuf.position());

        context.write(outputKey, outputValue);

        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

}
