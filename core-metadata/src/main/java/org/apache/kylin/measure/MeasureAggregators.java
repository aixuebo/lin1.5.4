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

package org.apache.kylin.measure;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * 存储一组度量集合
 */
@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public class MeasureAggregators implements Serializable {

    private final MeasureAggregator[] aggs;//度量聚合的集合
    private final int descLength;//有多少个度量聚合

    public MeasureAggregators(MeasureAggregator... aggs) {
        this.descLength = aggs.length;
        this.aggs = aggs;
    }

    public MeasureAggregators(Collection<MeasureDesc> measureDescs) {
        this((MeasureDesc[]) measureDescs.toArray(new MeasureDesc[measureDescs.size()]));
    }

    public MeasureAggregators(MeasureDesc... measureDescs) {
        descLength = measureDescs.length;
        aggs = new MeasureAggregator[descLength];

        //key是函数name,value是函数在集合中的下标
        Map<String, Integer> measureIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < descLength; i++) {
            FunctionDesc func = measureDescs[i].getFunction();//具体的函数
            aggs[i] = func.getMeasureType().newAggregator();//该函数对应的聚合对象
            measureIndexMap.put(measureDescs[i].getName(), i);
        }
        // fill back dependent aggregator
        for (int i = 0; i < descLength; i++) {
            String depMsrRef = measureDescs[i].getDependentMeasureRef();//说明有依赖关系
            if (depMsrRef != null) {
                int index = measureIndexMap.get(depMsrRef);//依赖哪一个聚合
                aggs[i].setDependentAggregator(aggs[index]);
            }
        }
    }

    public void reset() {
        for (int i = 0; i < aggs.length; i++) {
            aggs[i].reset();
        }
    }

    //对一组数据进行聚合
    public void aggregate(Object[] values) {
        assert values.length == descLength;

        for (int i = 0; i < descLength; i++) {
            aggs[i].aggregate(values[i]);
        }
    }

    //对aggrMask为true的聚合函数才参与聚合
    public void aggregate(Object[] values, boolean[] aggrMask) {
        assert values.length == descLength;
        assert aggrMask.length == descLength;

        for (int i = 0; i < descLength; i++) {
            if (aggrMask[i])
                aggs[i].aggregate(values[i]);
        }
    }

    //返回每一个聚合函数的结果
    public void collectStates(Object[] states) {
        for (int i = 0; i < descLength; i++) {
            states[i] = aggs[i].getState();
        }
    }

}
