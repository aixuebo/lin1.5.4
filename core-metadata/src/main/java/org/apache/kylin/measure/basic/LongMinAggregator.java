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

package org.apache.kylin.measure.basic;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.LongMutable;

/**
 * min算法
 */
@SuppressWarnings("serial")
public class LongMinAggregator extends MeasureAggregator<LongMutable> {

    LongMutable min = null;

    @Override
    public void reset() {
        min = null;
    }

    @Override
    public void aggregate(LongMutable value) {
        if (min == null)
            min = new LongMutable(value.get());
        else if (min.get() > value.get())
            min.set(value.get());
    }

    @Override
    public LongMutable getState() {
        return min;
    }

    @Override
    public int getMemBytesEstimate() {
        return guessLongMemBytes();
    }

}
