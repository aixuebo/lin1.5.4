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

package org.apache.kylin.cube.kv;

import java.util.HashMap;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;

import com.google.common.collect.Maps;

//一个cube对应的编码器的映射---每一个cuboid对应一个rowkey编码器
public class RowKeyEncoderProvider {

    private CubeSegment cubeSegment;
    private HashMap<Long, RowKeyEncoder> rowKeyEncoders;//不同的cuboid对应不同的编码器---因为RowKeyEncoder解码器里面会存储cuboid,因此必须一个cuboid对应一个单独的解码器RowKeyEncoder

    public RowKeyEncoderProvider(CubeSegment cubeSegment) {
        this.cubeSegment = cubeSegment;
        this.rowKeyEncoders = Maps.newHashMap();
    }

    //获取一个编码器
    public RowKeyEncoder getRowkeyEncoder(Cuboid cuboid) {
        RowKeyEncoder rowKeyEncoder = rowKeyEncoders.get(cuboid.getId());
        if (rowKeyEncoder == null) {
            rowKeyEncoder = new RowKeyEncoder(cubeSegment, cuboid);
            rowKeyEncoders.put(cuboid.getId(), rowKeyEncoder);
        }
        return rowKeyEncoder;
    }
}
