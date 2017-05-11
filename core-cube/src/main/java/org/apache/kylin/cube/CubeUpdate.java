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

package org.apache.kylin.cube;

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

/**
 * Hold changes to a cube so that they can be applied as one unit.
 * 此时要更新cube,因此该类表示cube要更改的信息
 */
public class CubeUpdate {
    private CubeInstance cubeInstance;//要更新的cube实例对象
    private CubeSegment[] toAddSegs = null;//增加的segment
    private CubeSegment[] toRemoveSegs = null;//要删除的segment集合
    private CubeSegment[] toUpdateSegs = null;//更新的segment
    private RealizationStatusEnum status;//要更改的cube状态
    private String owner;
    private int cost = -1;//更新cube的cost代价

    public CubeUpdate(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public CubeUpdate setCubeInstance(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
        return this;
    }

    public CubeSegment[] getToAddSegs() {
        return toAddSegs;
    }

    public CubeUpdate setToAddSegs(CubeSegment... toAddSegs) {
        this.toAddSegs = toAddSegs;
        return this;
    }

    public CubeSegment[] getToRemoveSegs() {
        return toRemoveSegs;
    }

    public CubeUpdate setToRemoveSegs(CubeSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public CubeSegment[] getToUpdateSegs() {
        return toUpdateSegs;
    }

    public CubeUpdate setToUpdateSegs(CubeSegment... toUpdateSegs) {
        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public CubeUpdate setStatus(RealizationStatusEnum status) {
        this.status = status;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public CubeUpdate setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public int getCost() {
        return cost;
    }

    public CubeUpdate setCost(int cost) {
        this.cost = cost;
        return this;
    }
}
