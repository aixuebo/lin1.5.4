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

package org.apache.kylin.cube.cuboid;

/** 
 */

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CuboidScheduler {

    private final CubeDesc cubeDesc;
    private final long max;//最多多少个cuboid节点
    private final Map<Long, List<Long>> cache;//记录每一个cuboid下有哪些子节点集合的映射

    public CuboidScheduler(CubeDesc cubeDesc) {
        this.cubeDesc = cubeDesc;
        int size = this.cubeDesc.getRowkey().getRowKeyColumns().length;
        this.max = (long) Math.pow(2, size) - 1;
        this.cache = new ConcurrentHashMap<Long, List<Long>>();
    }

    /**
     * 给定一个子节点,返回所对应的父节点
     */
    public long getParent(long child) {
        List<Long> candidates = Lists.newArrayList();
        long baseCuboidID = Cuboid.getBaseCuboidId(cubeDesc);
        if (child == baseCuboidID || !Cuboid.isValid(cubeDesc, child)) {
            throw new IllegalStateException();
        }
        for (AggregationGroup agg : Cuboid.getValidAggGroupForCuboid(cubeDesc, child)) {
            boolean thisAggContributed = false;
            if (agg.getPartialCubeFullMask() == child) {
                //                candidates.add(baseCuboidID);
                //                continue;
                return baseCuboidID;

            }

            //+1 dim

            //add one normal dim (only try the lowest dim)
            long normalDimsMask = (agg.getNormalDimsMask() & ~child);
            if (normalDimsMask != 0) {
                candidates.add(child | Long.lowestOneBit(normalDimsMask));
                thisAggContributed = true;
            }

            for (AggregationGroup.HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                if ((child & hierarchyMask.fullMask) == 0) {
                    candidates.add(child | hierarchyMask.dims[0]);
                    thisAggContributed = true;
                } else {
                    for (int i = hierarchyMask.allMasks.length - 1; i >= 0; i--) {
                        if ((child & hierarchyMask.allMasks[i]) == hierarchyMask.allMasks[i]) {
                            if (i == hierarchyMask.allMasks.length - 1) {
                                continue;//match the full hierarchy
                            }
                            if ((agg.getJointDimsMask() & hierarchyMask.dims[i + 1]) == 0) {
                                if ((child & hierarchyMask.dims[i + 1]) == 0) {
                                    //only when the hierarchy dim is not among joints
                                    candidates.add(child | hierarchyMask.dims[i + 1]);
                                    thisAggContributed = true;
                                }
                            }
                            break;//if hierarchyMask 111 is matched, won't check 110 or 100
                        }
                    }
                }
            }

            if (thisAggContributed) {
                //next section is going to append more than 2 dim to child
                //thisAggContributed means there's already 1 dim added to child
                //which can safely prune the 2+ dim candidates.
                continue;
            }

            //2+ dim candidates
            for (long joint : agg.getJoints()) {
                if ((child & joint) == 0) {
                    candidates.add(child | joint);
                }
            }
        }

        if (candidates.size() == 0) {
            throw new IllegalStateException();
        }

        return Collections.min(candidates, Cuboid.cuboidSelectComparator);
    }

    /**
     * 给定一个父节点----得到潜在的子节点集合
     */
    public Set<Long> getPotentialChildren(long parent) {

        if (parent != Cuboid.getBaseCuboid(cubeDesc).getId() && !Cuboid.isValid(cubeDesc, parent)) {
            throw new IllegalStateException();
        }

        HashSet<Long> set = Sets.newHashSet();
        if (Long.bitCount(parent) == 1) {//1个维度的时候已经不需要cubo了,因此是一个空的子集合
            //do not aggregate apex cuboid
            return set;
        }

        /**
         * 添加每一个AggregationGroup的全量include字段集合
         */
        if (parent == Cuboid.getBaseCuboidId(cubeDesc)) {//父节点是baseCoboid
            //base cuboid is responsible for spawning each agg group's root
            for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
                long partialCubeFullMask = agg.getPartialCubeFullMask();//include字段集合
                if (partialCubeFullMask != parent && Cuboid.isValid(agg, partialCubeFullMask)) {
                    set.add(partialCubeFullMask);
                }
            }
        }

        for (AggregationGroup agg : Cuboid.getValidAggGroupForCuboid(cubeDesc, parent)) {//父是有效的------返回包含该cuboidID的字段的AggregationGroup集合

            //normal dim section
            for (long normalDimMask : agg.getNormalDims()) {//循环正常的字段
                long common = parent & normalDimMask;//此时只有一个1
                long temp = parent ^ normalDimMask;//因为normalDimMask就一个位置是1,剩下的都是0 ,因此是1的依然是1,是0的依然是0,不会变化,仅仅normalDimMask位置会变化如果是1则是0,如果是0,则是1-----即只是将该正常字段位置变化一下,其余不变
                //common != 0 说明存在该字段,因此校验如果该位置为0的时候,是否合法,合法就加入到集合中
                if (common != 0 && Cuboid.isValid(agg, temp)) {
                    set.add(temp);
                }
            }

            //处理继承关系
            for (AggregationGroup.HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                for (int i = hierarchyMask.allMasks.length - 1; i >= 0; i--) {//循环表示继承关系中的顺序,是先谁后谁
                    if ((parent & hierarchyMask.allMasks[i]) == hierarchyMask.allMasks[i]) {//说明交集后依然是本身,即存在该继承关系
                        if ((agg.getJointDimsMask() & hierarchyMask.dims[i]) == 0) {//说明该字段不再joint中存在
                            if (Cuboid.isValid(agg, parent ^ hierarchyMask.dims[i])) {//校验反转该位置1变成0是否有效
                                //only when the hierarchy dim is not among joints
                                set.add(parent ^ hierarchyMask.dims[i]);//将该位置设置成0,将其加入到集合中
                            }
                        }
                        break;//if hierarchyMask 111 is matched, won't check 110 or 100
                    }
                }
            }

            //joint dim section
            for (long joint : agg.getJoints()) {
                if ((parent & joint) == joint) {//说明包含该joint集合所需要的字段
                    if (Cuboid.isValid(agg, parent ^ joint)) {//将这部分字段都去反,校验有效性
                        set.add(parent ^ joint);
                    }
                }
            }
        }

        return set;
    }


    //递归计算cuboidId节点下子子孙孙有多少个子节点
    public int getCuboidCount() {
        return getCuboidCount(Cuboid.getBaseCuboidId(cubeDesc));
    }

    //递归计算cuboidId节点下子子孙孙有多少个子节点
    private int getCuboidCount(long cuboidId) {
        int r = 1;
        for (Long child : getSpanningCuboid(cuboidId)) {
            r += getCuboidCount(child);
        }
        return r;
    }

    //计算cuboidId节点下有多少个子节点
    public List<Long> getSpanningCuboid(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboid + " is out of scope 0-" + max);
        }

        List<Long> result = cache.get(cuboid);
        if (result != null) {
            return result;
        }

        result = Lists.newArrayList();
        Set<Long> potentials = getPotentialChildren(cuboid);
        for (Long potential : potentials) {
            if (getParent(potential) == cuboid) {
                result.add(potential);
            }
        }

        cache.put(cuboid, result);
        return result;
    }

    //查看该维度组合下有多少个维度被查
    public int getCardinality(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cubiod " + cuboid + " is out of scope 0-" + max);
        }

        return Long.bitCount(cuboid);
    }

    //计算一共有多少个子节点,返回子节点集合
    public List<Long> getAllCuboidIds() {
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);//base cuboid
        List<Long> result = Lists.newArrayList();//返回子节点集合
        getSubCuboidIds(baseCuboidId, result);
        return result;
    }

    //计算parentCuboidId下面一共有多少个子节点,子节点集合存储在参数result中
    private void getSubCuboidIds(long parentCuboidId, List<Long> result) {
        result.add(parentCuboidId);
        for (Long cuboidId : getSpanningCuboid(parentCuboidId)) {
            getSubCuboidIds(cuboidId, result);
        }
    }
}
