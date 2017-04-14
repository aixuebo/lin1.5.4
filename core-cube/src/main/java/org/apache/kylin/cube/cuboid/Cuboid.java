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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.AggregationGroup.HierarchyMask;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

public class Cuboid implements Comparable<Cuboid> {

    //key是cubeName,value是该cube对应的每一种Cuboid
    private final static Map<String, Map<Long, Cuboid>> CUBOID_CACHE = new ConcurrentHashMap<String, Map<Long, Cuboid>>();

    //smaller is better
    //先比较1的数量,即维度的数量,然后比较long的值
    public final static Comparator<Long> cuboidSelectComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return ComparisonChain.start().compare(Long.bitCount(o1), Long.bitCount(o2)).compare(o1, o2).result();
        }
    };

    public static Cuboid identifyCuboid(CubeDesc cubeDesc, Set<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc metric : metrics) {
            if (metric.getMeasureType().onlyAggrInBaseCuboid())
                return Cuboid.getBaseCuboid(cubeDesc);
        }

        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cubeDesc.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return Cuboid.findById(cubeDesc, cuboidID);
    }

    //字节数组转换成long
    public static Cuboid findById(CubeDesc cube, byte[] cuboidID) {
        return findById(cube, Bytes.toLong(cuboidID));
    }

    //获取对应的Cuboid对象
    public static Cuboid findById(CubeDesc cube, long cuboidID) {
        Map<Long, Cuboid> cubeCache = CUBOID_CACHE.get(cube.getName());
        if (cubeCache == null) {
            cubeCache = new ConcurrentHashMap<Long, Cuboid>();
            CUBOID_CACHE.put(cube.getName(), cubeCache);
        }
        Cuboid cuboid = cubeCache.get(cuboidID);
        if (cuboid == null) {
            long validCuboidID = translateToValidCuboid(cube, cuboidID);
            cuboid = new Cuboid(cube, cuboidID, validCuboidID);
            cubeCache.put(cuboidID, cuboid);
        }
        return cuboid;

    }

    //true表示有效---在任意一个AggregationGroup内有效,都是有效的
    public static boolean isValid(CubeDesc cube, long cuboidID) {
        if (cuboidID == getBaseCuboidId(cube)) {//basecubo肯定有效
            return true;
        }

        for (AggregationGroup agg : cube.getAggregationGroups()) {
            if (isValid(agg, cuboidID)) {
                return true;
            }
        }

        return false;
    }

    /**
     * true表示有效,即该AggregationGroup可以包含cuboidID需要的字段集合
     * 该cuboid在AggregationGroup组内是否有效
     */
    static boolean isValid(AggregationGroup agg, long cuboidID) {
        if (cuboidID <= 0) {
            return false; //cuboid must be greater than 0
        }

        /**
         * 1.agg.getPartialCubeFullMask()表示includes中字段
         * 2.~agg.getPartialCubeFullMask() 取反,表示非includes中字段
         * 3.cuboidID & 表示只要都是1的
         */
        if ((cuboidID & ~agg.getPartialCubeFullMask()) != 0) {//不等于0,说明AggregationGroup的includes中不能包含全部cuboidID需要的字段,比如cuboidID需要10个字段,但是其中2个字段不再AggregationGroup的includes中
            return false; //a cuboid's parent within agg is at most partialCubeFullMask
        }

        return checkMandatoryColumns(agg, cuboidID) && checkHierarchy(agg, cuboidID) && checkJoint(agg, cuboidID);
    }

    /**
     * 返回包含该cuboidID的字段的AggregationGroup集合
     */
    public static List<AggregationGroup> getValidAggGroupForCuboid(CubeDesc cubeDesc, long cuboidID) {
        List<AggregationGroup> ret = Lists.newArrayList();
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            if (isValid(agg, cuboidID)) {
                ret.add(agg);
            }
        }
        return ret;
    }

    //所有属性对应的cuboid
    public static long getBaseCuboidId(CubeDesc cube) {
        return cube.getRowkey().getFullMask();
    }

    //获取基础cube对应的Cuboid对象
    public static Cuboid getBaseCuboid(CubeDesc cube) {
        return findById(cube, getBaseCuboidId(cube));
    }

    public static long translateToValidCuboid(CubeDesc cubeDesc, long cuboidID) {
        long baseCuboidId = getBaseCuboidId(cubeDesc);
        if (cuboidID == baseCuboidId) {
            return cuboidID;
        }
        List<Long> candidates = Lists.newArrayList();
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            Long candidate = translateToValidCuboid(agg, cuboidID);
            if (candidate != null) {
                candidates.add(candidate);
            }
        }

        if (candidates.size() == 0) {
            return baseCuboidId;
        }

        return Collections.min(candidates, cuboidSelectComparator);
    }

    /**
     * 转换成有效的cuboidID,即追加必须的字段、继承字段、联合字段
     */
    private static Long translateToValidCuboid(AggregationGroup agg, long cuboidID) {
        /**
         * 1.agg.getPartialCubeFullMask()表示该agg需要哪些字段
         * 2.~ 表示该agg不需要哪些字段
         * 3.与cuboidID进行&操作,找到哪些字段不需要
         */
        if ((cuboidID & ~agg.getPartialCubeFullMask()) > 0) {//true说明cuboidID包含的字段范围已经超过了AggregationGroup控制的字段范围了
            //the partial cube might not contain all required dims
            return null;
        }

        // add mandantory
        cuboidID = cuboidID | agg.getMandatoryColumnMask();//其实没什么意义,不过表示的含义就是把必须的字段都追加上

        // add hierarchy  追加需要的继承字段
        for (HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {//循环每一个继承关系
            long fullMask = hierarchyMask.fullMask;//继承关系涉及到的字段集合
            long intersect = cuboidID & fullMask;//交集---即纯粹继承关系涉及到的字段集合
            if (intersect != 0 && intersect != fullMask) {//说明有交集,并且交集的内容是fullMask的子集

                boolean startToFill = false;//最开始是false
                for (int i = hierarchyMask.dims.length - 1; i >= 0; i--) {//循环继承关系的每一个字段
                    if (startToFill) {
                        cuboidID |= hierarchyMask.dims[i];//继续追加字段
                    } else {
                        if ((cuboidID & hierarchyMask.dims[i]) != 0) {//说明存在该字段-----必须从后向前,即最后的都有了,说明前面的顺序一定都得有
                            startToFill = true;
                            cuboidID |= hierarchyMask.dims[i];//追加该字段
                        }
                    }
                }
            }
        }

        // add joint dims  追加联合字段
        for (Long joint : agg.getJoints()) {
            /**
             * 三种关系  全包含  部分包含 不包含,只能全包含、不包含返回true
             * (cuboidID | joint) != cuboidID true 表示一定部分包含或者不包含
             * (cuboidID & ~joint) != cuboidID 表示全包含和部分包含
             * 因此两个&的结果表示是部分包含,既然是部分包含,则就要把其他元素也加入进来
             * 这个可以使用集合的交集图在纸上画,反映关系
             */
            if (((cuboidID | joint) != cuboidID) && ((cuboidID & ~joint) != cuboidID)) {
                cuboidID = cuboidID | joint;//追加联合字段
            }
        }

        if (isValid(agg, cuboidID)) {//说明有效
            return cuboidID;
        } else {
            // no column, add one column
            //agg.getPartialCubeFullMask() ^ agg.getMandatoryColumnMask() 表示 一个0和一个1对应的结果才是1,即剩余非必须的列集合
            //移除所有的join联合部分
            long nonJointDims = removeBits((agg.getPartialCubeFullMask() ^ agg.getMandatoryColumnMask()), agg.getJoints());
            if (nonJointDims != 0) {//说明还有内容
                //继续移除继承的部分
                long nonJointNonHierarchy = removeBits(nonJointDims, Collections2.transform(agg.getHierarchyMasks(), new Function<HierarchyMask, Long>() {//过滤每一个继承的全部列
                    @Override
                    public Long apply(HierarchyMask input) {
                        return input.fullMask;//获取继承的全部列
                    }
                }));
                if (nonJointNonHierarchy != 0) {
                    //there exists dim that does not belong to any joint or any hierarchy, that's perfect
                    return cuboidID | Long.lowestOneBit(nonJointNonHierarchy);//返回最低位置的1是哪个位置
                } else {
                    //choose from a hierarchy that does not intersect with any joint dim, only check level 1 
                    long allJointDims = agg.getJointDimsMask();
                    for (HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                        long dim = hierarchyMask.allMasks[0];
                        if ((dim & allJointDims) == 0) {
                            return cuboidID | dim;
                        }
                    }
                }
            }

            return cuboidID | Collections.min(agg.getJoints(), cuboidSelectComparator);
        }
    }

    /**
     * 从original中将toRemove中是1的位置都转换成0
     * @param original 原始二进制
     * @param toRemove 要移除1的位置的元素
     */
    private static long removeBits(long original, Collection<Long> toRemove) {
        long ret = original;
        for (Long joint : toRemove) {
            ret = ret & ~joint;
        }
        return ret;
    }

    /**
     * 对必须存在的列进行校验,
     * true 表示cuboidID一定包含必须的列
     */
    private static boolean checkMandatoryColumns(AggregationGroup agg, long cuboidID) {
        long mandatoryColumnMask = agg.getMandatoryColumnMask();//必须存在的列
        if ((cuboidID & mandatoryColumnMask) != mandatoryColumnMask) {//说明不相同,即不是必须存在的,则直接返回false
            return false;
        } else {
            //下面说明要么上面的是相同的,那么是cuboidID的1比mandatoryColumnMask多,即比必须的列要多,只是必须的列是0,因此结果依然是0
            //cuboid with only mandatory columns maybe valid
            //
            /**
             *(cuboidID & ~mandatoryColumnMask) != 0; true表示确实cuboidID的范围大于必须的列的范围
             *(cuboidID & ~mandatoryColumnMask)=0,说明完全相同,这是可能会被返回false的
             */
            return agg.isMandatoryOnlyValid() || (cuboidID & ~mandatoryColumnMask) != 0;
        }
    }

    /**
     * 校验联合属性集合
     * true表示校验成功----即cuboidID不存在任意一个联合属性,或者cuboidID包含所有联合属性集合的列
     */
    private static boolean checkJoint(AggregationGroup agg, long cuboidID) {
        for (long joint : agg.getJoints()) {//循环所有的联合属性集合
            long common = cuboidID & joint;//获取交集-----交集相同,说明cuboidID包含全部联合属性集合,交集=0,说明cuboidID与联合属性集合是没有交集的
            if (!(common == 0 || common == joint)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 校验继承关系
     * true表示校验成功
     */
    private static boolean checkHierarchy(AggregationGroup agg, long cuboidID) {
        List<HierarchyMask> hierarchyMaskList = agg.getHierarchyMasks();
        // if no hierarchy defined in metadata
        if (hierarchyMaskList == null || hierarchyMaskList.size() == 0) {//没有继承,则返回true
            return true;
        }

        hier: for (HierarchyMask hierarchyMasks : hierarchyMaskList) {//循环每一个继承关系
            long result = cuboidID & hierarchyMasks.fullMask;
            if (result > 0) {//说明属性有交集
                for (long mask : hierarchyMasks.allMasks) {//按照先后顺序一次循环
                    if (result == mask) {//说明满足继承关系
                        continue hier;//查看下一个继承关系
                    }
                }
                return false;//说明不满足继承关系
            }
        }
        return true;
    }

    // ============================================================================

    private CubeDesc cubeDesc;
    private final long inputID;//原始的cuboid
    private final long id;//将原始的cuboid转换成有效的cubeid
    private final byte[] idBytes;//id对应的字节数组
    private List<TblColRef> dimensionColumns;//该cubeid对应的所有的列集合
    private final boolean requirePostAggregation;

    private volatile CuboidToGridTableMapping cuboidToGridTableMapping = null;

    // will translate the cuboidID if it is not valid

    /**
     * @param cubeDesc 哪一个cube
     * @param originalID 原始的cuboid
     * @param validID 将原始的cuboid转换成有效的cubeid
     */
    private Cuboid(CubeDesc cubeDesc, long originalID, long validID) {
        this.cubeDesc = cubeDesc;
        this.inputID = originalID;
        this.id = validID;
        this.idBytes = Bytes.toBytes(id);
        this.dimensionColumns = translateIdToColumns(this.id);
        this.requirePostAggregation = calcExtraAggregation(this.inputID, this.id) != 0;
    }

    //找到所有的列集合
    private List<TblColRef> translateIdToColumns(long cuboidID) {
        List<TblColRef> dimesnions = new ArrayList<TblColRef>();
        RowKeyColDesc[] allColumns = cubeDesc.getRowkey().getRowKeyColumns();//所有列
        for (int i = 0; i < allColumns.length; i++) {
            // NOTE: the order of column in list!!!
            long bitmask = 1L << allColumns[i].getBitIndex();//每一个列的二进制
            if ((cuboidID & bitmask) != 0) {//说明有该列
                TblColRef colRef = allColumns[i].getColRef();
                dimesnions.add(colRef);
            }
        }
        return dimesnions;
    }

    private long calcExtraAggregation(long inputID, long id) {
        long diff = id ^ inputID;
        return eliminateHierarchyAggregation(diff);
    }

    // higher level in hierarchy can be ignored when counting aggregation columns
    private long eliminateHierarchyAggregation(long id) {
        long finalId = id;

        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            long temp = id;
            List<HierarchyMask> hierarchyMaskList = agg.getHierarchyMasks();
            if (hierarchyMaskList != null && hierarchyMaskList.size() > 0) {
                for (HierarchyMask hierMask : hierarchyMaskList) {
                    long[] allMasks = hierMask.allMasks;
                    for (int i = allMasks.length - 1; i > 0; i--) {
                        long bit = allMasks[i] ^ allMasks[i - 1];
                        if ((inputID & bit) != 0) {
                            temp &= ~allMasks[i - 1];
                            if (temp < finalId)
                                finalId = temp;
                        }
                    }
                }
            }
        }
        return finalId;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public List<TblColRef> getColumns() {
        return dimensionColumns;
    }

    public List<TblColRef> getAggregationColumns() {
        long aggrColsID = eliminateHierarchyAggregation(id);
        return translateIdToColumns(aggrColsID);
    }

    public long getId() {
        return id;
    }

    public byte[] getBytes() {
        return idBytes;
    }

    public long getInputID() {
        return inputID;
    }

    public boolean requirePostAggregation() {
        return requirePostAggregation;
    }

    public static void clearCache() {
        CUBOID_CACHE.clear();
    }

    public static void reloadCache(String cubeDescName) {
        CUBOID_CACHE.remove(cubeDescName);
    }

    @Override
    public String toString() {
        return "Cuboid [id=" + id + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Cuboid other = (Cuboid) obj;
        if (id != other.id)
            return false;
        return true;
    }

    @Override
    public int compareTo(Cuboid o) {
        if (this.id < o.id) {
            return -1;
        } else if (this.id > o.id) {
            return 1;
        } else {
            return 0;
        }
    }

    public CuboidToGridTableMapping getCuboidToGridTableMapping() {
        if (cuboidToGridTableMapping == null) {
            cuboidToGridTableMapping = new CuboidToGridTableMapping(this);
        }
        return cuboidToGridTableMapping;
    }

    public static String getDisplayName(long cuboidID, int dimensionCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensionCount; ++i) {
            if ((cuboidID & (1L << i)) == 0) {
                sb.append('0');
            } else {
                sb.append('1');
            }
        }
        return StringUtils.reverse(sb.toString());
    }
}
