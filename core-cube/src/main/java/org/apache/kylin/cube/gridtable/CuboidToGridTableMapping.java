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

package org.apache.kylin.cube.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * org.apache.kylin.cube.gridtable.CuboidToGridTableMapping 表示一个cuboid如何存储数据
 */
public class CuboidToGridTableMapping {

    final private Cuboid cuboid;

    private List<DataType> gtDataTypes;//每一个列的数据类型以及度量的返回值
    private List<ImmutableBitSet> gtColBlocks;//第一个集合是存储rowkey需要了哪些列,后面的集合是说每一个列族-列 对应一个ImmutableBitSet,表示该列族-列使用了哪些序号的函数

    private int nDimensions;//rowkey需要多少列,即多少维
    private Map<TblColRef, Integer> dim2gt;//每一个维度列与该列对应的序号映射
    private ImmutableBitSet gtPrimaryKey;//哪些列是rowkey对应的主键

    private int nMetrics;//表示有多少个度量
    private Map<FunctionDesc, Integer> metrics2gt; // because count distinct may have a holistic version 每一个度量函数与对应的序号映射

    public CuboidToGridTableMapping(Cuboid cuboid) {
        this.cuboid = cuboid;
        init();
    }

    //初始化操作
    private void init() {
        int gtColIdx = 0;//列序号
        gtDataTypes = Lists.newArrayList();
        gtColBlocks = Lists.newArrayList();

        // dimensions
        dim2gt = Maps.newHashMap();
        BitSet pk = new BitSet();//哪些列是存在的
        for (TblColRef dimension : cuboid.getColumns()) {//循环所有的列
            gtDataTypes.add(dimension.getType());
            dim2gt.put(dimension, gtColIdx);
            pk.set(gtColIdx);//存储该列序号存在
            gtColIdx++;
        }

        gtPrimaryKey = new ImmutableBitSet(pk);//设置rowkey存在的主键
        gtColBlocks.add(gtPrimaryKey);

        nDimensions = gtColIdx;
        assert nDimensions == cuboid.getColumns().size();

        // column blocks of metrics 多少个列族--列 对应多少个该对象
        ArrayList<BitSet> metricsColBlocks = Lists.newArrayList();
        for (HBaseColumnFamilyDesc familyDesc : cuboid.getCubeDesc().getHbaseMapping().getColumnFamily()) {//所有的hbase的列族
            for (int i = 0; i < familyDesc.getColumns().length; i++) {//每一个列族下的列集合
                metricsColBlocks.add(new BitSet());
            }
        }

        // metrics
        metrics2gt = Maps.newHashMap();
        for (MeasureDesc measure : cuboid.getCubeDesc().getMeasures()) {//循环所有的度量
            // Count distinct & holistic count distinct are equals() but different.
            // Ensure the holistic version if exists is always the first.
            FunctionDesc func = measure.getFunction();
            metrics2gt.put(func, gtColIdx);
            gtDataTypes.add(func.getReturnDataType());

            // map to column block
            int cbIdx = 0;
            for (HBaseColumnFamilyDesc familyDesc : cuboid.getCubeDesc().getHbaseMapping().getColumnFamily()) {
                for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                    if (hbaseColDesc.containsMeasure(measure.getName())) {//找到该度量函数所属列族--列
                        metricsColBlocks.get(cbIdx).set(gtColIdx);//说明该列族--列 使用了第几个度量函数的序号
                    }
                    cbIdx++;
                }
            }

            gtColIdx++;
        }

        for (BitSet set : metricsColBlocks) {
            gtColBlocks.add(new ImmutableBitSet(set));
        }

        nMetrics = gtColIdx - nDimensions;
        assert nMetrics == cuboid.getCubeDesc().getMeasures().size();
    }

    //维度+度量一共多少列
    public int getColumnCount() {
        return nDimensions + nMetrics;
    }

    //维度+度量对应每一个的类型,度量的类型是度量函数的返回值的类型
    public DataType[] getDataTypes() {
        return gtDataTypes.toArray(new DataType[gtDataTypes.size()]);
    }

    //哪些列是rowkey需要的列
    public ImmutableBitSet getPrimaryKey() {
        return gtPrimaryKey;
    }

    //返回rowkey和每一个列族+列对应的ImmutableBitSet对象,表示需要到了哪些列
    public ImmutableBitSet[] getColumnBlocks() {
        return gtColBlocks.toArray(new ImmutableBitSet[gtColBlocks.size()]);
    }

    //返回该维度列 对应的维度序号
    public int getIndexOf(TblColRef dimension) {
        Integer i = dim2gt.get(dimension);
        return i == null ? -1 : i.intValue();
    }

    //返回该度量函数对应的序号
    public int getIndexOf(FunctionDesc metric) {
        Integer r = metrics2gt.get(metric);
        return r == null ? -1 : r;
    }

    //返回所有的度量列集合
    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        return cuboid.getColumns();
    }

    //按照rowkey的列顺序,返回每一个列对应的编码方式
    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        List<TblColRef> dims = cuboid.getColumns();//所有的度量列集合
        DimensionEncoding[] dimEncs = new DimensionEncoding[dims.size()];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dims.get(i));//设置每一个列对应的编码方式
        }
        return dimEncs;
    }

    /**
     * 返回度量中如果有依赖关系时,依赖的关系
     *
     * @return Map<Integer, Integer> key和value都是度量函数的序号,key是函数本身,value是该函数依赖哪一个函数
     */
    public Map<Integer, Integer> getDependentMetricsMap() {
        Map<Integer, Integer> result = Maps.newHashMap();
        List<MeasureDesc> measures = cuboid.getCubeDesc().getMeasures();//所有的度量集合
        for (MeasureDesc child : measures) {
            if (child.getDependentMeasureRef() != null) {//存在依赖关系
                boolean ok = false;
                for (MeasureDesc parent : measures) {//在所有的度量中找到依赖的度量
                    if (parent.getName().equals(child.getDependentMeasureRef())) {//说明已经找到了
                        int childIndex = getIndexOf(child.getFunction());//度量本身的序号
                        int parentIndex = getIndexOf(parent.getFunction());//依赖的序号
                        result.put(childIndex, parentIndex);//添加映射关系
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    throw new IllegalStateException("Cannot find dependent measure: " + child.getDependentMeasureRef());
            }
        }
        return result.isEmpty() ? Collections.<Integer, Integer> emptyMap() : result;
    }

    //获取该维度集合的ImmutableBitSet,该维度集合必须是rowkey需要的集合的子集合
    public ImmutableBitSet makeGridTableColumns(Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = this.getIndexOf(dim);
            if (idx >= 0)
                result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    //获取该度量集合的ImmutableBitSet,该度量集合必须是锁有函数的集合的子集合
    public ImmutableBitSet makeGridTableColumns(Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = this.getIndexOf(metric);
            if (idx < 0)
                throw new IllegalStateException(metric + " not found in " + this);
            result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    //函数名称数组集合
    public String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {

        //metrics are represented in ImmutableBitSet, which loses order information
        //sort the aggrFuns to align with metrics natural order 
        List<FunctionDesc> metricList = Lists.newArrayList(metrics);

        Collections.sort(metricList, new Comparator<FunctionDesc>() {//按照函数出现的序号进行排序
            @Override
            public int compare(FunctionDesc o1, FunctionDesc o2) {
                int a = CuboidToGridTableMapping.this.getIndexOf(o1);
                int b = CuboidToGridTableMapping.this.getIndexOf(o2);
                return a - b;
            }
        });

        String[] result = new String[metricList.size()];//字符串数组,每一个函数存在一个字符串
        int i = 0;
        for (FunctionDesc metric : metricList) {
            result[i++] = metric.getExpression();
        }
        return result;
    }

}
