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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

/**
 * MeasureType captures how a kind of aggregation is defined, how it is calculated 
 * during cube build, and how it is involved in query and storage scan.
 * 
 * @param <T> the Java type of aggregation data object, e.g. HyperLogLogPlusCounter
 */
abstract public class MeasureType<T> {

    /* ============================================================================
     * Define
     * ---------------------------------------------------------------------------- */

    /** Validates a user defined FunctionDesc has expected parameter etc. Throw IllegalArgumentException if anything wrong.
     * 校验一个函数是否合法
     **/
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        return;
    }

    /** Although most aggregated object takes only 8 bytes like long or double, 
     * some advanced aggregation like HyperLogLog or TopN can consume more than 10 KB for 
     * each object, which requires special care on memory allocation.
     * 虽然大多数聚合对象都是仅仅8个字节就可以完成,比如long或者double
     * 但是一些高级的聚合,比如HyperLogLog or TopN 或者raw 能够消耗多余10k的内存,
     * 因此这种函数要内存的方面要小心,因此要返回true
     **/
    public boolean isMemoryHungry() {
        return false;
    }

    /** Return true if this MeasureType only aggregate values in base cuboid, and output initial value in child cuboid.
     * true 表示仅仅能在baseCuboid上才能发生的聚合
     **/
    public boolean onlyAggrInBaseCuboid() {
        return false;
    }

    /* ============================================================================
     * Build
     * ---------------------------------------------------------------------------- */

    /** Return a MeasureIngester which knows how to init aggregation object from raw records.
     * 如何对值进行处理,比如编码
     **/
    abstract public MeasureIngester<T> newIngester();

    /** Return a MeasureAggregator which does aggregation.
     * 如何聚合该字段数据
     **/
    abstract public MeasureAggregator<T> newAggregator();

    /** Some special measures need dictionary to encode column values for optimal storage. TopN is an example.
     * 获取度量中需要编码的列集合
     **/
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        return Collections.emptyList();
    }

    /* ============================================================================
     * Cube Selection
     * ---------------------------------------------------------------------------- */

    /**
     * Some special measures hold columns which are usually treated as dimensions (or vice-versa). 
     * This is where they override to influence cube capability check.
     * 
     * A SQLDigest contains dimensions and measures extracted from a query.
     * SQLDigest从query中抽取出维度和度量
     * After comparing to cube definition, the matched dimensions and measures are crossed out, and what's left is
     * the <code>unmatchedDimensions</code> and <code>unmatchedAggregations</code>.
     * 比较cube的定义之后,可以将一些维度和度量取消掉,扔到另外两个集合中 、
     *
     * Each measure type on the cube is then called on this method to check if any of the unmatched
     * can be fulfilled. If a measure type cannot fulfill any of the unmatched, it simply return null.
     * Or otherwise, <code>unmatchedDimensions</code> and <code>unmatchedAggregations</code> must
     * be modified to drop the satisfied dimension or measure, and a CapabilityInfluence object
     * must be returned to mark the contribution of this measure type.
     */
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc measureDesc) {
        return null;
    }

    /* ============================================================================
     * Query Rewrite
     * ---------------------------------------------------------------------------- */

    // TODO support user defined Calcite aggr function

    /** Whether or not Calcite rel-tree needs rewrite to do last around of aggregation */
    abstract public boolean needRewrite();

    /** Does the rewrite involves an extra field for the pre-calculated */
    public boolean needRewriteField() {
        return true;
    }

    /** Returns a Calcite aggregation function implementation class
     * 返回一个聚合函数对应的class
     **/
    abstract public Class<?> getRewriteCalciteAggrFunctionClass();

    /* ============================================================================
     * Storage
     * ---------------------------------------------------------------------------- */

    /**
     * Some special measures hold columns which are usually treated as dimensions (or vice-versa). 
     * They need to adjust dimensions and measures in <code>sqlDigest</code> before scanning,
     * such that correct cuboid and measures can be selected by storage.
     * 对sql的调整
     */
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
    }

    /** Return true if one storage record maps to multiple tuples, or false otherwise.
     * 是否需要更高级的填充,即一行数据填充到多行中
     * 即getAdvancedTupleFiller方法是否有实现
     **/
    public boolean needAdvancedTupleFilling() {
        return false;
    }

    /** The simple filling mode, one tuple per storage record.
     * 简单的填充模式,一行记录填充到一个属性中
     **/
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        //为tuple的indexInTuple个属性填充对应的值
        tuple.setMeasureValue(indexInTuple, measureValue);
    }

    /** The advanced filling mode, multiple tuples per storage record.
     * 高级的填充模式,每一条结果被填充到多行中
     **/
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo returnTupleInfo, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        throw new UnsupportedOperationException();
    }

    public static interface IAdvMeasureFiller {

        /** Reload a value from storage and get ready to fill multiple tuples with it.
         * 加载参数数据成一个数据集合
         **/
        void reload(Object measureValue);

        /** Returns how many rows contained in last loaded value.
         * 加载的数据是有多少行数据
         **/
        int getNumOfRows();

        /** Fill in specified row into tuple.
         * 将第row行的数据填充到tuple中
         **/
        void fillTuple(Tuple tuple, int row);
    }
}
