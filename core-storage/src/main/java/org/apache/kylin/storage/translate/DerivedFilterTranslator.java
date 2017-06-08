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

package org.apache.kylin.storage.translate;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.kv.RowKeyColumnOrder;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.cube.model.CubeDesc.DeriveType;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 * 
 */
public class DerivedFilterTranslator {

    private static final int IN_THRESHOLD = 5;//元素数量在5以内,使用in或者or操作,否则使用>= 或者 <=操作

    /**
     *
     * @param lookup 表示lookup表对象
     * @param hostInfo
     * @param compf
     * @return 返回值是一个元组,
     * TupleFilter表示where条件
     * Boolean表示操作是in/or  还是 >= <= 链接的and操作,true表示 >= <= 链接的and操作
     */
    public static Pair<TupleFilter, Boolean> translate(LookupStringTable lookup, DeriveInfo hostInfo, CompareTupleFilter compf) {

        TblColRef derivedCol = compf.getColumn();
        TblColRef[] hostCols = hostInfo.columns;
        TblColRef[] pkCols = hostInfo.dimension.getJoin().getPrimaryKeyColumns();//lookup表作为on条件的字段

        if (hostInfo.type == DeriveType.PK_FK) {
            assert hostCols.length == 1;//主外键说明一定是1个映射字段,即1对1的映射
            CompareTupleFilter newComp = new CompareTupleFilter(compf.getOperator());
            newComp.addChild(new ColumnTupleFilter(hostCols[0]));//直接转换成对应的字段=value即可
            newComp.addChild(new ConstantTupleFilter(compf.getValues()));
            return new Pair<TupleFilter, Boolean>(newComp, false);
        }

        assert hostInfo.type == DeriveType.LOOKUP;//如果不是PK_FK,此时也不会是扩展的,因此一定是LOOKUP表
        assert hostCols.length == pkCols.length;//不懂为什么此时也要是相同的

        int di = derivedCol.getColumnDesc().getZeroBasedIndex();//列序号
        int[] pi = new int[pkCols.length];//lookup表on条件的列序号集合
        int hn = hostCols.length;
        for (int i = 0; i < hn; i++) {
            pi[i] = pkCols[i].getColumnDesc().getZeroBasedIndex();
        }

        //创建一个行组成的数据结果集
        Set<Array<String>> satisfyingHostRecords = Sets.newHashSet();
        SingleColumnTuple tuple = new SingleColumnTuple(derivedCol);//一个列对应一个值对象,即该列有一个值
        for (String[] row : lookup.getAllRows()) {//获取表内所有的数据内容
            tuple.value = row[di];//设置该列对应的值
            if (compf.evaluate(tuple, StringCodeSystem.INSTANCE)) {
                collect(row, pi, satisfyingHostRecords);
            }
        }

        TupleFilter translated;
        boolean loosened;//true表示使用非in操作
        if (satisfyingHostRecords.size() > IN_THRESHOLD) {
            translated = buildRangeFilter(hostCols, satisfyingHostRecords);
            loosened = true;
        } else {
            translated = buildInFilter(hostCols, satisfyingHostRecords);
            loosened = false;
        }

        return new Pair<TupleFilter, Boolean>(translated, loosened);
    }

    /**
     * 从一行数据中抽取想要的数据,添加到第三个参数集合中
     * @param row 一行原始数据
     * @param pi 需要哪些列的属性值
     * @param satisfyingHostRecords 最终组合的一行数据添加到该集合里面
     */
    private static void collect(String[] row, int[] pi, Set<Array<String>> satisfyingHostRecords) {
        // TODO when go beyond IN_THRESHOLD, only keep min/max is enough
        String[] rec = new String[pi.length];//多少列就有多少个元素
        for (int i = 0; i < pi.length; i++) {//循环每一个列序号
            rec[i] = row[pi[i]];//从row中获取该列对应的值
        }
        satisfyingHostRecords.add(new Array<String>(rec));
    }

    //组成根据列的值过滤filter操作
    private static TupleFilter buildInFilter(TblColRef[] hostCols, Set<Array<String>> satisfyingHostRecords) {
        if (satisfyingHostRecords.size() == 0) {
            return ConstantTupleFilter.FALSE;
        }

        int hn = hostCols.length;
        if (hn == 1) {//说明只有一列
            CompareTupleFilter in = new CompareTupleFilter(FilterOperatorEnum.IN);//列 in ()
            in.addChild(new ColumnTupleFilter(hostCols[0]));//该列对应的具体的值
            in.addChild(new ConstantTupleFilter(asValues(satisfyingHostRecords)));//每一行所有的值组成的集合作为in的参数
            return in;
        } else {//说明有多列
            //每一行组成or连接,行里面的数据使用and连接
            //比如  (列1 = 1 and 列2 = 2) or (列1 = 11 and 列2 = 22)
            LogicalTupleFilter or = new LogicalTupleFilter(FilterOperatorEnum.OR);
            for (Array<String> rec : satisfyingHostRecords) {//循环每一行
                LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
                for (int i = 0; i < hn; i++) {//每一个列使用and连接
                    CompareTupleFilter eq = new CompareTupleFilter(FilterOperatorEnum.EQ);//列 = 常数值
                    eq.addChild(new ColumnTupleFilter(hostCols[i]));//该列对应的具体的值
                    eq.addChild(new ConstantTupleFilter(rec.data[i]));//获取该列具体的值
                    and.addChild(eq);
                }
                or.addChild(and);
            }
            return or;
        }
    }

    //返回每一行数据的第一个元素组成的集合
    private static List<String> asValues(Set<Array<String>> satisfyingHostRecords) {
        List<String> values = Lists.newArrayListWithCapacity(satisfyingHostRecords.size());//有多少行数据,就有多少个元素
        for (Array<String> rec : satisfyingHostRecords) {
            values.add(rec.data[0]);//返回每一行数据的第一列的值
        }
        return values;
    }

    /**
     * @param satisfyingHostRecords 每一个元素是一行数据,因为一行数据是多列组成,因此元素是由Array组成的
     * 组成 每一个列>=min and 每一个列<=min的过滤器
     */
    private static LogicalTupleFilter buildRangeFilter(TblColRef[] hostCols, Set<Array<String>> satisfyingHostRecords) {
        int hn = hostCols.length;//有多少个列
        //每一个列的最小值和最大值
        String[] min = new String[hn];
        String[] max = new String[hn];
        findMinMax(satisfyingHostRecords, hostCols, min, max);//找到每一个列的最大值和最小值,并且填充到min和max上
        LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
        for (int i = 0; i < hn; i++) {
            CompareTupleFilter compMin = new CompareTupleFilter(FilterOperatorEnum.GTE);//设置该列>= min值
            compMin.addChild(new ColumnTupleFilter(hostCols[i]));//具体该列在每一行的值
            compMin.addChild(new ConstantTupleFilter(min[i]));//具体最小值
            and.addChild(compMin);

            CompareTupleFilter compMax = new CompareTupleFilter(FilterOperatorEnum.LTE);//设置该列<=max值
            compMax.addChild(new ColumnTupleFilter(hostCols[i]));//具体的该列在每一行的值
            compMax.addChild(new ConstantTupleFilter(max[i]));//具体的最大值
            and.addChild(compMax);
        }
        return and;
    }

    /**
     * 找到每一个列的最大值和最小值
     * @param satisfyingHostRecords 每一个元素是一行数据,因为一行数据是多列组成,因此元素是由Array组成的
     */
    private static void findMinMax(Set<Array<String>> satisfyingHostRecords, TblColRef[] hostCols, String[] min, String[] max) {

        RowKeyColumnOrder[] orders = new RowKeyColumnOrder[hostCols.length];//每一个列对应一个比较对象
        for (int i = 0; i < hostCols.length; i++) {
            orders[i] = RowKeyColumnOrder.getInstance(hostCols[i].getType());
        }

        for (Array<String> rec : satisfyingHostRecords) {//循环每一行数据
            String[] row = rec.data;//获取每一个列数据
            for (int i = 0; i < row.length; i++) {//循环每一个列
                //设置每一个列的最大值和最小值
                min[i] = orders[i].min(min[i], row[i]);
                max[i] = orders[i].max(max[i], row[i]);
            }
        }
    }

    //一个列对应一个值对象
    private static class SingleColumnTuple implements IEvaluatableTuple {

        private TblColRef col;
        private String value;

        SingleColumnTuple(TblColRef col) {
            this.col = col;
        }

        @Override
        public Object getValue(TblColRef col) {
            if (this.col.equals(col))
                return value;
            else
                throw new IllegalArgumentException("unexpected column " + col);
        }

    }

}
