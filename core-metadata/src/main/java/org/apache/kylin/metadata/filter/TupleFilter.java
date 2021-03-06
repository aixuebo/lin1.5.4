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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Maps;

/**
 * 
 * @author xjiang
 * 
 */
public abstract class TupleFilter {

    //过滤操作符号
    public enum FilterOperatorEnum {
        //以下比较的操作符实现类都是CompareTupleFilter
        EQ(1), NEQ(2),//等于  不等于
        GT(3), LT(4), GTE(5), LTE(6),//大于  小于 大于等于  小于等于
        ISNULL(7), ISNOTNULL(8),//是否是null
        IN(9), NOTIN(10),//in 和not in

        AND(20), OR(21), NOT(22),//关系操作---LogicalTupleFilter具体实现类

        COLUMN(30),//列名字---说明该表达式可以代表一个列以及该列在运行中对应的值  ColumnTupleFilter具体实现类,该类是不允许支持子类的
        CONSTANT(31),//常量---ConstantTupleFilter具体实现类,该类是不允许支持子类的
        DYNAMIC(32),//动态绑定一个变量名字---DynamicTupleFilter是具体的实现类,该类是不允许支持子类的
        EXTRACT(33),//抽取数据---ExtractTupleFilter具体的实现类,抽取日期和时间的数据值
        CASE(34),//case when then操作-----CaseTupleFilter具体的实现类,子类有若干个Filter组成
        FUNCTION(35),//函数
        MASSIN(36),//参见MassInTupleFilter实现类 表示块函数
        EVAL_FUNC(37),
        UNSUPPORTED(38);//表示不支持的过滤器--具体实现UnsupportedTupleFilter

        private final int value;

        private FilterOperatorEnum(int v) {
            this.value = v;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static final int BUFFER_SIZE = 10240;

    protected static final Map<FilterOperatorEnum, FilterOperatorEnum> REVERSE_OP_MAP = Maps.newHashMap();//可以反转的操作,比如1 >= c1,意味着c1 < 1,表示意义都变化了
    protected static final Map<FilterOperatorEnum, FilterOperatorEnum> SWAP_OP_MAP = Maps.newHashMap();//可以交换的操作,比如1 >= c1" => "c1 <= 1 等价交换,即左右两边交换一下顺序而已,意义不变化,常常用于将sql属性 和常量的操作写反的时候,交换一下

    static {
        REVERSE_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.NEQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.EQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.IN, FilterOperatorEnum.NOTIN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NOTIN, FilterOperatorEnum.IN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNULL, FilterOperatorEnum.ISNOTNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNOTNULL, FilterOperatorEnum.ISNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.AND, FilterOperatorEnum.OR);
        REVERSE_OP_MAP.put(FilterOperatorEnum.OR, FilterOperatorEnum.AND);

        SWAP_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.EQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.NEQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LT);
        SWAP_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GTE);
        SWAP_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GT);
        SWAP_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LTE);
    }

    protected final List<TupleFilter> children;//子类
    protected FilterOperatorEnum operator;//如何操作子类

    protected TupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        this.children = filters;
        this.operator = op;
    }

    public void addChild(TupleFilter child) {
        children.add(child);
    }

    final public void addChildren(List<? extends TupleFilter> children) {
        for (TupleFilter c : children)
            addChild(c); // subclass overrides addChild()
    }

    public List<? extends TupleFilter> getChildren() {
        return children;
    }

    public boolean hasChildren() {
        return children != null && !children.isEmpty();
    }

    public FilterOperatorEnum getOperator() {
        return operator;
    }

    public TupleFilter copy() {
        throw new UnsupportedOperationException();
    }

    public TupleFilter reverse() {
        throw new UnsupportedOperationException();
    }

    /**
     * flatten to OR-AND filter, (A AND B AND ..) OR (C AND D AND ..) OR ..
     * flatten filter will ONLY contain AND and OR , no NOT will exist.
     * This will help to decide scan ranges.
     * 
     * Notice that the flatten filter will ONLY be used for determining scan ranges,
     * The filter that is later pushed down into storage level is still the ORIGINAL
     * filter, since the flattened filter will be too "fat" to evaluate
     * 
     * @return
     * 让条件扁平化起来
     */
    public TupleFilter flatFilter() {
        return flattenInternal(this);
    }

    private TupleFilter flattenInternal(TupleFilter filter) {
        TupleFilter flatFilter = null;
        if (!(filter instanceof LogicalTupleFilter)) {//将一个单一的filter  转换成 and filter对象
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
            flatFilter.addChild(filter);
            return flatFilter;
        }

        //代码执行到这边,说明filter就是LogicalTupleFilter
        // post-order recursive travel
        FilterOperatorEnum op = filter.getOperator();//本身操作
        List<TupleFilter> andChildren = new LinkedList<TupleFilter>();
        List<TupleFilter> orChildren = new LinkedList<TupleFilter>();

        for (TupleFilter child : filter.getChildren()) {
            TupleFilter flatChild = flattenInternal(child);//属于递归操作
            FilterOperatorEnum childOp = flatChild.getOperator();
            if (childOp == FilterOperatorEnum.AND) {//属于and操作
                andChildren.add(flatChild);
            } else if (childOp == FilterOperatorEnum.OR) {//属于or操作
                orChildren.add(flatChild);
            } else {
                throw new IllegalStateException("Filter is " + filter + " and child is " + flatChild);
            }
        }

        // boolean algebra flatten
        if (op == FilterOperatorEnum.AND) {
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);//组建新的and filter
            for (TupleFilter andChild : andChildren) {//添加and 子节点
                flatFilter.addChildren(andChild.getChildren());
            }
            if (!orChildren.isEmpty()) {//or不是空
                List<TupleFilter> fullAndFilters = cartesianProduct(orChildren, flatFilter);//将or的集合和and的集合进行笛卡尔乘积运算
                flatFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);//最终的结果就是or操作所有的笛卡尔乘积
                flatFilter.addChildren(fullAndFilters);
            }
        } else if (op == FilterOperatorEnum.OR) {//组建新的or filter
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
            for (TupleFilter orChild : orChildren) {
                flatFilter.addChildren(orChild.getChildren());
            }
            flatFilter.addChildren(andChildren);//and作为一个整体添加到or中
        } else if (op == FilterOperatorEnum.NOT) {
            assert (filter.children.size() == 1);//一定只有一个子节点
            TupleFilter reverse = filter.children.get(0).reverse();
            flatFilter = flattenInternal(reverse);
        } else {
            throw new IllegalStateException("Filter is " + filter);
        }
        return flatFilter;
    }

    //笛卡尔乘积运算
    private List<TupleFilter> cartesianProduct(List<TupleFilter> leftOrFilters, TupleFilter partialAndFilter) {
        //and
        List<TupleFilter> oldProductFilters = new LinkedList<TupleFilter>();
        oldProductFilters.add(partialAndFilter);

        for (TupleFilter orFilter : leftOrFilters) {//循环所有的的or条件
            List<TupleFilter> newProductFilters = new LinkedList<TupleFilter>();
            for (TupleFilter orChildFilter : orFilter.getChildren()) {
                for (TupleFilter productFilter : oldProductFilters) {
                    TupleFilter fullAndFilter = productFilter.copy();
                    fullAndFilter.addChildren(orChildFilter.getChildren());
                    newProductFilters.add(fullAndFilter);
                }
            }
            oldProductFilters = newProductFilters;
        }
        return oldProductFilters;
    }

    //该函数是否能够执行,true表示可以执行
    public abstract boolean isEvaluable();

    //具体执行过滤函数,true表示执行成功
    public abstract boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs);

    //执行结果
    public abstract Collection<?> getValues();

    public abstract void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public abstract void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    //所有的子类过滤器一定都得是可执行的,才会返回true
    public static boolean isEvaluableRecursively(TupleFilter filter) {
        if (filter == null)
            return true;

        if (!filter.isEvaluable())//说明该filter不能被执行,因此结果是false
            return false;

        for (TupleFilter child : filter.getChildren()) {
            if (!isEvaluableRecursively(child))
                return false;
        }
        return true;
    }

    //收集filter的子子孙孙中,需要的列集合,将收集的列集合存储到collector集合中
    public static void collectColumns(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null || collector == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            ColumnTupleFilter columnTupleFilter = (ColumnTupleFilter) filter;
            collector.add(columnTupleFilter.getColumn());
        }

        for (TupleFilter child : filter.getChildren()) {
            collectColumns(child, collector);
        }
    }

}
