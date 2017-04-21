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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * @author xjiang
 * 对两个对象进行比较
 */
public class CompareTupleFilter extends TupleFilter {

    // operand 1 is either a column or a function 第一个出现的一定是一个列或者函数,比如列money>90,即出现的是列money
    private TblColRef column;
    private FunctionTupleFilter function;

    // operand 2 is constants  第二个出现的一定是值
    private Set<Object> conditionValues;//值集合
    private Object firstCondValue;//第一个值
    private Map<String, Object> dynamicVariables;//动态变量

    public CompareTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);//有两个子类
        this.conditionValues = new HashSet<Object>();
        this.dynamicVariables = new HashMap<String, Object>();
        //定义支持的操作
        boolean opGood = (op == FilterOperatorEnum.EQ || op == FilterOperatorEnum.NEQ //
                || op == FilterOperatorEnum.LT || op == FilterOperatorEnum.LTE //
                || op == FilterOperatorEnum.GT || op == FilterOperatorEnum.GTE //
                || op == FilterOperatorEnum.IN || op == FilterOperatorEnum.NOTIN //
                || op == FilterOperatorEnum.ISNULL || op == FilterOperatorEnum.ISNOTNULL);
        if (opGood == false)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    private CompareTupleFilter(CompareTupleFilter another) {
        super(new ArrayList<TupleFilter>(another.children), another.operator);
        this.column = another.column;
        this.conditionValues = new HashSet<Object>();
        this.conditionValues.addAll(another.conditionValues);
        this.dynamicVariables = new HashMap<String, Object>();
        this.dynamicVariables.putAll(another.dynamicVariables);
    }

    //添加一个元素
    @Override
    public void addChild(TupleFilter child) {
        super.addChild(child);
        if (child instanceof ColumnTupleFilter) {//说明第一个函数是字段
            ColumnTupleFilter columnFilter = (ColumnTupleFilter) child;
            if (this.column != null) {
                throw new IllegalStateException("Duplicate columns! old is " + column.getName() + " and new is " + columnFilter.getColumn().getName());
            }
            this.column = columnFilter.getColumn();
            // if value is before column, we need to reverse the operator. e.g. "1 >= c1" => "c1 <= 1" 如果值出现在列的前面,我们需要反转一下他们的关系
            if (!this.conditionValues.isEmpty() && needSwapOperator()) {
                this.operator = SWAP_OP_MAP.get(this.operator);//交换操作
                TupleFilter last = this.children.remove(this.children.size() - 1);
                this.children.add(0, last);
            }
        } else if (child instanceof ConstantTupleFilter) {//说明出现的是值
            this.conditionValues.addAll(child.getValues());
            if (!this.conditionValues.isEmpty()) {
                this.firstCondValue = this.conditionValues.iterator().next();
            }
        } else if (child instanceof DynamicTupleFilter) {
            DynamicTupleFilter dynamicFilter = (DynamicTupleFilter) child;
            if (!this.dynamicVariables.containsKey(dynamicFilter.getVariableName())) {
                this.dynamicVariables.put(dynamicFilter.getVariableName(), null);
            }
        } else if (child instanceof FunctionTupleFilter) {//说明第一个参数是函数
            this.function = (FunctionTupleFilter) child;
        }
    }

    //可以反转则返回true,因为==和!=没有必要反转,所以不会出现在这里面
    private boolean needSwapOperator() {
        return operator == FilterOperatorEnum.LT || operator == FilterOperatorEnum.GT || operator == FilterOperatorEnum.LTE || operator == FilterOperatorEnum.GTE;
    }

    @Override
    public Set<?> getValues() {
        return conditionValues;
    }

    public Object getFirstValue() {
        return firstCondValue;
    }

    public TblColRef getColumn() {
        return column;
    }

    public FunctionTupleFilter getFunction() {
        return function;
    }

    public Map<String, Object> getVariables() {
        return dynamicVariables;
    }

    //绑定一个变量值
    public void bindVariable(String variable, Object value) {
        this.dynamicVariables.put(variable, value);
        this.conditionValues.add(value);
        this.firstCondValue = this.conditionValues.iterator().next();
    }

    @Override
    public TupleFilter copy() {
        return new CompareTupleFilter(this);
    }

    @Override
    public TupleFilter reverse() {
        TupleFilter reverse = copy();
        reverse.operator = REVERSE_OP_MAP.get(this.operator);
        return reverse;
    }

    @Override
    public String toString() {
        return (function == null ? column : function) + " " + operator + " " + conditionValues;
    }

    // TODO requires generalize, currently only evaluates COLUMN {op} CONST
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem cs) {
        // extract tuple value
        Object tupleValue = null;
        for (TupleFilter filter : this.children) {
            if (!isConstant(filter)) {
                filter.evaluate(tuple, cs);
                tupleValue = filter.getValues().iterator().next();
            }
        }

        // consider null case
        if (cs.isNull(tupleValue)) {
            if (operator == FilterOperatorEnum.ISNULL)
                return true;
            else
                return false;
        }
        if (cs.isNull(firstCondValue)) {
            return false;
        }

        // tricky here -- order is ensured by string compare (even for number columns)
        // because it's row key ID (not real value) being compared
        int comp = cs.compare(tupleValue, firstCondValue);

        boolean result;
        switch (operator) {
        case EQ:
            result = comp == 0;
            break;
        case NEQ:
            result = comp != 0;
            break;
        case LT:
            result = comp < 0;
            break;
        case LTE:
            result = comp <= 0;
            break;
        case GT:
            result = comp > 0;
            break;
        case GTE:
            result = comp >= 0;
            break;
        case IN:
            result = conditionValues.contains(tupleValue);
            break;
        case NOTIN:
            result = !conditionValues.contains(tupleValue);
            break;
        default:
            result = false;
        }
        return result;
    }

    private boolean isConstant(TupleFilter filter) {
        return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
    }

    @Override
    public boolean isEvaluable() {
        return ((function != null && function.isEvaluable()) || column != null) && !conditionValues.isEmpty();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void serialize(IFilterCodeSystem cs, ByteBuffer buffer) {
        int size = this.dynamicVariables.size();
        BytesUtil.writeVInt(size, buffer);
        for (Map.Entry<String, Object> entry : this.dynamicVariables.entrySet()) {
            BytesUtil.writeUTFString(entry.getKey(), buffer);
            cs.serialize(entry.getValue(), buffer);
        }
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

        this.dynamicVariables.clear();
        int size = BytesUtil.readVInt(buffer);
        for (int i = 0; i < size; i++) {
            String name = BytesUtil.readUTFString(buffer);
            Object value = cs.deserialize(buffer);
            bindVariable(name, value);
        }
    }

}
