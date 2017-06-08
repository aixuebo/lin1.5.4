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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

//支持逻辑的查询
//仅仅支持 and or not 三个操作
public class LogicalTupleFilter extends TupleFilter {

    public LogicalTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);
        boolean opGood = (op == FilterOperatorEnum.AND || op == FilterOperatorEnum.OR || op == FilterOperatorEnum.NOT);
        if (opGood == false)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    private LogicalTupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        super(filters, op);
    }

    @Override
    public TupleFilter copy() {
        List<TupleFilter> cloneChildren = new LinkedList<TupleFilter>(children);
        TupleFilter cloneTuple = new LogicalTupleFilter(cloneChildren, operator);
        return cloneTuple;
    }

    //    private TupleFilter reverseNestedNots(TupleFilter filter, int depth) {
    //        if ((filter instanceof LogicalTupleFilter) && (filter.operator == FilterOperatorEnum.NOT)) {
    //            assert (filter.children.size() == 1);
    //            return reverseNestedNots(filter.children.get(0), depth + 1);
    //        }
    //
    //        if (depth % 2 == 1) {
    //            return filter;
    //        } else {
    //            return filter.reverse();
    //        }
    //    }

    //反转操作
    @Override
    public TupleFilter reverse() {
        switch (operator) {
        case NOT:
            throw new IllegalStateException("not( not in ()) is invalid syntax");
            //return reverseNestedNots(this, 0);
        case AND:
        case OR:
            LogicalTupleFilter reverse = new LogicalTupleFilter(REVERSE_OP_MAP.get(operator));//比如原来是and操作,反转后变成or操作
            for (TupleFilter child : children) {
                reverse.addChild(child.reverse());
            }
            return reverse;
        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public String toString() {
        return operator + " " + children;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        switch (this.operator) {
        case AND:
            return evalAnd(tuple, cs);
        case OR:
            return evalOr(tuple, cs);
        case NOT:
            return evalNot(tuple, cs);
        default:
            return false;
        }
    }

    //与操作
    private boolean evalAnd(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        for (TupleFilter filter : this.children) {
            if (!filter.evaluate(tuple, cs)) {//都是true结果才是true,有一个是false,结果都是false
                return false;
            }
        }
        return true;
    }

    //or操作,有一个是true,则结果就是true
    private boolean evalOr(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        for (TupleFilter filter : this.children) {
            if (filter.evaluate(tuple, cs)) {//有任意一个是true,结果都是true
                return true;
            }
        }
        return false;
    }

    //not操作只要一个child----例如WHERE NOT condition
    private boolean evalNot(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        return !this.children.get(0).evaluate(tuple, cs);//返回值是false,则即true
    }

    //没有返回值
    @Override
    public Collection<?> getValues() {
        return Collections.emptyList();
    }

    //是否能执行
    @Override
    public boolean isEvaluable() {
        switch (operator) {
        case NOT:
            // Un-evaluatable branch will be pruned and be replaced with TRUE.
            // And this must happen at the top NOT, otherwise NOT (TRUE) becomes false.
            for (TupleFilter child : children) {
                if (TupleFilter.isEvaluableRecursively(child) == false)
                    return false;
            }
            return true;
        case OR:
            // (anything OR un-evaluable) will become (anything or TRUE) which is effectively TRUE.
            // The "anything" is not evaluated, kinda disabled, by the un-evaluable part.
            // If it's partially un-evaluable, then "anything" is partially disabled, and the OR is still not fully evaluatable.
            for (TupleFilter child : children) {
                if (TupleFilter.isEvaluableRecursively(child) == false) //说明子类表达式有不可执行的
                    return false;
            }
            return true;
        default://and操作符是一定可以被执行的
            return true;
        }
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        //do nothing
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

    }

}
