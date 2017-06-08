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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.kv.RowKeyColumnOrder;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

/**
 * 
 * @author xjiang
 * 设置每一个属性的开始--结束范围
 */
public class ColumnValueRange {
    private TblColRef column;//属性
    private RowKeyColumnOrder order;//如何对值进行比较
    //开始和结束值
    private String beginValue;
    private String endValue;
    private Set<String> equalValues;//因为有in操作,可以相等的value可能是一个集合

    public ColumnValueRange(TblColRef column, Collection<String> values, FilterOperatorEnum op) {
        this.column = column;
        this.order = RowKeyColumnOrder.getInstance(column.getType());

        switch (op) {
        case EQ:
        case IN:
            equalValues = new HashSet<String>(values);
            refreshBeginEndFromEquals();
            break;
        case LT:
        case LTE:
            endValue = order.max(values);
            break;
        case GT:
        case GTE:
            beginValue = order.min(values);
            break;
        case NEQ:
        case NOTIN:
        case ISNULL: // TODO ISNULL worth pass down as a special equal value
        case ISNOTNULL:
            // let Optiq filter it!
            break;
        default:
            throw new UnsupportedOperationException(op.name());
        }
    }

    public ColumnValueRange(TblColRef column, String beginValue, String endValue, Set<String> equalValues) {
        copy(column, beginValue, endValue, equalValues);
    }

    void copy(TblColRef column, String beginValue, String endValue, Set<String> equalValues) {
        this.column = column;
        this.order = RowKeyColumnOrder.getInstance(column.getType());
        this.beginValue = beginValue;
        this.endValue = endValue;
        this.equalValues = equalValues;
    }

    public TblColRef getColumn() {
        return column;
    }

    public String getBeginValue() {
        return beginValue;
    }

    public String getEndValue() {
        return endValue;
    }

    public Set<String> getEqualValues() {
        return equalValues;
    }

    private void refreshBeginEndFromEquals() {
        this.beginValue = order.min(this.equalValues);
        this.endValue = order.max(this.equalValues);
    }

    public boolean satisfyAll() {
        return beginValue == null && endValue == null && equalValues == null; // the NEQ case
    }

    public boolean satisfyNone() {
        if (equalValues != null) {
            return equalValues.isEmpty();
        } else if (beginValue != null && endValue != null) {
            return order.compare(beginValue, endValue) > 0;
        } else {
            return false;
        }
    }

    public void andMerge(ColumnValueRange another) {
        assert this.column.equals(another.column);

        if (another.satisfyAll()) {
            return;
        }

        if (this.satisfyAll()) {
            copy(another.column, another.beginValue, another.endValue, another.equalValues);
            return;
        }

        if (this.equalValues != null && another.equalValues != null) {
            this.equalValues.retainAll(another.equalValues);
            refreshBeginEndFromEquals();
            return;
        }

        if (this.equalValues != null) {
            this.equalValues = filter(this.equalValues, another.beginValue, another.endValue);
            refreshBeginEndFromEquals();
            return;
        }

        if (another.equalValues != null) {
            this.equalValues = filter(another.equalValues, this.beginValue, this.endValue);
            refreshBeginEndFromEquals();
            return;
        }

        this.beginValue = order.max(this.beginValue, another.beginValue);
        this.endValue = order.min(this.endValue, another.endValue);
    }

    /**
     * 返回equalValues中的元素在[beginValue,endValue]之间的数据
     */
    private Set<String> filter(Set<String> equalValues, String beginValue, String endValue) {
        Set<String> result = Sets.newHashSetWithExpectedSize(equalValues.size());
        for (String v : equalValues) {
            if (between(v, beginValue, endValue)) {
                result.add(v);
            }
        }
        return equalValues;
    }

    //如果v在[beginValue,endValue]则返回true
    private boolean between(String v, String beginValue, String c) {
        return (beginValue == null || order.compare(beginValue, v) <= 0) && (endValue == null || order.compare(v, endValue) <= 0);
    }

    // remove invalid EQ/IN values and round start/end according to dictionary
    //如果value值不在字典里面,则删除该值
    //获取开始和结束值在字段里面的序号
    public void preEvaluateWithDict(Dictionary<String> dict) {
        if (dict == null)
            return;

        if (equalValues != null) {//属于in操作
            Iterator<String> it = equalValues.iterator();//循环每一个元素
            while (it.hasNext()) {
                String v = it.next();
                try {
                    dict.getIdFromValue(v);
                } catch (IllegalArgumentException e) {
                    // value not in dictionary 说明该值不再字典里面,则删除该值
                    it.remove();
                }
            }
            refreshBeginEndFromEquals();
        }

        if (beginValue != null) {
            try {
                beginValue = dict.getValueFromId(dict.getIdFromValue(beginValue, 1));
            } catch (IllegalArgumentException e) {
                // beginValue is greater than the biggest in dictionary, mark FALSE
                equalValues = Sets.newHashSet();
            }
        }

        if (endValue != null) {
            try {
                endValue = dict.getValueFromId(dict.getIdFromValue(endValue, -1));
            } catch (IllegalArgumentException e) {
                // endValue is lesser than the smallest in dictionary, mark FALSE
                equalValues = Sets.newHashSet();
            }
        }
    }

    public String toString() {
        if (equalValues == null) {
            return column.getName() + " between " + beginValue + " and " + endValue;
        } else {
            return column.getName() + " in " + equalValues;
        }
    }
}
