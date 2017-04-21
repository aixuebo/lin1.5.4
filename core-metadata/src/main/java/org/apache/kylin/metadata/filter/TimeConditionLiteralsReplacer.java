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

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TimeConditionLiteralsReplacer implements TupleFilterSerializer.Decorator {

    private IdentityHashMap<TupleFilter, DataType> dateCompareTupleChildren;

    public TimeConditionLiteralsReplacer(TupleFilter root) {
        this.dateCompareTupleChildren = Maps.newIdentityHashMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter cfilter = (CompareTupleFilter) filter;
            List<? extends TupleFilter> children = cfilter.getChildren();

            if (children == null || children.size() < 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            TblColRef col = cfilter.getColumn();
            if (col == null || !col.getType().isDateTimeFamily()) {
                return cfilter;
            }

            for (TupleFilter child : filter.getChildren()) {
                dateCompareTupleChildren.put(child, col.getType());
            }
        }

        if (filter instanceof ConstantTupleFilter && dateCompareTupleChildren.containsKey(filter)) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            Set<String> newValues = Sets.newHashSet();
            DataType columnType = dateCompareTupleChildren.get(filter);

            for (String value : (Collection<String>) constantTupleFilter.getValues()) {
                newValues.add(formatTime(value, columnType));
            }
            return new ConstantTupleFilter(newValues);
        }
        return filter;
    }

    /**
     *
     * @param dateStr 是定义好的时间字符串格式具体的值,比如20160404
     * @param dataType
     * @return 返回具体的时间值
     */
    private String formatTime(String dateStr, DataType dataType) {
        if (dataType.isDatetime() || dataType.isTime()) {//说明数据类型一定不是时间类型的,如果是时间类型的,就没必要进行格式化了
            throw new RuntimeException("Datetime and time type are not supported yet");
        }

        if (DateFormat.isSupportedDateFormat(dateStr)) {//true表示dateStr对应的时间值是允许的时间格式
            return dateStr;
        }

        long millis = Long.valueOf(dateStr);//说明此时是long时间戳
        if (dataType.isTimestamp()) {
            return DateFormat.formatToTimeStr(millis);//转换成yyyy-MM-dd HH:mm:ss.SSS
        } else if (dataType.isDate()) {
            return DateFormat.formatToDateStr(millis);//转换成yyyy-MM-dd
        } else {
            throw new RuntimeException("Unknown type " + dataType + " to formatTime");
        }
    }
}
