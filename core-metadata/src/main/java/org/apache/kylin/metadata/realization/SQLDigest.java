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

package org.apache.kylin.metadata.realization;

import java.util.Collection;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 */
public class SQLDigest {

    //排序方式
    public enum OrderEnum {
        ASCENDING, DESCENDING
    }

    public String factTable;
    public TupleFilter filter;
    public Collection<JoinDesc> joinDescs;
    public Collection<TblColRef> allColumns;
    public Collection<TblColRef> groupbyColumns;//group by需要的维度集合
    public Collection<TblColRef> filterColumns;//where需要的维度集合

    public Collection<TblColRef> metricColumns;//度量的列
    public Collection<FunctionDesc> aggregations;
    public Collection<MeasureDesc> sortMeasures;
    public Collection<OrderEnum> sortOrders;//排序方式
    public boolean isRawQuery;//true表示没有group by 也没有聚合函数出现

    //initialized when org.apache.kylin.query.routing.QueryRouter.selectRealization()
    public SQLDigest(String factTable, TupleFilter filter, Collection<JoinDesc> joinDescs, Collection<TblColRef> allColumns, //
            Collection<TblColRef> groupbyColumns, Collection<TblColRef> filterColumns, Collection<TblColRef> aggregatedColumns, Collection<FunctionDesc> aggregateFunnc, Collection<MeasureDesc> sortMeasures, Collection<OrderEnum> sortOrders) {
        this.factTable = factTable;
        this.filter = filter;
        this.joinDescs = joinDescs;
        this.allColumns = allColumns;
        this.groupbyColumns = groupbyColumns;
        this.filterColumns = filterColumns;
        this.metricColumns = aggregatedColumns;
        this.aggregations = aggregateFunnc;
        this.sortMeasures = sortMeasures;
        this.sortOrders = sortOrders;
        this.isRawQuery = isRawQuery();
    }

    private boolean isRawQuery() {
        return this.groupbyColumns.isEmpty() && // select a group by a -> not raw
                this.aggregations.isEmpty(); // has aggr -> not raw
        //the reason to choose aggregations rather than metricColumns is because the former is set earlier at implOLAP
    }

    @Override
    public String toString() {
        return "fact table " + this.factTable + "," + //
                "group by " + this.groupbyColumns + "," + //
                "filter on " + this.filterColumns + "," + //
                "with aggregates" + this.aggregations + ".";
    }

}
