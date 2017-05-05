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

import java.util.List;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

//实现类是CubeInstance和HybridInstance
public interface IRealization extends IStorageAware {

    /**
     * Given the features of a query, check how capable the realization is to answer the query.
     * true表示cube的定义是能够满足sql查询的
     */
    public CapabilityResult isCapable(SQLDigest digest);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     */
    public RealizationType getType();//返回类型,比如CUBE

    public DataModelDesc getDataModelDesc();//获取该cube对应的model对象

    public String getFactTable();//获取该cube对应的fact表

    public List<TblColRef> getAllColumns();

    public List<TblColRef> getAllDimensions();

    public List<MeasureDesc> getMeasures();

    public boolean isReady();//是否已经ready状态

    public String getName();//cube的名字

    public String getCanonicalName();//type+name,即可以容易读的名字

    public long getDateRangeStart();//ready中最小的segment对应的开始位置

    public long getDateRangeEnd();//ready中最大的segment对应的结束位置

    public boolean supportsLimitPushDown();
}
