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

package org.apache.kylin.metadata.model;

import java.util.List;

/**
 * 为一个cube的segment产生一个hive的临时中间表,用于存储该build需要的数据内容
 * 参见org.apache.kylin.cube.model.CubeJoinedFlatTableDesc
 */
public interface IJoinedFlatTableDesc {

    String getTableName();//临时表的名字

    DataModelDesc getDataModel();//该cube需要哪个模型
    
    List<TblColRef> getAllColumns();//该cube需要的列集合---包含rowkey需要的列+扩展列+字典列+度量需要的列
    
    int getColumnIndex(TblColRef colRef);//获取一个列在列集合中的下标

    long getSourceOffsetStart();//该segment的开始日期
    
    long getSourceOffsetEnd();//该segment的结束日期
    
    TblColRef getDistributedBy();//kylin的rowkey中第一个设置shareBy为true的字段
}
