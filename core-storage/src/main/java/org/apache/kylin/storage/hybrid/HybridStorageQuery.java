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
package org.apache.kylin.storage.hybrid;

import java.util.List;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageFactory;

import com.google.common.collect.Lists;

/**
 * 因为hybrid是混血模式,即将多个cube组合起来,作为代理方式来查询sql
 *
 * 该类表示如何真正的去查询sql,即使用代理类查询sql,返回每一个cube具体的查询结果的迭代器
 */
public class HybridStorageQuery implements IStorageQuery {

    //每一个cube对应一个具体的cube和查询接口
    private IRealization[] realizations;//元素是具体的cube
    private IStorageQuery[] storageEngines;//元素是具体cube的查询结构

    public HybridStorageQuery(HybridInstance hybridInstance) {
        this.realizations = hybridInstance.getRealizations();
        storageEngines = new IStorageQuery[realizations.length];
        for (int i = 0; i < realizations.length; i++) {
            storageEngines[i] = StorageFactory.createQuery(realizations[i]);
        }
    }

    //真正的查询sql
    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {
        List<ITupleIterator> tupleIterators = Lists.newArrayList();//返回值
        for (int i = 0; i < realizations.length; i++) {//查询每一个cube
            if (realizations[i].isReady() && realizations[i].isCapable(sqlDigest).capable) {//说明该cube是可以查询该sql的
                ITupleIterator dataIterator = storageEngines[i].search(context, sqlDigest, returnTupleInfo);//真正的查询sql,返回查询的结果
                tupleIterators.add(dataIterator);
            }
        }
        // combine tuple iterator
        return new CompoundTupleIterator(tupleIterators);//组合所有的查询结果返回给客户端
    }

}
