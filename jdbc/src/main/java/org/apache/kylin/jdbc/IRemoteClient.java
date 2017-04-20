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

package org.apache.kylin.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;

public interface IRemoteClient extends Closeable {

    //查询结果
    public static class QueryResult {
        public final List<ColumnMetaData> columnMeta;//将查询结果转换成列对象,即列的元数据,即查询的sql中每一列的详细信息,包括列名,列的类型
        public final Iterable<Object> iterable;//查询结果的迭代器,因为每一行数据是包含多个列的值,而且不同列,类型不一样,因此Object是Object[]

        public QueryResult(List<ColumnMetaData> columnMeta, Iterable<Object> iterable) {
            this.columnMeta = columnMeta;
            this.iterable = iterable;
        }
    }

    /**
     * Connect to Kylin restful service. IOException will be thrown if authentication failed.
     * 连接到kylin sql执行服务器
     */
    public void connect() throws IOException;

    /**
     * Retrieve meta data of given project.
     * 获取projecy的元数据信息
     */
    public KMetaProject retrieveMetaData(String project) throws IOException;

    /**
     * Execute query remotely and get back result.
     * @param  params 表示预编译sql中?的位置
     * @param  paramValues 表示预编译sql中?对应的值
     * 去远程执行一个查询sql任务,返回结果集
     */
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues) throws IOException;

}
