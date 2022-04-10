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

package org.apache.kylin.query.enumerator;

import java.util.Map;
import java.util.Properties;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读从kylin上取一行一行的数据
 */
public class OLAPEnumerator implements Enumerator<Object[]> {

    private final static Logger logger = LoggerFactory.getLogger(OLAPEnumerator.class);

    private final OLAPContext olapContext;//上下文对象
    private final DataContext optiqContext;
    private Object[] current;//读取的一行数据的返回值
    private ITupleIterator cursor;//读取的迭代器

    public OLAPEnumerator(OLAPContext olapContext, DataContext optiqContext) {
        this.olapContext = olapContext;
        this.optiqContext = optiqContext;
        this.cursor = null;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (cursor == null) {
            cursor = queryStorage();//查询kylin
        }

        if (!cursor.hasNext()) {
            return false;
        }

        ITuple tuple = cursor.next();//一行数据
        if (tuple == null) {
            return false;
        }
        convertCurrentRow(tuple);
        return true;
    }

    //设置一行数据
    private Object[] convertCurrentRow(ITuple tuple) {
        // make sure the tuple layout is correct
        //assert tuple.getAllFields().equals(olapContext.returnTupleInfo.getAllFields());

        current = tuple.getAllValues();
        return current;
    }

    //重新查询
    @Override
    public void reset() {
        close();
        cursor = queryStorage();
    }

    @Override
    public void close() {
        if (cursor != null)
            cursor.close();
    }

    //真正的查询kylin
    private ITupleIterator queryStorage() {
        logger.debug("query storage...");

        // set connection properties
        setConnectionProperties();//设置连接属性  后期与kylin建立连接

        // bind dynamic variables
        bindVariable(olapContext.filter);//绑定变量

        // cube don't have correct result for simple query without group by, but let's try to return something makes sense
        olapContext.resetSQLDigest();
        SQLDigest sqlDigest = olapContext.getSQLDigest();

        // query storage engine 查询引擎
        IStorageQuery storageEngine = StorageFactory.createQuery(olapContext.realization);
        ITupleIterator iterator = storageEngine.search(olapContext.storageContext, sqlDigest, olapContext.returnTupleInfo);//查询sql--返回迭代器
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        return iterator;
    }

    //绑定变量
    private void bindVariable(TupleFilter filter) {
        if (filter == null) {
            return;
        }

        for (TupleFilter childFilter : filter.getChildren()) {//递归绑定
            bindVariable(childFilter);
        }

        if (filter instanceof CompareTupleFilter && optiqContext != null) {
            CompareTupleFilter compFilter = (CompareTupleFilter) filter;
            for (Map.Entry<String, Object> entry : compFilter.getVariables().entrySet()) {
                String variable = entry.getKey();
                Object value = optiqContext.get(variable);
                if (value != null) {
                    String str = value.toString();
                    if (compFilter.getColumn().getType().isDateTimeFamily())
                        str = String.valueOf(DateFormat.stringToMillis(str));

                    compFilter.bindVariable(variable, str);
                }

            }
        }
    }

    //设置连接属性
    private void setConnectionProperties() {
        CalciteConnection conn = (CalciteConnection) optiqContext.getQueryProvider();
        Properties connProps = conn.getProperties();

        String propThreshold = connProps.getProperty(OLAPQuery.PROP_SCAN_THRESHOLD);//服务端口
        int threshold = Integer.valueOf(propThreshold);
        olapContext.storageContext.setThreshold(threshold);//设置端口
    }

}
