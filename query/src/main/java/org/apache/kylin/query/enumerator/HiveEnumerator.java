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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.query.relnode.OLAPContext;

/**
 * Hive Query Result Enumerator
 * 表示如何查询hive的数据
 * 该类也是calcite调用的读取一行数据的接口
 */
public class HiveEnumerator implements Enumerator<Object[]> {

    private final OLAPContext olapContext;
    private final Object[] current;//具体的值 表示一行数据列的集合
    private ResultSet rs;//查询结果
    private Connection conn;

    public HiveEnumerator(OLAPContext olapContext) {
        this.olapContext = olapContext;
        this.current = new Object[olapContext.returnTupleInfo.size()];//有多少个属性
    }

    //返回一行记录
    @Override
    public Object[] current() {
        return current;
    }

    //移动到下一行记录
    @Override
    public boolean moveNext() {
        if (rs == null) {
            rs = executeQuery();//去执行查询
        }
        return populateResult();//获取一条结果
    }

    //真正执行hive查询
    private ResultSet executeQuery() {
        String url = olapContext.olapSchema.getStarSchemaUrl();
        String user = olapContext.olapSchema.getStarSchemaUser();
        String pwd = olapContext.olapSchema.getStarSchemaPassword();
        String sql = olapContext.sql;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(url, user, pwd);
            stmt = conn.createStatement();
            return stmt.executeQuery(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(url + " can't execute query " + sql, e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            stmt = null;
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                conn = null;
            }
        }
    }

    //获取一条结果
    private boolean populateResult() {
        try {
            boolean hasNext = rs.next();//有下一行数据
            if (hasNext) {
                List<String> allFields = olapContext.returnTupleInfo.getAllFields();
                for (int i = 0; i < allFields.size(); i++) {//循环每一个属性
                    Object value = rs.getObject(allFields.get(i).toLowerCase());//获取属性对应的值
                    current[i] = value;//为下一行数据赋值
                }
            }
            return hasNext;
        } catch (SQLException e) {
            throw new IllegalStateException("Can't populate result!", e);
        }
    }

    //重新传
    @Override
    public void reset() {
        close();
        rs = executeQuery();
    }

    @Override
    public void close() {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can't close ResultSet!", e);
        }
    }

}
