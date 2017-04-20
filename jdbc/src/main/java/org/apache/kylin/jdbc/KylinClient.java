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

import java.io.IOException;
import java.math.BigDecimal;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;
import org.apache.kylin.jdbc.json.PreparedQueryRequest;
import org.apache.kylin.jdbc.json.QueryRequest;
import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.apache.kylin.jdbc.json.StatementParameter;
import org.apache.kylin.jdbc.json.TableMetaStub;
import org.apache.kylin.jdbc.json.TableMetaStub.ColumnMetaStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * jdbc:kylin://<hostname>:<port>/<kylin_project_name>
 * 连接ip:port,使用http去连接一个节点
 *
 * 去远程执行sql,然后将返回的结果内容进行转换成对象
 */
public class KylinClient implements IRemoteClient {

    private static final Logger logger = LoggerFactory.getLogger(KylinClient.class);

    private final KylinConnection conn;
    private final Properties connProps;
    private CloseableHttpClient httpClient;
    private final ObjectMapper jsonMapper;

    public KylinClient(KylinConnection conn) {
        this.conn = conn;
        this.connProps = conn.getConnectionProperties();
        this.httpClient = HttpClients.createDefault();
        this.jsonMapper = new ObjectMapper();

        // trust all certificates
        if (isSSL()) {
            try {
                TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
                    public boolean isTrusted(X509Certificate[] certificate, String type) {
                        return true;
                    }
                };
                SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
                httpClient = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).setSSLContext(sslContext).build();
            } catch (Exception e) {
                throw new RuntimeException("Initialize HTTPS client failed", e);
            }
        }
    }

    //mysql类型转换成java的类
    @SuppressWarnings("rawtypes")
    public static Class convertType(int sqlType) {
        Class result = Object.class;

        switch (sqlType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            result = String.class;
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            result = BigDecimal.class;
            break;
        case Types.BIT:
            result = Boolean.class;
            break;
        case Types.TINYINT:
            result = Byte.class;
            break;
        case Types.SMALLINT:
            result = Short.class;
            break;
        case Types.INTEGER:
            result = Integer.class;
            break;
        case Types.BIGINT:
            result = Long.class;
            break;
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            result = Double.class;
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            result = Byte[].class;
            break;
        case Types.DATE:
            result = Date.class;
            break;
        case Types.TIME:
            result = Time.class;
            break;
        case Types.TIMESTAMP:
            result = Timestamp.class;
            break;
        default:
            //do nothing
            break;
        }

        return result;
    }

    //根据sql的类型转换成java的类型,并且将该java对象初始化value
    public static Object wrapObject(String value, int sqlType) {
        if (null == value) {
            return null;
        }

        switch (sqlType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return value;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimal(value);
        case Types.BIT:
            return Boolean.parseBoolean(value);
        case Types.TINYINT:
            return Byte.valueOf(value);
        case Types.SMALLINT:
            return Short.valueOf(value);
        case Types.INTEGER:
            return Integer.parseInt(value);
        case Types.BIGINT:
            return Long.parseLong(value);
        case Types.FLOAT:
            return Float.parseFloat(value);
        case Types.REAL:
        case Types.DOUBLE:
            return Double.parseDouble(value);
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return value.getBytes();
        case Types.DATE:
            return Date.valueOf(value);
        case Types.TIME:
            return Time.valueOf(value);
        case Types.TIMESTAMP:
            return Timestamp.valueOf(value);
        default:
            //do nothing
            break;

        }

        return value;
    }

    private boolean isSSL() {
        return Boolean.parseBoolean(connProps.getProperty("ssl", "false"));
    }

    private String baseUrl() {
        return (isSSL() ? "https://" : "http://") + conn.getBaseUrl();
    }

    //设置http请求头
    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");//接收json数据
        method.addHeader("Content-Type", "application/json");

        //用户名和密码转换成base64
        String username = connProps.getProperty("user");
        String password = connProps.getProperty("password");
        String basicAuth = DatatypeConverter.printBase64Binary((username + ":" + password).getBytes());
        method.addHeader("Authorization", "Basic " + basicAuth);
    }

    //发送用户名和密码,去连接服务器
    @Override
    public void connect() throws IOException {
        HttpPost post = new HttpPost(baseUrl() + "/kylin/api/user/authentication");
        addHttpHeaders(post);
        StringEntity requestEntity = new StringEntity("{}", ContentType.create("application/json", "UTF-8"));
        post.setEntity(requestEntity);

        CloseableHttpResponse response = httpClient.execute(post);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(post, response);
        }
        response.close();
    }

    //获取project下元数据结果

    /**
     按照 SQL 标准的解释，在 SQL 环境下 Catalog 和 Schema 都属于抽象概念，可以把它们理解为一个容器或者数据库对象命名空间中的一个层次，主要用来解决命名冲突问题。
     从概念上说，一个数据库系统包含多个 Catalog，每个 Catalog 又包含多个 Schema，而每个 Schema 又包含多个数据库对象（表、视图、字段等），反过来讲一个数据库对象必然属于一个 Schema，
     而该 Schema 又必然属于一个 Catalog，这样我们就可以得到该数据库对象的完全限定名称从而解决命名冲突的问题了；例如数据库对象表的完全限定名称就可以表示为：Catalog名称.Schema名称.表名称。
     */
    @Override
    public KMetaProject retrieveMetaData(String project) throws IOException {
        assert conn.getProject().equals(project);

        String url = baseUrl() + "/kylin/api/tables_and_columns?project=" + project;
        HttpGet get = new HttpGet(url);
        addHttpHeaders(get);

        CloseableHttpResponse response = httpClient.execute(get);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(get, response);
        }

        //解析返回的json
        List<TableMetaStub> tableMetaStubs = jsonMapper.readValue(response.getEntity().getContent(), new TypeReference<List<TableMetaStub>>() {
        });

        List<KMetaTable> tables = convertMetaTables(tableMetaStubs);
        List<KMetaSchema> schemas = convertMetaSchemas(tables);
        List<KMetaCatalog> catalogs = convertMetaCatalogs(schemas);
        response.close();
        return new KMetaProject(project, catalogs);
    }

    //一个Catalog下有若干个Schema,每一个Schema下又有若干个表
    private List<KMetaCatalog> convertMetaCatalogs(List<KMetaSchema> schemas) {
        Map<String, List<KMetaSchema>> catalogMap = new LinkedHashMap<String, List<KMetaSchema>>();
        for (KMetaSchema schema : schemas) {
            List<KMetaSchema> list = catalogMap.get(schema.tableCatalog);
            if (list == null) {
                list = new ArrayList<KMetaSchema>();
                catalogMap.put(schema.tableCatalog, list);
            }
            list.add(schema);
        }

        List<KMetaCatalog> result = new ArrayList<KMetaCatalog>();
        for (List<KMetaSchema> catSchemas : catalogMap.values()) {
            String catalog = catSchemas.get(0).tableCatalog;
            result.add(new KMetaCatalog(catalog, catSchemas));
        }
        return result;
    }

    //一个Catalog+Schema 下有若干个table,因此填写该映射
    private List<KMetaSchema> convertMetaSchemas(List<KMetaTable> tables) {
        Map<String, List<KMetaTable>> schemaMap = new LinkedHashMap<String, List<KMetaTable>>();
        for (KMetaTable table : tables) {
            String key = table.tableCat + "!!" + table.tableSchem;
            List<KMetaTable> list = schemaMap.get(key);
            if (list == null) {
                list = new ArrayList<KMetaTable>();
                schemaMap.put(key, list);
            }
            list.add(table);
        }

        List<KMetaSchema> result = new ArrayList<KMetaSchema>();
        for (List<KMetaTable> schemaTables : schemaMap.values()) {
            String catalog = schemaTables.get(0).tableCat;
            String schema = schemaTables.get(0).tableSchem;
            result.add(new KMetaSchema(catalog, schema, schemaTables));
        }
        return result;
    }

    //先转换成表对象
    private List<KMetaTable> convertMetaTables(List<TableMetaStub> tableMetaStubs) {
        List<KMetaTable> result = new ArrayList<KMetaTable>(tableMetaStubs.size());//表的数量
        for (TableMetaStub tableStub : tableMetaStubs) {
            result.add(convertMetaTable(tableStub));//转换成表对象
        }
        return result;
    }

    //一个表对象的转换
    private KMetaTable convertMetaTable(TableMetaStub tableStub) {
        List<KMetaColumn> columns = new ArrayList<KMetaColumn>(tableStub.getColumns().size());//多少列
        for (ColumnMetaStub columnStub : tableStub.getColumns()) {
            columns.add(convertMetaColumn(columnStub));
        }
        return new KMetaTable(tableStub.getTABLE_CAT(), tableStub.getTABLE_SCHEM(), tableStub.getTABLE_NAME(), tableStub.getTABLE_TYPE(), columns);
    }

    //转换成列对象
    private KMetaColumn convertMetaColumn(ColumnMetaStub columnStub) {
        return new KMetaColumn(columnStub.getTABLE_CAT(), columnStub.getTABLE_SCHEM(), columnStub.getTABLE_NAME(), columnStub.getCOLUMN_NAME(), columnStub.getDATA_TYPE(), columnStub.getTYPE_NAME(), columnStub.getCOLUMN_SIZE(), columnStub.getDECIMAL_DIGITS(), columnStub.getNUM_PREC_RADIX(), columnStub.getNULLABLE(), columnStub.getCHAR_OCTET_LENGTH(), columnStub.getORDINAL_POSITION(), columnStub.getIS_NULLABLE());
    }

    //执行查询

    /**
     * @param  params 表示预编译sql中?的位置
     * @param  paramValues 表示预编译sql中?对应的值
     */
    @Override
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues) throws IOException {

        SQLResponseStub queryResp = executeKylinQuery(sql, convertParameters(params, paramValues));//真正的执行sql,到远程kylin节点去查询结果
        if (queryResp.getIsException())
            throw new IOException(queryResp.getExceptionMessage());

        List<ColumnMetaData> metas = convertColumnMeta(queryResp);
        List<Object> data = convertResultData(queryResp, metas);

        return new QueryResult(metas, data);
    }

    //用于预编译sql中每一个value值所属class类型与对应的value映射
    private List<StatementParameter> convertParameters(List<AvaticaParameter> params, List<Object> paramValues) {
        if (params == null || params.isEmpty())
            return null;

        assert params.size() == paramValues.size();

        //用于预编译sql中每一个value值所属class类型与对应的value映射
        List<StatementParameter> result = new ArrayList<StatementParameter>();
        for (Object v : paramValues) {//循环每一个具体的值
            result.add(new StatementParameter(v.getClass().getCanonicalName(), String.valueOf(v)));
        }
        return result;
    }

    //发送sql请求
    private SQLResponseStub executeKylinQuery(String sql, List<StatementParameter> params) throws IOException {
        String url = baseUrl() + "/kylin/api/query";
        String project = conn.getProject();

        QueryRequest request = null;
        if (null != params) {
            request = new PreparedQueryRequest();
            ((PreparedQueryRequest) request).setParams(params);//设置预编译的sql请求
            url += "/prestate"; // means prepared statement..
        } else {
            request = new QueryRequest();
        }
        request.setSql(sql);
        request.setProject(project);

        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);

        String postBody = jsonMapper.writeValueAsString(request);
        logger.debug("Post body:\n " + postBody);
        StringEntity requestEntity = new StringEntity(postBody, ContentType.create("application/json", "UTF-8"));
        post.setEntity(requestEntity);

        CloseableHttpResponse response = httpClient.execute(post);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(post, response);
        }

        SQLResponseStub stub = jsonMapper.readValue(response.getEntity().getContent(), SQLResponseStub.class);
        response.close();
        return stub;
    }

    //将查询结果转换成列对象,即列的元数据
    private List<ColumnMetaData> convertColumnMeta(SQLResponseStub queryResp) {
        List<ColumnMetaData> metas = new ArrayList<ColumnMetaData>();
        for (int i = 0; i < queryResp.getColumnMetas().size(); i++) {
            SQLResponseStub.ColumnMetaStub scm = queryResp.getColumnMetas().get(i);
            Class columnClass = convertType(scm.getColumnType());//该列类型对应的java对象
            ScalarType type = ColumnMetaData.scalar(scm.getColumnType(), scm.getColumnTypeName(), Rep.of(columnClass));//列类型对应的java的class

            //描述列对象
            ColumnMetaData meta = new ColumnMetaData(i, scm.isAutoIncrement(), scm.isCaseSensitive(), scm.isSearchable(), scm.isCurrency(), scm.getIsNullable(), scm.isSigned(), scm.getDisplaySize(), scm.getLabel(), scm.getName(), scm.getSchemaName(), scm.getPrecision(), scm.getScale(), scm.getTableName(), scm.getSchemaName(), type, scm.isReadOnly(), scm.isWritable(), scm.isWritable(), columnClass.getCanonicalName());

            metas.add(meta);
        }

        return metas;
    }

    //查询结果以及列对象的详细信息---返回查询结果对象集合,集合内元素因为字段类型不一样,转换后的类型也不一样,因此是Object数组
    private List<Object> convertResultData(SQLResponseStub queryResp, List<ColumnMetaData> metas) {
        List<String[]> stringResults = queryResp.getResults();
        List<Object> data = new ArrayList<Object>(stringResults.size());//查询结果的size
        for (String[] result : stringResults) {//循环每一个查询结果
            Object[] row = new Object[result.length];//每一个列对应的String转换成java的class对象

            for (int i = 0; i < result.length; i++) {//循环每一个列对应的值
                ColumnMetaData meta = metas.get(i);
                row[i] = wrapObject(result[i], meta.type.id);//对类型的值进行转换
            }

            data.add(row);
        }
        return (List<Object>) data;
    }

    private IOException asIOException(HttpRequestBase request, HttpResponse response) throws IOException {
        return new IOException(request.getMethod() + " failed, error code " + response.getStatusLine().getStatusCode() + " and response: " + EntityUtils.toString(response.getEntity()));
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
