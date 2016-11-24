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

package org.apache.kylin.common.restclient;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.util.JsonUtil;

/**
 * @author yangli9
 * rest方式 user:pwd@host:port
 *
 */
public class RestClient {

    protected String host;
    protected int port;
    protected String baseUrl;//http://host:port/kylin/api
    protected String userName;//登录该host的用户名和密码
    protected String password;
    protected CloseableHttpClient client;//客户端对象

    protected static Pattern fullRestPattern = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

    public static boolean matchFullRestPattern(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        return m.matches();
    }

    /**
     * @param uri
     *            "user:pwd@host:port"
     */
    public RestClient(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        if (!m.matches())
            throw new IllegalArgumentException("URI: " + uri + " -- does not match pattern " + fullRestPattern);

        String user = m.group(1);
        String pwd = m.group(2);
        String host = m.group(3);
        String portStr = m.group(4);
        int port = Integer.parseInt(portStr == null ? "7070" : portStr);

        init(host, port, user, pwd);
    }

    //创建一个http对象,发送请求,发送给http://host:port/kylin/api
    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = "http://" + host + ":" + port + "/kylin/api";

        client = HttpClients.createDefault();

        //为请求设置name和密码
        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client = HttpClients.custom().setDefaultCredentialsProvider(provider).build();
        }
    }

    /**
     * 发送请求 http://host:port/kylin/cache/${type}/${name}/${action}
     * 只是检查返回状态码是否是200
     */
    public void wipeCache(String type, String action, String name) throws IOException {
        String url = baseUrl + "/cache/" + type + "/" + name + "/" + action;
        HttpPut request = new HttpPut(url);

        try {
            CloseableHttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());//返回值转换成字符串

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with cache wipe url " + url + "\n" + msg);
            response.close();
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            request.releaseConnection();
        }
    }

    //获取配置信息http://host:port/kylin/api/admin/config---返回json.config的内容
    public String getKylinProperties() throws IOException {
        String url = baseUrl + "/admin/config";
        HttpGet request = new HttpGet(url);
        try {
            CloseableHttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());
            Map<String, String> map = JsonUtil.readValueAsMap(msg);//转换成json对象
            msg = map.get("config");//获取对应的config的内容

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with cache wipe url " + url + "\n" + msg);
            response.close();
            return msg;
        } finally {
            request.releaseConnection();
        }
    }

}
