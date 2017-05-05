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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.security.AclHBaseStorage;
import org.apache.kylin.rest.util.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 00003a15-ced4-4c33-ad3c-0d6a013bcee0                 column=i:i, timestamp=1491789607694, value=\xFF
 00003a15-ced4-4c33-ad3c-0d6a013bcee0                 column=i:o, timestamp=1491789607694, value={"sid":"ADMIN","principal":true}
 00003a15-ced4-4c33-ad3c-0d6a013bcee0                 column=i:p, timestamp=1491789607701, value={"id":"39c6ffc5-a8ef-4d16-8bce-d7eef092626b","type":"org.apache.kylin.cube.CubeInstance"}
 00003a15-ced4-4c33-ad3c-0d6a013bcee0                 column=i:t, timestamp=1491789607694, value=org.apache.kylin.job.JobInstance
 001a38f1-aca1-4e52-8fdb-682403f9e447                 column=i:i, timestamp=1493952126046, value=\xFF
 001a38f1-aca1-4e52-8fdb-682403f9e447                 column=i:o, timestamp=1493952126046, value={"sid":"ADMIN","principal":true}
 001a38f1-aca1-4e52-8fdb-682403f9e447                 column=i:p, timestamp=1493952126055, value={"id":"0ce1aa37-abc8-4029-9ff6-600919964b31","type":"org.apache.kylin.cube.CubeInstance"}
 001a38f1-aca1-4e52-8fdb-682403f9e447                 column=i:t, timestamp=1493952126046, value=org.apache.kylin.job.JobInstance
 00e5a4e7-f750-4bea-8792-691d04fdf21a                 column=i:i, timestamp=1486690204093, value=\xFF
 00e5a4e7-f750-4bea-8792-691d04fdf21a                 column=i:o, timestamp=1486690204093, value={"sid":"ADMIN","principal":true}
 00e5a4e7-f750-4bea-8792-691d04fdf21a                 column=i:p, timestamp=1486690204100, value={"id":"f40d613f-b3f5-40d7-a9b9-8262a75a1df8","type":"org.apache.kylin.cube.CubeInstance"}
 00e5a4e7-f750-4bea-8792-691d04fdf21a                 column=i:t, timestamp=1486690204093, value=org.apache.kylin.job.JobInstance
 0120a519-c200-4249-bd51-67ed05453658                 column=i:i, timestamp=1493952094822, value=\xFF
 0120a519-c200-4249-bd51-67ed05453658                 column=i:o, timestamp=1493952094822, value={"sid":"ADMIN","principal":true}
 0120a519-c200-4249-bd51-67ed05453658                 column=i:p, timestamp=1493952094832, value={"id":"39c6ffc5-a8ef-4d16-8bce-d7eef092626b","type":"org.apache.kylin.cube.CubeInstance"}
 0120a519-c200-4249-bd51-67ed05453658                 column=i:t, timestamp=1493952094822, value=org.apache.kylin.job.JobInstance
 02359ba9-fef7-461c-b2e1-1e1d1db54e5e                 column=i:i, timestamp=1490751147397, value=\xFF
 02359ba9-fef7-461c-b2e1-1e1d1db54e5e                 column=i:o, timestamp=1490751147397, value={"sid":"ADMIN","principal":true}
 02359ba9-fef7-461c-b2e1-1e1d1db54e5e                 column=i:p, timestamp=1490751147405, value={"id":"1e850529-2fc8-401f-8576-bfabe8e82277","type":"org.apache.kylin.cube.CubeInstance"}
 02359ba9-fef7-461c-b2e1-1e1d1db54e5e                 column=i:t, timestamp=1490751147397, value=org.apache.kylin.job.JobInstance
 */
@Component("userService")
public class UserService implements UserDetailsManager {

    private static final String PWD_PREFIX = "PWD:";

    private Serializer<UserGrantedAuthority[]> ugaSerializer = new Serializer<UserGrantedAuthority[]>(UserGrantedAuthority[].class);

    private String userTableName = null;

    @Autowired
    protected AclHBaseStorage aclHBaseStorage;

    @PostConstruct
    public void init() throws IOException {
        userTableName = aclHBaseStorage.prepareHBaseTable(UserService.class);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        HTableInterface htable = null;
        try {
            htable = aclHBaseStorage.getTable(userTableName);

            Get get = new Get(Bytes.toBytes(username));//username为rowkey
            get.addFamily(Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_FAMILY));
            Result result = htable.get(get);

            User user = hbaseRowToUser(result);
            if (user == null)
                throw new UsernameNotFoundException("User " + username + " not found.");

            return user;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    //将结果转换成user对象
    private User hbaseRowToUser(Result result) throws JsonParseException, JsonMappingException, IOException {
        if (null == result || result.isEmpty())
            return null;

        String username = Bytes.toString(result.getRow());

        byte[] valueBytes = result.getValue(Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_FAMILY), Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_COLUMN));//获取密码以及其他权限内容
        UserGrantedAuthority[] deserialized = ugaSerializer.deserialize(valueBytes);

        String password = "";
        List<UserGrantedAuthority> authorities = Collections.emptyList();

        // password is stored at [0] of authorities for backward compatibility
        if (deserialized != null) {
            if (deserialized.length > 0 && deserialized[0].getAuthority().startsWith(PWD_PREFIX)) {
                password = deserialized[0].getAuthority().substring(PWD_PREFIX.length());
                authorities = Arrays.asList(deserialized).subList(1, deserialized.length);
            } else {
                authorities = Arrays.asList(deserialized);
            }
        }

        return new User(username, password, authorities);
    }

    private Pair<byte[], byte[]> userToHBaseRow(UserDetails user) throws JsonProcessingException {
        byte[] key = Bytes.toBytes(user.getUsername());

        Collection<? extends GrantedAuthority> authorities = user.getAuthorities();
        if (authorities == null)
            authorities = Collections.emptyList();

        UserGrantedAuthority[] serializing = new UserGrantedAuthority[authorities.size() + 1];

        // password is stored as the [0] authority
        serializing[0] = new UserGrantedAuthority(PWD_PREFIX + user.getPassword());//第一个存储密码
        int i = 1;
        for (GrantedAuthority a : authorities) {
            serializing[i++] = new UserGrantedAuthority(a.getAuthority());
        }

        byte[] value = ugaSerializer.serialize(serializing);
        return Pair.newPair(key, value);
    }

    @Override
    public void createUser(UserDetails user) {
        updateUser(user);
    }

    @Override
    public void updateUser(UserDetails user) {
        HTableInterface htable = null;
        try {
            htable = aclHBaseStorage.getTable(userTableName);

            Pair<byte[], byte[]> pair = userToHBaseRow(user);
            Put put = new Put(pair.getKey());
            put.add(Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_FAMILY), Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_COLUMN), pair.getSecond());

            htable.put(put);
            htable.flushCommits();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public void deleteUser(String username) {
        HTableInterface htable = null;
        try {
            htable = aclHBaseStorage.getTable(userTableName);

            Delete delete = new Delete(Bytes.toBytes(username));

            htable.delete(delete);
            htable.flushCommits();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String username) {
        HTableInterface htable = null;
        try {
            htable = aclHBaseStorage.getTable(userTableName);

            Result result = htable.get(new Get(Bytes.toBytes(username)));

            return null != result && !result.isEmpty();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    public List<String> listUserAuthorities() {
        List<String> all = new ArrayList<String>();
        for (UserDetails user : listUsers()) {
            for (GrantedAuthority auth : user.getAuthorities()) {
                if (!all.contains(auth.getAuthority())) {
                    all.add(auth.getAuthority());
                }
            }
        }
        return all;
    }

    public List<UserDetails> listUsers() {
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_FAMILY), Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_COLUMN));

        List<UserDetails> all = new ArrayList<UserDetails>();
        HTableInterface htable = null;
        ResultScanner scanner = null;
        try {
            htable = aclHBaseStorage.getTable(userTableName);
            scanner = htable.getScanner(s);

            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                User user = hbaseRowToUser(result);
                all.add(user);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan users", e);
        } finally {
            IOUtils.closeQuietly(scanner);
            IOUtils.closeQuietly(htable);
        }
        return all;
    }

    public static class UserGrantedAuthority implements GrantedAuthority {
        private static final long serialVersionUID = -5128905636841891058L;
        private String authority;

        public UserGrantedAuthority() {
        }

        public UserGrantedAuthority(String authority) {
            setAuthority(authority);
        }

        @Override
        public String getAuthority() {
            return authority;
        }

        public void setAuthority(String authority) {
            this.authority = authority;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((authority == null) ? 0 : authority.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            UserGrantedAuthority other = (UserGrantedAuthority) obj;
            if (authority == null) {
                if (other.authority != null)
                    return false;
            } else if (!authority.equals(other.authority))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return authority;
        }
    }

}
