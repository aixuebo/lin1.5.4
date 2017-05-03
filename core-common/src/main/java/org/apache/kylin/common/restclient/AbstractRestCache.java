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

import org.apache.kylin.common.KylinConfig;

/**
 * @author xjiang
 * 
 */
public abstract class AbstractRestCache<K, V> {

    protected final KylinConfig config;
    protected final Broadcaster.TYPE syncType;

    protected AbstractRestCache(KylinConfig config, Broadcaster.TYPE syncType) {
        this.config = config;
        this.syncType = syncType;
    }

    public Broadcaster getBroadcaster() {
        return Broadcaster.getInstance(config);
    }

    //存储本地节点,并且也分发给其他节点
    public abstract void put(K key, V value);

    //只是存储在本地节点上,不去分发
    //本地方法是用于接收到远端发过来的同步请求时候使用,因为是接收同步信息,因此如果再分发,则会形成无限循环了
    public abstract void putLocal(K key, V value);

    //删除该key,并且分发到其他节点去做同步删除
    public abstract void remove(K key);

    //只是在本地节点删除该key
    public abstract void removeLocal(K key);

    //清空内部缓存
    public abstract void clear();

    public abstract int size();
}
