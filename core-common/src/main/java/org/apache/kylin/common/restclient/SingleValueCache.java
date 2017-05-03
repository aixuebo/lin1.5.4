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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;

/**
 * @author xjiang
 */
public abstract class SingleValueCache<K, V> extends AbstractRestCache<K, V> {

    private final ConcurrentMap<K, V> innerCache;//内部缓存,put的时候都存储到缓存里面

    public SingleValueCache(KylinConfig config, Broadcaster.TYPE syncType) {
        this(config, syncType, new ConcurrentHashMap<K, V>());
    }

    public SingleValueCache(KylinConfig config, Broadcaster.TYPE syncType, ConcurrentMap<K, V> innerCache) {
        super(config, syncType);
        this.innerCache = innerCache;
    }

    //不仅仅存储缓存,还要向广播推送数据
    public void put(K key, V value) {
        boolean exists = innerCache.containsKey(key);

        innerCache.put(key, value);

        if (!exists) {//判断是否是创建还是更新
            getBroadcaster().queue(syncType.getType(), Broadcaster.EVENT.CREATE.getType(), key.toString());//加入队列去分发到其他节点
        } else {
            getBroadcaster().queue(syncType.getType(), Broadcaster.EVENT.UPDATE.getType(), key.toString());
        }
    }

    //只是存储在缓存里面
    public void putLocal(K key, V value) {
        innerCache.put(key, value);
    }

    public void remove(K key) {
        boolean exists = innerCache.containsKey(key);

        innerCache.remove(key);

        if (exists) {//分发到其他节点去做同步删除
            getBroadcaster().queue(syncType.getType(), Broadcaster.EVENT.DROP.getType(), key.toString());
        }
    }

    //只是在本地节点删除
    public void removeLocal(K key) {
        innerCache.remove(key);
    }

    public void clear() {
        innerCache.clear();
    }

    public int size() {
        return innerCache.size();
    }

    public V get(K key) {
        return innerCache.get(key);
    }

    public Collection<V> values() {
        return innerCache.values();
    }

    public boolean containsKey(String key) {
        return innerCache.containsKey(key);
    }

    public Map<K, V> getMap() {
        return Collections.unmodifiableMap(innerCache);
    }

    public Set<K> keySet() {
        return innerCache.keySet();
    }
}
