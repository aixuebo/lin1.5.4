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
package org.apache.kylin.common.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Provide switch between different implementations of a same interface.
 * Each implementation is identified by an integer ID.
 * 提供若干种不同的实现方式,每一个实现方式使用int进行切换
 */
public class ImplementationSwitch<I> {

    private static final Logger logger = LoggerFactory.getLogger(ImplementationSwitch.class);

    final private Object[] instances;//具体实现类
    private Class<I> interfaceClz;//该实现是实现了哪个接口
    private Map<Integer, String> impls = Maps.newHashMap();//每一个实现提供一个int和class全路径的映射

    public ImplementationSwitch(Map<Integer, String> impls, Class<I> interfaceClz) {
        this.impls.putAll(impls);
        this.interfaceClz = interfaceClz;
        this.instances = initInstances(this.impls);
    }

    private Object[] initInstances(Map<Integer, String> impls) {
        int maxId = 0;
        for (Integer id : impls.keySet()) {
            maxId = Math.max(maxId, id);
        }
        if (maxId > 100)
            throw new IllegalArgumentException("you have more than 100 implementations?");//不可能有100中实现

        Object[] result = new Object[maxId + 1];//创建数组,用于日后存储若干个实现类

        return result;
    }

    public synchronized I get(int id) {
        String clzName = impls.get(id);//找到对应的class
        if (clzName == null) {
            throw new IllegalArgumentException("Implementation class missing, ID " + id + ", interface " + interfaceClz.getName());
        }

        @SuppressWarnings("unchecked")
        I result = (I) instances[id];//看是否已经实例化了

        if (result == null) {
            try {
                result = (I) ClassUtil.newInstance(clzName);//实例化并且添加到缓存中
                instances[id] = result;
            } catch (Exception ex) {
                logger.warn("Implementation missing " + clzName + " - " + ex);
            }
        }

        if (result == null)
            throw new IllegalArgumentException("Implementations missing, ID " + id + ", interface " + interfaceClz.getName());

        return result;
    }
}
