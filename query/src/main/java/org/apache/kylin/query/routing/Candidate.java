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

package org.apache.kylin.query.routing;

import java.util.Collections;
import java.util.Map;

import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.Maps;

/**
 * 候选人对象,支持排序功能
 */
public class Candidate implements Comparable<Candidate> {

    static Map<RealizationType, Integer> DEFAULT_PRIORITIES = Maps.newHashMap();//默认的候选人优先级
    static Map<RealizationType, Integer> PRIORITIES = DEFAULT_PRIORITIES;

    static {
        DEFAULT_PRIORITIES.put(RealizationType.HYBRID, 0);
        DEFAULT_PRIORITIES.put(RealizationType.CUBE, 1);
        DEFAULT_PRIORITIES.put(RealizationType.INVERTED_INDEX, 1);
    }

    /** for test only */
    public static void setPriorities(Map<RealizationType, Integer> priorities) {
        PRIORITIES = Collections.unmodifiableMap(priorities);
    }

    /** for test only */
    public static void restorePriorities() {
        PRIORITIES = Collections.unmodifiableMap(DEFAULT_PRIORITIES);
    }

    // ============================================================================

    IRealization realization;
    SQLDigest sqlDigest;
    int priority;
    CapabilityResult capability;

    public Candidate(IRealization realization, SQLDigest sqlDigest) {
        this.realization = realization;
        this.sqlDigest = sqlDigest;
        this.priority = PRIORITIES.get(realization.getType());
    }

    public IRealization getRealization() {
        return realization;
    }

    public SQLDigest getSqlDigest() {
        return sqlDigest;
    }

    public int getPriority() {
        return priority;
    }

    public CapabilityResult getCapability() {
        return capability;
    }

    public void setCapability(CapabilityResult capability) {
        this.capability = capability;
    }

    //先比较优先级,在比较价值cost
    @Override
    public int compareTo(Candidate o) {
        int comp = this.priority - o.priority;
        if (comp != 0) {
            return comp;
        }

        comp = this.capability.cost - o.capability.cost;
        if (comp != 0) {
            return comp;
        }

        return 0;
    }

    @Override
    public String toString() {
        return realization.toString();
    }
}
