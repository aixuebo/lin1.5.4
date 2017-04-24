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

package org.apache.kylin.query.routing.rules;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RoutingRule;

import com.google.common.collect.Sets;

/**
 * for IT use, exclude some cubes
 * 在黑名单的一定会被删除
 * 只保留报名单内的数据
 *
 * 因此当白和黑名单都有同一个数据的时候,该数据因为在黑名单存在,则一定会被删除
 */
public class RemoveBlackoutRealizationsRule extends RoutingRule {
    public static Set<String> blackList = Sets.newHashSet();//黑名单
    public static Set<String> whiteList = Sets.newHashSet();//白名单

    @Override
    public void apply(List<Candidate> candidates) {
        for (Iterator<Candidate> iterator = candidates.iterator(); iterator.hasNext();) {
            Candidate candidate = iterator.next();

            if (blackList.contains(candidate.getRealization().getCanonicalName())) {//黑名单的要被删除
                iterator.remove();
                continue;
            }

            if (!whiteList.isEmpty() && !whiteList.contains(candidate.getRealization().getCanonicalName())) {//不在白名单的,都要被删除
                iterator.remove();
                continue;
            }
        }
    }

}
