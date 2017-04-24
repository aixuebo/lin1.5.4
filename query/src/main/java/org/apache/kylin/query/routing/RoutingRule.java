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

import java.util.Iterator;
import java.util.List;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.rules.RealizationSortRule;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.query.routing.rules.RemoveUncapableRealizationsRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public abstract class RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);
    private static List<RoutingRule> rules = Lists.newLinkedList();

    //添加顺序
    static {
        rules.add(new RemoveBlackoutRealizationsRule());
        rules.add(new RemoveUncapableRealizationsRule());
        rules.add(new RealizationSortRule());
    }

    //分别应用每一个规则
    public static void applyRules(List<Candidate> candidates) {
        for (RoutingRule rule : rules) {
            String before = getPrintableText(candidates);//实现前
            rule.apply(candidates);
            String after = getPrintableText(candidates);//实现后
            logger.info("Applying rule: " + rule + ", realizations before: " + before + ", realizations after: " + after);
        }
    }

    public static String getPrintableText(List<Candidate> candidates) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        for (Candidate candidate : candidates) {
            IRealization r = candidate.realization;
            sb.append(r.getName());
            sb.append("(");
            sb.append(r.getType());
            sb.append(")");
            sb.append(",");
        }
        if (sb.charAt(sb.length() - 1) != '[')
            sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    /**
     *
     * @param rule 要插入的规则
     * @param applyOrder RoutingRule are applied in order, latter rules can override previous rules 该规则是第几个执行
     */
    public static void registerRule(RoutingRule rule, int applyOrder) {
        if (applyOrder > rules.size()) {//说明执行顺序比现在的规则数量还大,因此直接追加即可
            logger.warn("apply order " + applyOrder + "  is larger than rules size " + rules.size() + ", will put the new rule at the end");
            rules.add(rule);
        }

        rules.add(applyOrder, rule);//插入到指定顺序位置上
    }

    //删除参数对应的规则
    public static void removeRule(RoutingRule rule) {
        for (Iterator<RoutingRule> iter = rules.iterator(); iter.hasNext();) {
            RoutingRule r = iter.next();
            if (r.getClass() == rule.getClass()) {
                iter.remove();
            }
        }
    }

    //找到符合type类型的序号
    protected List<Integer> findRealizationsOf(List<IRealization> realizations, RealizationType type) {
        List<Integer> itemIndexes = Lists.newArrayList();
        for (int i = 0; i < realizations.size(); ++i) {
            if (realizations.get(i).getType() == type) {
                itemIndexes.add(i);
            }
        }
        return itemIndexes;
    }

    @Override
    public String toString() {
        return this.getClass().toString();
    }

    //在候选人集合上应用该规则
    public abstract void apply(List<Candidate> candidates);

}
