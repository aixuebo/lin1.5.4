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

package org.apache.kylin.cube.model.validation;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.rule.AggregationGroupRule;
import org.apache.kylin.cube.model.validation.rule.DictionaryRule;
import org.apache.kylin.cube.model.validation.rule.FunctionRule;
import org.apache.kylin.cube.model.validation.rule.RowKeyAttrRule;

/**
 * For cube metadata validator
 * 对cube的描述信息进行校验
 * @author jianliu
 * 
 */
public class CubeMetadataValidator {
    //校验规则集合
    @SuppressWarnings("unchecked")
    private IValidatorRule<CubeDesc>[] rules = new IValidatorRule[] { new FunctionRule(), new AggregationGroupRule(), new RowKeyAttrRule(), new DictionaryRule() };

    public ValidateContext validate(CubeDesc cube) {
        return validate(cube, false);
    }

    /**
     * @param inject    inject error into cube desc,true表示对cube的描述内容进行反馈,将错误信息反馈给cube
     * @return
     */
    public ValidateContext validate(CubeDesc cube, boolean inject) {
        ValidateContext context = new ValidateContext();//创建校验的上下文
        for (int i = 0; i < rules.length; i++) {//循环每一个规则
            IValidatorRule<CubeDesc> rule = rules[i];//对每一个规则进行校验
            rule.validate(cube, context);
        }
        if (inject) {
            injectResult(cube, context);
        }
        return context;
    }

    /**
     * 
     * Inject errors info into cubeDesc
     * 如果出现错误,则对其进行输出到cube描述中
     * 
     * @param cubeDesc
     * @param context
     */
    public void injectResult(CubeDesc cubeDesc, ValidateContext context) {
        ValidateContext.Result[] results = context.getResults();//获取错误信息
        for (int i = 0; i < results.length; i++) {
            ValidateContext.Result result = results[i];
            cubeDesc.addError(result.getLevel() + " : " + result.getMessage(), true);//true表示出现错误后要抛出异常
        }

    }

}
