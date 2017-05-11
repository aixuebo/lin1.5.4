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

package org.apache.kylin.cube.model.validation.rule;

import java.util.HashMap;
import java.util.List;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by sunyerui on 16/6/1.
 * 校验字典
 */
public class DictionaryRule implements IValidatorRule<CubeDesc> {

    @Override
    public void validate(CubeDesc cubeDesc, ValidateContext context) {
        List<DictionaryDesc> dictDescs = cubeDesc.getDictionaries();
        if (dictDescs == null || dictDescs.isEmpty()) {
            return;
        }

        HashMap<TblColRef, String> colToBuilderMap = new HashMap<>();//每一个列使用哪个字典class
        HashMap<TblColRef, TblColRef> colToReuseColMap = new HashMap<>();//每一个列使用哪个resuse列
        for (DictionaryDesc dictDesc : dictDescs) {//循环每一个字典
            TblColRef dictCol = dictDesc.getColumnRef();//需要字典的列
            if (dictCol == null) {
                context.addResult(ResultLevel.ERROR, "Some column in dictionaries not found");
                return;
            }
            String builder = dictDesc.getBuilderClass();
            TblColRef reuseCol = dictDesc.getResuseColumnRef();//与哪个列同用一个字典
            if (reuseCol == null) {//如果没有选择设置Resuse,则必须设置builder
                if (builder == null || builder.isEmpty()) {
                    context.addResult(ResultLevel.ERROR, "Column " + dictCol + " cannot have builder and reuse column both empty");
                    return;
                }
                
                // Make sure the same column associate with same builder class
                String oldBuilder = colToBuilderMap.put(dictCol, builder);
                if (oldBuilder != null && !oldBuilder.equals(builder)) {//说明字典不同,修改的时候容易出现该错误,即不允许更换字典
                    context.addResult(ResultLevel.ERROR, "Column " + dictCol + " has inconsistent builders " + builder + " and " + oldBuilder);
                    return;
                }
            } else {
                if (builder != null && !builder.isEmpty()) {//不允许定义builder
                    context.addResult(ResultLevel.ERROR, "Column " + dictCol + " cannot have builder and reuse column both");
                    return;
                }
                
                // Make sure one column only reuse another one column
                TblColRef oldReuseCol = colToReuseColMap.put(dictCol, reuseCol);//修改时候容易出现该错误,不允许更换字典列
                if (oldReuseCol != null && !reuseCol.equals(oldReuseCol)) {
                    context.addResult(ResultLevel.ERROR, "Column " + dictCol + " reuse inconsistent column " + reuseCol + " and " + oldReuseCol);
                    return;
                }
            }
        }
    }
}
