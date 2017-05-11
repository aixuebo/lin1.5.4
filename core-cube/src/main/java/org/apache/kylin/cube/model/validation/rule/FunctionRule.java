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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;

/**
 * Validate function parameter.
 * <p/>
 * if type is column, check values are valid fact table columns if type is
 * constant, the value only can be numberic
 * <p/>
 * the return type only can be int/bigint/long/double/decimal
 * 对函数进行校验
 * 1.校验函数的参数值是常数时候必须是int,是字段的时候,必须是fact表字段
 */
public class FunctionRule implements IValidatorRule<CubeDesc> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.kylin.metadata.validation.IValidatorRule#validate(java.lang.Object
     * , org.apache.kylin.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        List<MeasureDesc> measures = cube.getMeasures();

        if (validateMeasureNamesDuplicated(measures, context)) {//校验度量name不允许重复
            return;
        }

        List<FunctionDesc> countFuncs = new ArrayList<FunctionDesc>();

        Iterator<MeasureDesc> it = measures.iterator();
        while (it.hasNext()) {
            MeasureDesc measure = it.next();
            FunctionDesc func = measure.getFunction();
            ParameterDesc parameter = func.getParameter();
            if (parameter == null) {//参数必须存在
                context.addResult(ResultLevel.ERROR, "Must define parameter for function " + func.getExpression() + " in " + measure.getName());
                return;
            }

            //参数必须有类型以及值 还有返回值
            String type = func.getParameter().getType();
            String value = func.getParameter().getValue();
            if (StringUtils.isEmpty(type)) {
                context.addResult(ResultLevel.ERROR, "Must define type for parameter type " + func.getExpression() + " in " + measure.getName());
                return;
            }
            if (StringUtils.isEmpty(value)) {
                context.addResult(ResultLevel.ERROR, "Must define type for parameter value " + func.getExpression() + " in " + measure.getName());
                return;
            }
            if (StringUtils.isEmpty(func.getReturnType())) {
                context.addResult(ResultLevel.ERROR, "Must define return type for function " + func.getExpression() + " in " + measure.getName());
                return;
            }

            if (StringUtils.equalsIgnoreCase(FunctionDesc.PARAMETER_TYPE_COLUMN, type)) {//column
                validateColumnParameter(context, cube, value);//列必须是fact表中的列
            } else if (StringUtils.equals(FunctionDesc.PARAMETER_TYPE_CONSTANT, type)) {//constant
                validateCostantParameter(context, cube, value);//常数必须是整数
            }

            try {
                func.getMeasureType().validate(func);//校验该函数表达式是否合法
            } catch (IllegalArgumentException ex) {
                context.addResult(ResultLevel.ERROR, ex.getMessage());
            }

            if (func.isCount())
                countFuncs.add(func);

            if (TopNMeasureType.FUNC_TOP_N.equalsIgnoreCase(func.getExpression())) {//TOP_N函数
                if (parameter.getNextParameter() == null) {
                    context.addResult(ResultLevel.ERROR, "Must define at least 2 parameters for function " + func.getExpression() + " in " + measure.getName());
                    return;
                }

                ParameterDesc groupByCol = parameter.getNextParameter();
                List<String> duplicatedCol = Lists.newArrayList();//参与group by的维度列集合
                while (groupByCol != null) {
                    String embeded_groupby = groupByCol.getValue();
                    for (DimensionDesc dimensionDesc : cube.getDimensions()) {
                        if (dimensionDesc.getColumn() != null && dimensionDesc.getColumn().equalsIgnoreCase(embeded_groupby)) {
                            duplicatedCol.add(embeded_groupby);
                        }
                    }
                    groupByCol = groupByCol.getNextParameter();
                }

                if (duplicatedCol.size() > 0) {
                    context.addResult(ResultLevel.ERROR, "Couldn't use " + duplicatedCol.toString() + " in Top-N as it is already defined as dimension.");
                    return;

                }

            }
        }

        if (countFuncs.size() != 1) {//必须有一个count函数
            context.addResult(ResultLevel.ERROR, "Must define one and only one count(1) function, but there are " + countFuncs.size() + " -- " + countFuncs);
        }
    }

    /**
     * 校验参数值必须是数字
     * @param context
     * @param cube
     * @param value
     */
    private void validateCostantParameter(ValidateContext context, CubeDesc cube, String value) {
        try {
            Integer.parseInt(value);
        } catch (Exception e) {
            context.addResult(ResultLevel.ERROR, "Parameter value must be number, but it is " + value);
        }
    }

    /**
     * @param context
     * @param cube
     * @param value 列集合,用逗号拆分,该列都是来自于fact表
     * 校验value中列必须来自于fact表,否则会添加错误日志
     */
    private void validateColumnParameter(ValidateContext context, CubeDesc cube, String value) {
        String factTable = cube.getFactTable();
        if (StringUtils.isEmpty(factTable)) {//必须有fact表
            context.addResult(ResultLevel.ERROR, "Fact table can not be null.");
            return;
        }
        TableDesc table = MetadataManager.getInstance(cube.getConfig()).getTableDesc(factTable);//fact表对象
        if (table == null) {//fact表必须存在
            context.addResult(ResultLevel.ERROR, "Fact table can not be found: " + cube);
            return;
        }
        // Prepare column set
        Set<String> set = new HashSet<String>();
        ColumnDesc[] cdesc = table.getColumns();//fact表的列集合
        for (int i = 0; i < cdesc.length; i++) {
            ColumnDesc columnDesc = cdesc[i];
            set.add(columnDesc.getName());
        }

        String[] items = value.split(",");
        for (int i = 0; i < items.length; i++) {
            String item = items[i].trim();
            if (StringUtils.isEmpty(item)) {
                continue;
            }
            if (!set.contains(item)) {
                context.addResult(ResultLevel.ERROR, "Column [" + item + "] does not exist in factable table" + factTable);
            }
        }

    }

    /**
     * @param measures
     * 校验度量的名字是有重复的
     * true表示有重复名字存在
     */
    private boolean validateMeasureNamesDuplicated(List<MeasureDesc> measures, ValidateContext context) {
        Set<String> nameSet = new HashSet<>();
        for (MeasureDesc measure: measures){
            if (nameSet.contains(measure.getName())){
                context.addResult(ResultLevel.ERROR, "There is duplicated measure's name: " + measure.getName());
                return true;
            } else {
                nameSet.add(measure.getName());
            }
        }
        return false;
    }
}
