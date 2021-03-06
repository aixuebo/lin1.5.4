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

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;

/**
 * 校验rowkey列必须存在
 */
public class RowKeyAttrRule implements IValidatorRule<CubeDesc> {

    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        RowKeyDesc row = cube.getRowkey();
        if (row == null) {
            context.addResult(ResultLevel.ERROR, "Rowkey does not exist");
            return;
        }

        RowKeyColDesc[] rcd = row.getRowKeyColumns();
        if (rcd == null || rcd.length == 0) {
            context.addResult(ResultLevel.ERROR, "Rowkey columns do not exist");
            return;
        }

        for (int i = 0; i < rcd.length; i++) {
            RowKeyColDesc rd = rcd[i];
            if (rd.getColumn() == null || rd.getColumn().length() == 0) {
                context.addResult(ResultLevel.ERROR, "Rowkey column empty");
            }

        }

    }
}
