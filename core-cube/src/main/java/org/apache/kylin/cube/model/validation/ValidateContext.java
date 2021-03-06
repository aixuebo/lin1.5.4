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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Context. Supply all dependent objects for validator
 * 校验上下文,用于存储所有的校验过程中出现的问题
 */
public class ValidateContext {
    private List<Result> results = new ArrayList<ValidateContext.Result>();

    public void addResult(ResultLevel level, String message) {
        results.add(new Result(level, message));
    }

    public void addResult(Result result) {
        results.add(result);
    }

    public class Result {
        private ResultLevel level;
        private String message;

        /**
         * @param level
         * @param message
         */
        public Result(ResultLevel level, String message) {
            this.level = level;
            this.message = message;
        }

        /**
         * @return the level
         */
        public ResultLevel getLevel() {
            return level;
        }

        /**
         * @return the message
         */
        public String getMessage() {
            return message;
        }
    }

    /**
     * Get validation result
     * 输出错误信息
     * @return
     */
    public Result[] getResults() {
        Result[] rs = new Result[0];
        rs = results.toArray(rs);
        return rs;
    }

    /**
     * 打印错误信息
     */
    public void print(PrintStream out) {
        if (results.isEmpty()) {
            out.println("The element is perfect.");//说明没有错误,是完美的
        }
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            out.println(result.level + " : " + result.message);
        }
    }

    /**
     * @return if there is not validation errors
     * true表示校验通过,没有异常
     */
    public boolean ifPass() {
        return results.isEmpty();
    }

}
