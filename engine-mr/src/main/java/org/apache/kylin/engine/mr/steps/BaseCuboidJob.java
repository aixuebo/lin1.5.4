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

package org.apache.kylin.engine.mr.steps;

import org.apache.hadoop.util.ToolRunner;

/**
 * @author honma
 * 对基本的cuboid进行处理---设置rowkey为需要的全部字段,设置value为需要的全部度量的值
 */
public class BaseCuboidJob extends CuboidJob {

    public BaseCuboidJob() {
        this.setMapperClass(HiveToBaseCuboidMapper.class);
    }

    public static void main(String[] args) throws Exception {
        CuboidJob job = new BaseCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
