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

package org.apache.kylin.cube.model;

import com.fasterxml.jackson.annotation.JsonProperty;

//同一个维度组里面的列是如何规划的
public class SelectRule {
    @JsonProperty("hierarchy_dims")
    public String[][] hierarchy_dims;//继承关系的列集合,因为一个集合可以添加多组数据,因此是二维数组
    @JsonProperty("mandatory_dims")
    public String[] mandatory_dims;//必须要有的列集合

    //因为一个集合可以添加多组数据,因此是二维数组
    @JsonProperty("joint_dims")
    public String[][] joint_dims;//表示这一组列总是一起被查询,即同时出现,这里面的列集合是作为一组,比如有4个维度,表示的是1个可能,而不是2的4次方可能
}
