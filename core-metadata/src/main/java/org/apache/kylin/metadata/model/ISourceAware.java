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

package org.apache.kylin.metadata.model;

//数据源类型

/**
 * 主要解释了数据源是什么,使用什么引擎进行builder,如何存储最终结果
 * 该类主要解决的是数据源的问题
 */
public interface ISourceAware {

    public static final int ID_HIVE = 0;//来源于hive
    public static final int ID_STREAMING = 1;//streaming,比如kafka
    public static final int ID_SPARKSQL = 5;//来源于spark sql
    public static final int ID_EXTERNAL = 7;//来自于外部存储,存储在hdfs上或者hbase上

    int getSourceType();
}
