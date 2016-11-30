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

package org.apache.kylin.job.execution;

import java.util.Map;

import org.apache.kylin.job.exception.ExecuteException;

/**
 * 表示每一个要执行的任务job
 */
public interface Executable {

    String getId();//任务的唯一ID

    String getName();//任务的名字

    ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException;//去执行一个job,传递一个上下文对象,返回执行结果对象

    ExecutableState getStatus();//job运行中的状态机

    Output getOutput();//任务的输出对象

    boolean isRunnable();//任务是否在运行中

    Map<String, String> getParams();//任务的执行参数集合
}
