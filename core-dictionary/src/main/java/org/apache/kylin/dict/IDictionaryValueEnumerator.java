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

package org.apache.kylin.dict;

import java.io.IOException;

/**
 * Created by dongli on 10/28/15.
 * 读取table的某一个列
 * 实现类是org.apache.kylin.dict.TableColumnValueEnumerator 读取一个文件
 * 实现类是org.apache.kylin.dict.MultipleDictionaryValueEnumerator 读取多个文件
 * 实现类是org.apache.kylin.dict.IterableDictionaryValueEnumerator 从一个list的迭代器中读取字典的内容
 */
public interface IDictionaryValueEnumerator {
    byte[] current() throws IOException;//当前行数据内容

    boolean moveNext() throws IOException;//获取下一行该列的数据

    void close() throws IOException;
}
