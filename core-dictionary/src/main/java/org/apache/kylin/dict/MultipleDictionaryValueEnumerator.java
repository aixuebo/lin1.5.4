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
import java.util.List;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 10/28/15.
 * 对多个字典进行merge合并处理,即如何读取多个数据源
 */
@SuppressWarnings("rawtypes")
public class MultipleDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    private int curDictIndex = 0;
    private Dictionary curDict;//当前正在处理哪个字典
    private int curKey;//当前的baseid
    private byte[] curValue = null;//当前这行该列的数据
    private List<Dictionary> dictionaryList;//多个字典

    public MultipleDictionaryValueEnumerator(List<DictionaryInfo> dictionaryInfoList) {
        dictionaryList = Lists.newArrayListWithCapacity(dictionaryInfoList.size());//字典集合
        for (DictionaryInfo dictInfo : dictionaryInfoList) {
            dictionaryList.add(dictInfo.getDictionaryObject());
        }
        if (!dictionaryList.isEmpty()) {
            curDict = dictionaryList.get(0);
            curKey = curDict.getMinId();
        }
    }

    @Override
    public byte[] current() throws IOException {
        return curValue;
    }

    @Override
    public boolean moveNext() throws IOException {//获取下一行该列的数据
        while (curDictIndex < dictionaryList.size()) {
            if (curKey <= curDict.getMaxId()) {//说明还有数据
                byte[] buffer = new byte[curDict.getSizeOfValue()];
                int size = curDict.getValueBytesFromId(curKey, buffer, 0);//向buffer中添加数据内容.数据是第curKey位置的数据
                curValue = Bytes.copy(buffer, 0, size);//从buffer中读取多少字节
                curKey ++;

                return true;
            }

            // move to next dict if exists
            if (++curDictIndex < dictionaryList.size()) {//切换字典文件
                curDict = dictionaryList.get(curDictIndex);
                curKey = curDict.getMinId();//每一个字典都是有一个最小值的
            }
        }
        curValue = null;
        return false;
    }

    @Override
    public void close() throws IOException {
    }
}
