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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;

/**
 * @author yangli9
 * 如何对DictionaryInfo进行序列化和反序列化
 */
public class DictionaryInfoSerializer implements Serializer<DictionaryInfo> {

    public static final DictionaryInfoSerializer FULL_SERIALIZER = new DictionaryInfoSerializer(false);//加载字典的描述信息和字典的内容,因此会比较慢
    public static final DictionaryInfoSerializer INFO_SERIALIZER = new DictionaryInfoSerializer(true);//仅仅加载字典描述信息

    private boolean infoOnly;//true 表示仅仅序列化DictionaryInfo对象,不会深度序列化DictionaryInfo中的Dictionary对象

    public DictionaryInfoSerializer() {
        this(false);
    }

    public DictionaryInfoSerializer(boolean infoOnly) {
        this.infoOnly = infoOnly;
    }

    //序列化
    @Override
    public void serialize(DictionaryInfo obj, DataOutputStream out) throws IOException {
        String json = JsonUtil.writeValueAsIndentString(obj);//序列化成json
        out.writeUTF(json);

        if (infoOnly == false)
            obj.getDictionaryObject().write(out);
    }

    @Override
    public DictionaryInfo deserialize(DataInputStream in) throws IOException {
        String json = in.readUTF();
        DictionaryInfo obj = JsonUtil.readValue(json, DictionaryInfo.class);

        if (infoOnly == false) {
            Dictionary<?> dict;
            try {
                dict = (Dictionary<?>) ClassUtil.forName(obj.getDictionaryClass(), Dictionary.class).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            dict.readFields(in);
            obj.setDictionaryObject(dict);
        }
        return obj;
    }

}
