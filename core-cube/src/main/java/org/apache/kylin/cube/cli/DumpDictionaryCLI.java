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

package org.apache.kylin.cube.cli;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;

//打印一组路径下所有字典内容
public class DumpDictionaryCLI {

    //参数是一组path路径集合
    public static void main(String[] args) throws IOException {
        for (String path : args) {
            dump(new File(path));
        }
    }

    public static void dump(File f) throws IOException {
        if (f.isDirectory()) {//打印该目录下所有文件
            for (File c : f.listFiles())
                dump(c);
            return;
        }

        if (f.getName().endsWith(".dict")) {//如果文件是.dict就执行
            DictionaryInfoSerializer ser = new DictionaryInfoSerializer();
            DictionaryInfo dictInfo = ser.deserialize(new DataInputStream(new FileInputStream(f)));//反序列化该文件,得到字典对象

            System.out.println("============================================================================");
            System.out.println("File: " + f.getAbsolutePath());
            System.out.println(new Date(dictInfo.getLastModified()));//字典的最后修改时间
            System.out.println(JsonUtil.writeValueAsIndentString(dictInfo));//打印字典的元数据信息
            dictInfo.getDictionaryObject().dump(System.out);//字典内容反序列化后打印出来
            System.out.println();
        }
    }
}
