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

package org.apache.kylin.metadata;

/**
 * Constances to describe metadata and it's change.
 * 用于存储元数据参数信息的key
 */
public interface MetadataConstants {

    public static final String FILE_SURFIX = ".json";//文件后缀

    // Extended attribute keys
    public static final String TABLE_EXD_TABLENAME = "tableName";//hive的表名字
    public static final String TABLE_EXD_LOCATION = "location";//hive的Location,hdfs://host:port/log/stat/path
    public static final String TABLE_EXD_IF = "inputformat";//hive的输入格式化类org.apache.hadoop.mapred.TextInputFormat
    public static final String TABLE_EXD_OF = "outputformat";//hive的输出格式化类org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat

    public static final String TABLE_EXD_COLUMN = "columns";
    public static final String TABLE_EXD_PC = "partitionColumns";//表的分区信息,多个分区字段用逗号拆分,比如column1,column2
    public static final String TABLE_EXD_PARTITIONED = "partitioned";//表是否包含分区


    public static final String TABLE_EXD_MINFS = "minFileSize";
    public static final String TABLE_EXD_MAXFS = "maxFileSize";

    public static final String TABLE_EXD_LUT = "lastUpdateTime";
    public static final String TABLE_EXD_LAT = "lastAccessTime";//表的最后访问时间
    public static final String TABLE_EXD_TNF = "totalNumberFiles";//表此时的文件数量
    public static final String TABLE_EXD_TFS = "totalFileSize";//表此时的大小
    public static final String TABLE_EXD_OWNER = "owner";//表的所有者

    /**
     * The value is an array
     */
    public static final String TABLE_EXD_CARDINALITY = "cardinality";//标识为hive的每一列计算不同值数量,例如有5列,那么这个值是五个值,用逗号拆分组成的数组
    public static final String TABLE_EXD_DELIM = "delim";
    public static final String TABLE_EXD_DEFAULT_VALUE = "unknown";

    public static final String TABLE_EXD_STATUS_KEY = "EXD_STATUS";//true表示该table有key-value的属性信息存在,false表示不存在

}
