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

package org.apache.kylin.job.constant;

/**
 */
public final class ExecutableConstants {

    private ExecutableConstants() {
    }

    public static final String MR_JOB_ID = "mr_job_id";//存储mr的jobid


    public static final String YARN_APP_ID = "yarn_application_id";//yarn上产生的application_id
    public static final String YARN_APP_URL = "yarn_application_tracking_url";//yarn上app的url,比如hive的输出,Starting Job = job_1477918604929_15950, Tracking URL = http://path:8088/proxy/application_1477918604929_15950/


    public static final String HDFS_BYTES_WRITTEN = "hdfs_bytes_written";//hive写入多少个字节到hdfs
    public static final String SOURCE_RECORDS_COUNT = "source_records_count";//匹配输入源有多少行数据
    public static final String SOURCE_RECORDS_SIZE = "source_records_size";//匹配输入源读取多少字节数据

    public static final int DEFAULT_SCHEDULER_INTERVAL_SECONDS = 60;

    //JOB监控中的job名字
    public static final String STEP_NAME_CREATE_FLAT_HIVE_TABLE = "Create Intermediate Flat Hive Table";//创建hive的临时表执行任务,即执行hive的sql
    public static final String STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP = "Materialize Hive View in Lookup Tables";//针对lookup表是视图表的时候进行操作的job
    public static final String STEP_NAME_COUNT_HIVE_TABLE = "Count Source Table";//表的记录条数
    public static final String STEP_NAME_FACT_DISTINCT_COLUMNS = "Extract Fact Table Distinct Columns";//1.用于将rowkey中每一个字典列需要的字段值收集起来,一个reduce一个字段的不重复的字典值  2.统计rowkey的各种组合情况,即每一个cuboid有多少个不同的元素
    public static final String STEP_NAME_BUILD_DICTIONARY = "Build Dimension Dictionary";//STEP_NAME_FACT_DISTINCT_COLUMNS任务的输出目录作为输入目录,主要处理字典信息以及对lookup表设置快照
    public static final String STEP_NAME_SAVE_STATISTICS = "Save Cuboid Statistics";//根据对cube的统计结果,决定使用什么算法处理cube的build

    //执行MR处理或者基于内存处理,选择一种执行方式去执行
    public static final String STEP_NAME_BUILD_BASE_CUBOID = "Build Base Cuboid Data";//对baseCuboid进行mr处理 -----对基本的cuboid进行处理---设置rowkey为需要的全部字段,设置value为需要的全部度量的值
    public static final String STEP_NAME_BUILD_N_D_CUBOID = "Build N-Dimension Cuboid Data";//处理每一个cuboid子任务
    public static final String STEP_NAME_BUILD_IN_MEM_CUBE = "Build Cube";//执行内存操作

    public static final String STEP_NAME_CONVERT_CUBOID_TO_HFILE = "Convert Cuboid Data to HFile";//读取rowkey--所有度量的值作为输入源,输出到hbase中,rowkey不变,只是将所有的度量值,拆分成若干个列族,存放到不同的列里面
    public static final String STEP_NAME_BULK_LOAD_HFILE = "Load HFile to HBase Table";//将一个hfile的文件路径,插入到hbase的一个表下
    public static final String STEP_NAME_UPDATE_CUBE_INFO = "Update Cube Info";//更新cube的元数据以及segment的元数据信息
    public static final String STEP_NAME_GARBAGE_COLLECTION = "Garbage Collection";//删除数据库以及HDFS的内容任务

    public static final String STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION = "Calculate HTable Region Splits";//用于对所有的rowkey进行划分范围
    public static final String STEP_NAME_CREATE_HBASE_TABLE = "Create HTable";//读取rowkey的分布范围--创建hbase的table表
    public static final String STEP_NAME_MERGE_DICTIONARY = "Merge Cuboid Dictionary";
    public static final String STEP_NAME_MERGE_STATISTICS = "Merge Cuboid Statistics";
    public static final String STEP_NAME_MERGE_CUBOID = "Merge Cuboid Data";
    public static final String STEP_NAME_GARBAGE_COLLECTION_HDFS = "Garbage Collection on HDFS";
    public static final String STEP_NAME_BUILD_II = "Build Inverted Index";
    public static final String STEP_NAME_CONVERT_II_TO_HFILE = "Convert Inverted Index Data to HFile";
    public static final String STEP_NAME_UPDATE_II_INFO = "Update Inverted Index Info";
    public static final String STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE = "Redistribute Flat Hive Table";//计算select count(*) 表的总记录任务

    //将${job_name}替换成真正的名字   等等
    public static final String NOTIFY_EMAIL_TEMPLATE = "<div><b>Build Result of Job ${job_name}</b><pre><ul>" + "<li>Build Result: <b>${result}</b></li>" + "<li>Job Engine: ${job_engine}</li>" + "<li>Env: ${env_name}</li>" + "<li>Project: ${project_name}</li>" + "<li>Cube Name: ${cube_name}</li>" + "<li>Source Records Count: ${source_records_count}</li>" + "<li>Start Time: ${start_time}</li>" + "<li>Duration: ${duration}</li>" + "<li>MR Waiting: ${mr_waiting}</li>" + "<li>Last Update Time: ${last_update_time}</li>" + "<li>Submitter: ${submitter}</li>" + "<li>Error Log: ${error_log}</li>" + "</ul></pre><div/>";
}
