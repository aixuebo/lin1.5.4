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

package org.apache.kylin.engine.mr.common;

public interface BatchConstants {

    /**
     * source data config
     */
    char INTERMEDIATE_TABLE_ROW_DELIMITER = 127;//默认的分隔符

    /**
     * ConFiGuration entry names for MR jobs
     */

    String CFG_CUBE_NAME = "cube.name";
    String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";
    String CFG_CUBE_SEGMENT_ID = "cube.segment.id";
    String CFG_CUBE_CUBOID_LEVEL = "cube.cuboid.level";

    String CFG_II_NAME = "ii.name";
    String CFG_II_SEGMENT_NAME = "ii.segment.name";

    String CFG_OUTPUT_PATH = "output.path";//输出路径
    String CFG_TABLE_NAME = "table.name";
    String CFG_IS_MERGE = "is.merge";
    String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";//行分隔符

    //hbase的配置
    //因为hbase是regionserver存储数据,数据以region为单位瘃在regionserver上,而每一个region又分为若干个HFile文件
    String CFG_REGION_NUMBER_MIN = "region.number.min";//最少需要多少个region
    String CFG_REGION_NUMBER_MAX = "region.number.max";//最多需要多少个region
    String CFG_REGION_SPLIT_SIZE = "region.split.size";//最终切分成多少个region
    String CFG_HFILE_SIZE_GB = "hfile.size.gb";//表示一个hfile文件的大小,单位是G

    String CFG_KYLIN_LOCAL_TEMP_DIR = "/tmp/kylin/";
    String CFG_KYLIN_HDFS_TEMP_DIR = "/tmp/kylin/";

    String CFG_STATISTICS_LOCAL_DIR = CFG_KYLIN_LOCAL_TEMP_DIR + "cuboidstatistics/";
    String CFG_STATISTICS_ENABLED = "statistics.enabled";//是否进行统计
    String CFG_STATISTICS_OUTPUT = "statistics.ouput";//spell error, for compatibility issue better not change it //统计的输出目录
    String CFG_STATISTICS_SAMPLING_PERCENT = "statistics.sampling.percent";//统计抽样的百分比
    String CFG_STATISTICS_CUBE_ESTIMATION_FILENAME = "cube_statistics.txt";
    String CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME = "cuboid_statistics.seq";//统计文件的名字

    /**
     * command line ARGuments
     * 以下是命令行输入的key
     */
    //输入路径以及输入的格式
    String ARG_INPUT = "input";
    String ARG_INPUT_FORMAT = "inputformat";
    String ARG_OUTPUT = "output";//输出目录
    String ARG_JOB_NAME = "jobname";//job的名字,在yarn上可以看到

    String ARG_CUBING_JOB_ID = "cubingJobId";
    String ARG_CUBE_NAME = "cubename";//cubeName,在kylin的管理页面上定义cube的时候定义的名字
    String ARG_II_NAME = "iiname";

    //segmentName和segmentID
    String ARG_SEGMENT_NAME = "segmentname";
    String ARG_SEGMENT_ID = "segmentid";

    String ARG_PARTITION = "partitions";


    String ARG_STATS_ENABLED = "statisticsenabled";//true/false,是否打开统计功能
    String ARG_STATS_OUTPUT = "statisticsoutput";//统计的输出目录
    String ARG_STATS_SAMPLING_PERCENT = "statisticssamplingpercent";//统计抽样百分比

    String ARG_HTABLE_NAME = "htablename";
    String ARG_LEVEL = "level";//level级别,内容是整数数字

    /**
     * logger and counter
     */
    String MAPREDUCE_COUNTER_GROUP_NAME = "Cube Builder";
    int NORMAL_RECORD_LOG_THRESHOLD = 100000; //map处理多少行数据后,打印一次日志
    int ERROR_RECORD_LOG_THRESHOLD = 100;//row解析失败的行数上限,超过该值,则抛异常
}
