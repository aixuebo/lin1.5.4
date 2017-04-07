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
package org.apache.kylin.source.hive;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行hive sql的执行器
 */
public class CreateFlatHiveTableStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CreateFlatHiveTableStep.class);
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    //读取cunt文件,返回一个long类型内容---该值是select count from 表的结果
    private long readRowCountFromFile() throws IOException {
        Path rowCountFile = new Path(getRowCountOutputDir(), "000000_0");

        FileSystem fs = FileSystem.get(rowCountFile.toUri(), HadoopUtil.getCurrentConfiguration());
        InputStream in = fs.open(rowCountFile);
        try {
            String content = IOUtils.toString(in, Charset.defaultCharset());
            return Long.valueOf(content.trim()); // strip the '\n' character

        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    //计算reduce数量
    //参数rowCount 表示总行数
    private int determineNumReducer(KylinConfig config, long rowCount) throws IOException {
        int mapperInputRows = config.getHadoopJobMapperInputRows();//每一个reduce要处理多少行数据

        int numReducers = Math.round(rowCount / ((float) mapperInputRows));//计算多少个reduce能完成这些任务

        //让reduce在max和min之间
        numReducers = Math.max(numReducers, config.getHadoopJobMinReducerNumber());
        numReducers = Math.min(numReducers, config.getHadoopJobMaxReducerNumber());

        stepLogger.log("total input rows = " + rowCount);
        stepLogger.log("expected input rows per mapper = " + mapperInputRows);
        stepLogger.log("reducers for RedistributeFlatHiveTableStep = " + numReducers);

        return numReducers;
    }

    //执行hive的sql
    private void createFlatHiveTable(KylinConfig config, int numReducers) throws IOException {
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(getInitStatement());//hive初始化sql
        boolean useRedistribute = getUseRedistribute();
        if (useRedistribute == true) {
            hiveCmdBuilder.addStatement("set mapreduce.job.reduces=" + numReducers + ";\n");//设置reduce数量
            hiveCmdBuilder.addStatement("set hive.merge.mapredfiles=false;\n"); //disable merge 不需要合并文件
        }
        hiveCmdBuilder.addStatement(getCreateTableStatement());
        final String cmd = hiveCmdBuilder.toString();

        stepLogger.log("Create and distribute table, cmd: ");
        stepLogger.log(cmd);

        //本地去执行该命令,返回执行的状态码和输出日志
        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
        if (response.getFirst() != 0) {//状态码出现异常,则抛异常
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    //获取该cube对应的配置文件
    private KylinConfig getCubeSpecificConfig() {
        String cubeName = CubingExecutableUtil.getCubeName(getParams());
        CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = manager.getCube(cubeName);
        return cube.getConfig();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = getCubeSpecificConfig();
        try {

            boolean useRedistribute = getUseRedistribute();

            int numReducers = 0;
            if (useRedistribute == true) {
                long rowCount = readRowCountFromFile();
                if (!config.isEmptySegmentAllowed() && rowCount == 0) {
                    stepLogger.log("Detect upstream hive table is empty, " + "fail the job because \"kylin.job.allow.empty.segment\" = \"false\"");
                    return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
                }

                numReducers = determineNumReducer(config, rowCount);
            }

            createFlatHiveTable(config, numReducers);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    //hive初始化sql,即hive的set name=value等一些列属性
    public String getInitStatement() {
        return getParam("HiveInit");
    }
    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    //true表示使用DISTRIBUTE BY RAND() 或者DISTRIBUTE BY 字段 语法
    public boolean getUseRedistribute() {
        return Boolean.valueOf(getParam("useRedistribute"));
    }
    public void setUseRedistribute(boolean useRedistribute) {
        setParam("useRedistribute", String.valueOf(useRedistribute));
    }

    //执行的hive sql,参见HiveMRInput类的createFlatHiveTableStep方法,包含设置hive的set key=value,drop表,create 表,以及insert into 以及select语法一整套sql
    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }
    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    //行总数输出目录
    public void setRowCountOutputDir(String rowCountOutputDir) {
        setParam("rowCountOutputDir", rowCountOutputDir);
    }
    public String getRowCountOutputDir() {
        return getParam("rowCountOutputDir");
    }
}
