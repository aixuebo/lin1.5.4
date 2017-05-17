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

package org.apache.kylin.storage.hbase.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 真正删除HDFS上的数据以及Hbase上的数据
 */
public class StorageCleanupJob extends AbstractApplication {

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");

    protected static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);
    public static final int TIME_THRESHOLD_DELETE_HTABLE = 10; // Unit minute

    protected boolean delete = false;
    protected static ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());//线程池去删除hbase的表

    //删除hbase的表
    private void cleanUnusedHBaseTables(Configuration conf) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        long TIME_THREADSHOLD = KylinConfig.getInstanceFromEnv().getStorageCleanupTimeThreshold();//存储周期伐值
        // get all kylin hbase tables
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");//全部hbase的表
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();//需要被删除的hbase表
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);//获取该表的key为KYLIN_HOST的属性值
            String creationTime = desc.getValue(IRealizationConstants.HTableCreationTime);//创建时间
            if (KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                //only take care htables that belongs to self, and created more than 2 days
                if (StringUtils.isEmpty(creationTime) || (System.currentTimeMillis() - Long.valueOf(creationTime) > TIME_THREADSHOLD)) {
                    allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
                } else {
                    logger.info("Exclude table " + desc.getTableName().getNameAsString() + " from drop list, as it is newly created");
                }
            }
        }

        // remove every segment htable from drop list 保持还活跃的cube的segment对应的hbase的表
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();
                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    logger.info("Exclude table " + tablename + " from drop list, as the table belongs to cube " + cube.getName() + " with status " + cube.getStatus());
                }
            }
        }

        //删除操作
        if (delete == true) {
            // drop tables
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            for (String htableName : allTablesNeedToBeDropped) {
                FutureTask futureTask = new FutureTask(new DeleteHTableRunnable(hbaseAdmin, htableName));
                executorService.execute(futureTask);
                try {
                    futureTask.get(TIME_THRESHOLD_DELETE_HTABLE, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    logger.warn("It fails to delete htable " + htableName + ", for it cost more than " + TIME_THRESHOLD_DELETE_HTABLE + " minutes!");
                    futureTask.cancel(true);
                } catch (Exception e) {
                    e.printStackTrace();
                    futureTask.cancel(true);
                }
            }
            executorService.shutdown();
        } else {
            System.out.println("--------------- Tables To Be Dropped ---------------");
            for (String htableName : allTablesNeedToBeDropped) {
                System.out.println(htableName);
            }
            System.out.println("----------------------------------------------------");
        }

        hbaseAdmin.close();
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));

        Configuration conf = HBaseConfiguration.create();

        cleanUnusedIntermediateHiveTable(conf);//删除已经jobid不是在工作中的数据库表
        cleanUnusedHdfsFiles(conf);//删除HDFS上的内容
        cleanUnusedHBaseTables(conf);//删除hbase的表

    }

    //删除一个habse的表线程
    class DeleteHTableRunnable implements Callable {
        HBaseAdmin hbaseAdmin;
        String htableName;

        DeleteHTableRunnable(HBaseAdmin hbaseAdmin, String htableName) {
            this.hbaseAdmin = hbaseAdmin;
            this.htableName = htableName;
        }

        public Object call() throws Exception {
            logger.info("Deleting HBase table " + htableName);
            if (hbaseAdmin.tableExists(htableName)) {
                if (hbaseAdmin.isTableEnabled(htableName)) {
                    hbaseAdmin.disableTable(htableName);
                }

                hbaseAdmin.deleteTable(htableName);
                logger.info("Deleted HBase table " + htableName);
            } else {
                logger.info("HBase table" + htableName + " does not exist");
            }
            return null;
        }
    }

    private void cleanUnusedHdfsFiles(Configuration conf) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = FileSystem.get(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        // GlobFilter filter = new
        // GlobFilter(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
        // + "/kylin-.*");
        //查目录下是kylin-开头的目录,即可以被删除的kylin下的HDFS目录
        FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));///kylin/kylin_metadata/
        for (FileStatus status : fStatus) {
            String path = status.getPath().getName();
            // System.out.println(path);
            if (path.startsWith("kylin-")) {//存储的是所有的jobId
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + path;
                allHdfsPathsNeedToBeDeleted.add(kylinJobPath);//等待删除的目录
            }
        }

        //将运行中的job从删除集合中删除,即不能删除这些job的信息
        List<String> allJobs = executableManager.getAllJobIds();//所有执行的job
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files 仅仅删除成功和丢弃的job
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {//只要该job不是成功,也不是抛弃的,则都不会被删除
                String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobId);
                allHdfsPathsNeedToBeDeleted.remove(path);
                logger.info("Skip " + path + " from deletion list, as the path belongs to job " + jobId + " with status " + state);
            }
        }

        // remove every segment working dir from deletion list
        //segment不能被删除
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {//最后一个job也是不能删除的
                    String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobUuid);
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    logger.info("Skip " + path + " from deletion list, as the path belongs to segment " + seg + " of cube " + cube.getName());
                }
            }
        }

        //删除或者打印
        if (delete == true) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                logger.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p) == true) {
                    fs.delete(p, true);
                    logger.info("Deleted hdfs path " + hdfsPath);
                } else {
                    logger.info("Hdfs path " + hdfsPath + "does not exist");
                }
            }
        } else {
            System.out.println("--------------- HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }
    }

    //删除已经jobid不是在工作中的数据库表
    //疑问:hive的表使用的是cube的name和cube的segment的UUID组成的,为什么要用jobid去过滤,觉得有问题,不会有符合标准的hive table被筛选出来呢
    //而且临时表hive都已经在cube的builder过程的最后阶段被清理掉了,难道是job也会创建临时表么?
    private void cleanUnusedIntermediateHiveTable(Configuration conf) throws IOException {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final CliCommandExecutor cmdExec = config.getCliCommandExecutor();
        final int uuidLength = 36;//uuid的长度

        final String useDatabaseHql = "USE " + config.getHiveDatabaseForIntermediateTable() + ";";//hive的use 数据库
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(useDatabaseHql);
        hiveCmdBuilder.addStatement("show tables " + "\'kylin_intermediate_*\'" + "; ");//展示kylin_intermediate开头的临时表

        Pair<Integer, String> result = cmdExec.execute(hiveCmdBuilder.build());

        String outputStr = result.getSecond();//返回的查询结果集
        BufferedReader reader = new BufferedReader(new StringReader(outputStr));
        String line = null;
        List<String> allJobs = executableManager.getAllJobIds();//所有的jobid集合
        List<String> workingJobList = new ArrayList<String>();//工作中的job,不能被删除
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();//要准备去删除的hive的table


        for (String jobId : allJobs) {//循环每一个job
            // only remove FINISHED and DISCARDED job intermediate table
            final ExecutableState state = executableManager.getOutput(jobId).getState();//获取该job的状态

            if (!state.isFinalState()) {//说明不是DISCARDED 也不是 SUCCEED,则保留,说明该任务还在运行中
                workingJobList.add(jobId);
                logger.info("Skip intermediate hive table with job id " + jobId + " with job status " + state);
            }
        }

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("kylin_intermediate_")) {
                boolean isNeedDel = false;
                String uuid = line.substring(line.length() - uuidLength, line.length());//获取uuid
                uuid = uuid.replace("_", "-");
                //Check whether it's a hive table in use
                if (allJobs.contains(uuid) && !workingJobList.contains(uuid)) {
                    isNeedDel = true;
                }

                if (isNeedDel) {
                    allHiveTablesNeedToBeDeleted.add(line);
                }
            }
        }

        if (delete == true) {
            hiveCmdBuilder.reset();
            //执行删除hive表操作
            hiveCmdBuilder.addStatement(useDatabaseHql);//使用哪个数据库
            for (String delHive : allHiveTablesNeedToBeDeleted) {
                hiveCmdBuilder.addStatement("drop table if exists " + delHive + "; ");//删除hive的表
                logger.info("Remove " + delHive + " from hive tables.");
            }

            try {
                cmdExec.execute(hiveCmdBuilder.build());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //打印信息
            System.out.println("------ Intermediate Hive Tables To Be Dropped ------");
            for (String hiveTable : allHiveTablesNeedToBeDeleted) {
                System.out.println(hiveTable);
            }
            System.out.println("----------------------------------------------------");
        }

        if (reader != null)
            reader.close();
    }

    public static void main(String[] args) throws Exception {
        StorageCleanupJob cli = new StorageCleanupJob();
        cli.execute(args);
    }
}
