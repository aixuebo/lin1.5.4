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

package org.apache.kylin.engine.mr.steps;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 用于删除过期的资源
 * ./bin/metastore.sh clean --delete true
 * ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.storage.hbase.util.StorageCleanupJob --delete true
 */
public class MetadataCleanupJob extends AbstractHadoopJob {

    //-delete true表示删除未使用的元数据
    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused metadata").create("delete");

    protected static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);

    boolean delete = false;

    private KylinConfig config = null;

    public static final long TIME_THREADSHOLD = 2 * 24 * 3600 * 1000L; // 2 days
    public static final long TIME_THREADSHOLD_FOR_JOB = 30 * 24 * 3600 * 1000L; // 30 days

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        logger.info("jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            parseOptions(options, args);

            logger.info("options: '" + getOptionsAsString() + "'");
            logger.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));

            config = KylinConfig.getInstanceFromEnv();

            cleanup();

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    //参数是文件的最后修改时间
    //true表示资源已经很老了
    private boolean isOlderThanThreshold(long resourceTime) {
        long currentTime = System.currentTimeMillis();

        if (currentTime - resourceTime > TIME_THREADSHOLD)
            return true;
        return false;
    }

    public void cleanup() throws Exception {
        CubeManager cubeManager = CubeManager.getInstance(config);

        //存储已经过期的资源path路径集合
        List<String> toDeleteResource = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        //查找两种资源,table_snapshot和cube_statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT, ResourceStore.CUBE_STATISTICS_ROOT }) {
            NavigableSet<String> snapshotTables = getStore().listResources(resourceRoot);

            if (snapshotTables != null) {
                for (String snapshotTable : snapshotTables) {
                    NavigableSet<String> snapshotNames = getStore().listResources(snapshotTable);
                    if (snapshotNames != null)
                        for (String snapshot : snapshotNames) {
                            if (isOlderThanThreshold(getStore().getResourceTimestamp(snapshot)))//获取文件的最后修改时间
                                toDeleteResource.add(snapshot);
                        }
                }
            }
        }

        // three level resources, only dictionaries
        //dict资源
        NavigableSet<String> dictTables = getStore().listResources(ResourceStore.DICT_RESOURCE_ROOT);

        if (dictTables != null) {
            for (String table : dictTables) {
                NavigableSet<String> tableColNames = getStore().listResources(table);
                if (tableColNames != null)
                    for (String tableCol : tableColNames) {
                        NavigableSet<String> dictionaries = getStore().listResources(tableCol);
                        if (dictionaries != null)
                            for (String dict : dictionaries)
                                if (isOlderThanThreshold(getStore().getResourceTimestamp(dict)))
                                    toDeleteResource.add(dict);
                    }
            }
        }

        //活跃的资源
        Set<String> activeResourceList = Sets.newHashSet();
        for (org.apache.kylin.cube.CubeInstance cube : cubeManager.listAllCubes()) {//所有cube
            for (org.apache.kylin.cube.CubeSegment segment : cube.getSegments()) {//每一个cube的所有segment
                activeResourceList.addAll(segment.getSnapshotPaths());
                activeResourceList.addAll(segment.getDictionaryPaths());
                activeResourceList.add(segment.getStatisticsResourcePath());
            }
        }

        //删除的资源-活跃的资源
        toDeleteResource.removeAll(activeResourceList);

        // delete old and completed jobs
        ExecutableDao executableDao = ExecutableDao.getInstance(KylinConfig.getInstanceFromEnv());
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            ExecutableOutputPO output = executableDao.getJobOutput(executable.getUuid());
            if (System.currentTimeMillis() - lastModified > TIME_THREADSHOLD_FOR_JOB && (ExecutableState.SUCCEED.toString().equals(output.getStatus()) || ExecutableState.DISCARDED.toString().equals(output.getStatus()))) {
                toDeleteResource.add(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + executable.getUuid());
                toDeleteResource.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + executable.getUuid());

                for (ExecutablePO task : executable.getTasks()) {
                    toDeleteResource.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                }
            }
        }

        if (toDeleteResource.size() > 0) {
            logger.info("The following resources have no reference or is too old, will be cleaned from metadata store: \n");

            for (String s : toDeleteResource) {
                logger.info(s);//只是打印信息
                if (delete == true) {//true的时候才会选择真的去删除
                    getStore().deleteResource(s);
                }
            }
        } else {
            logger.info("No resource to be cleaned up from metadata store;");
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MetadataCleanupJob(), args);
        System.exit(exitCode);
    }
}
