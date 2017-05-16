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

/**
 * @author George Song (ysong1)
 *
 */

import static org.apache.hadoop.util.StringUtils.formatTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//表示一个mr的job的抽象类
@SuppressWarnings("static-access")
public abstract class AbstractHadoopJob extends Configured implements Tool {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractHadoopJob.class);

    //job的名字,在yarn上可以看到
    protected static final Option OPTION_JOB_NAME = OptionBuilder.withArgName(BatchConstants.ARG_JOB_NAME).hasArg().isRequired(true).withDescription("Job name. For example, Kylin_Cuboid_Builder-clsfd_v2_Step_22-D)").create(BatchConstants.ARG_JOB_NAME);
    //cubeName,在kylin定义cube的name
    protected static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube name. For exmaple, flat_item_cube").create(BatchConstants.ARG_CUBE_NAME);
    protected static final Option OPTION_CUBING_JOB_ID = OptionBuilder.withArgName(BatchConstants.ARG_CUBING_JOB_ID).hasArg().isRequired(false).withDescription("ID of cubing job executable").create(BatchConstants.ARG_CUBING_JOB_ID);
    //segmentName和segmentID
    protected static final Option OPTION_SEGMENT_NAME = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_NAME).hasArg().isRequired(true).withDescription("Cube segment name").create(BatchConstants.ARG_SEGMENT_NAME);
    protected static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_ID).hasArg().isRequired(true).withDescription("Cube segment id").create(BatchConstants.ARG_SEGMENT_ID);
    //输入路径以及输入的格式
    protected static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg().isRequired(true).withDescription("Input path").create(BatchConstants.ARG_INPUT);
    protected static final Option OPTION_INPUT_FORMAT = OptionBuilder.withArgName(BatchConstants.ARG_INPUT_FORMAT).hasArg().isRequired(false).withDescription("Input format").create(BatchConstants.ARG_INPUT_FORMAT);
    //输出目录
    protected static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg().isRequired(true).withDescription("Output path").create(BatchConstants.ARG_OUTPUT);

    //level级别,内容是整数数字
    protected static final Option OPTION_NCUBOID_LEVEL = OptionBuilder.withArgName(BatchConstants.ARG_LEVEL).hasArg().isRequired(true).withDescription("N-Cuboid build level, e.g. 1, 2, 3...").create(BatchConstants.ARG_LEVEL);
    protected static final Option OPTION_PARTITION_FILE_PATH = OptionBuilder.withArgName(BatchConstants.ARG_PARTITION).hasArg().isRequired(true).withDescription("Partition file path.").create(BatchConstants.ARG_PARTITION);
    protected static final Option OPTION_HTABLE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_HTABLE_NAME).hasArg().isRequired(true).withDescription("HTable name").create(BatchConstants.ARG_HTABLE_NAME);

    protected static final Option OPTION_STATISTICS_ENABLED = OptionBuilder.withArgName(BatchConstants.ARG_STATS_ENABLED).hasArg().isRequired(false).withDescription("Statistics enabled").create(BatchConstants.ARG_STATS_ENABLED);
    protected static final Option OPTION_STATISTICS_OUTPUT = OptionBuilder.withArgName(BatchConstants.ARG_STATS_OUTPUT).hasArg().isRequired(false).withDescription("Statistics output").create(BatchConstants.ARG_STATS_OUTPUT);
    protected static final Option OPTION_STATISTICS_SAMPLING_PERCENT = OptionBuilder.withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg().isRequired(false).withDescription("Statistics sampling percentage").create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);

    private static final String MAP_REDUCE_CLASSPATH = "mapreduce.application.classpath";

    //kylin依赖的hive的jar包
    private static final String KYLIN_HIVE_DEPENDENCY_JARS = "[^,]*hive-exec[0-9.-]+[^,]*?\\.jar" + "|" + "[^,]*hive-metastore[0-9.-]+[^,]*?\\.jar" + "|" + "[^,]*hive-hcatalog-core[0-9.-]+[^,]*?\\.jar";

    protected static void runJob(Tool job, String[] args) {
        try {
            int exitCode = ToolRunner.run(job, args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(5);
        }
    }

    // ============================================================================

    protected String name;
    protected boolean isAsync = false;//true表示异步,即MR操作提交后不用等待完成
    protected OptionsHelper optionsHelper = new OptionsHelper();

    protected Job job;

    public AbstractHadoopJob() {
        super(HadoopUtil.getCurrentConfiguration());
    }

    protected void parseOptions(Options options, String[] args) throws ParseException {
        optionsHelper.parseOptions(options, args);
    }

    public void printUsage(Options options) {
        optionsHelper.printUsage(getClass().getSimpleName(), options);
    }

    public Option[] getOptions() {
        return optionsHelper.getOptions();
    }

    public String getOptionsAsString() {
        return optionsHelper.getOptionsAsString();
    }

    protected String getOptionValue(Option option) {
        return optionsHelper.getOptionValue(option);
    }

    protected boolean hasOption(Option option) {
        return optionsHelper.hasOption(option);
    }

    //提交job
    protected int waitForCompletion(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        int retVal = 0;
        long start = System.nanoTime();
        if (isAsync) {//true表示异步,即提交后不用等待完成
            job.submit();
        } else {//默认是提交后等待完成,即同步方式
            job.waitForCompletion(true);
            retVal = job.isSuccessful() ? 0 : 1;
            logger.debug("Job '" + job.getJobName() + "' finished " + (job.isSuccessful() ? "successfully in " : "with failures.  Time taken ") + formatTime((System.nanoTime() - start) / 1000000L));
        }
        return retVal;
    }

    //添加classpath信息
    protected void setJobClasspath(Job job, KylinConfig kylinConf) {
        String jarPath = kylinConf.getKylinJobJarPath();//设置job启动的主要jar
        File jarFile = new File(jarPath);
        if (jarFile.exists()) {
            job.setJar(jarPath);//设置主jar
            logger.info("append job jar: " + jarPath);
        } else {
            job.setJarByClass(this.getClass());
        }

        //三个依赖路径
        String kylinHiveDependency = System.getProperty("kylin.hive.dependency");
        String kylinHBaseDependency = System.getProperty("kylin.hbase.dependency");
        String kylinKafkaDependency = System.getProperty("kylin.kafka.dependency");
        logger.info("append kylin.hbase.dependency: " + kylinHBaseDependency + " to " + MAP_REDUCE_CLASSPATH);

        Configuration jobConf = job.getConfiguration();
        String classpath = jobConf.get(MAP_REDUCE_CLASSPATH);
        if (classpath == null || classpath.length() == 0) {//设置mr依赖路径
            logger.info("Didn't find " + MAP_REDUCE_CLASSPATH + " in job configuration, will run 'mapred classpath' to get the default value.");
            classpath = getDefaultMapRedClasspath();
            logger.info("The default mapred classpath is: " + classpath);
        }

        if (kylinHBaseDependency != null) {
            // yarn classpath is comma separated
            kylinHBaseDependency = kylinHBaseDependency.replace(":", ",");
            classpath = classpath + "," + kylinHBaseDependency;
        }

        jobConf.set(MAP_REDUCE_CLASSPATH, classpath);
        logger.info("Hadoop job classpath is: " + job.getConfiguration().get(MAP_REDUCE_CLASSPATH));

        /*
         *  set extra dependencies as tmpjars & tmpfiles if configured
         */
        StringBuilder kylinDependency = new StringBuilder();

        // for hive dependencies
        if (kylinHiveDependency != null) {
            // yarn classpath is comma separated
            kylinHiveDependency = kylinHiveDependency.replace(":", ",");

            logger.info("Hive Dependencies Before Filtered: " + kylinHiveDependency);
            String filteredHive = filterKylinHiveDependency(kylinHiveDependency);
            logger.info("Hive Dependencies After Filtered: " + filteredHive);

            if (kylinDependency.length() > 0)
                kylinDependency.append(",");
            kylinDependency.append(filteredHive);
        } else {

            logger.info("No hive dependency jars set in the environment, will find them from jvm:");

            try {
                String hiveExecJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hadoop.hive.ql.Driver"));
                kylinDependency.append(hiveExecJarPath).append(",");
                logger.info("hive-exec jar file: " + hiveExecJarPath);

                String hiveHCatJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hive.hcatalog.mapreduce.HCatInputFormat"));
                kylinDependency.append(hiveHCatJarPath).append(",");
                logger.info("hive-catalog jar file: " + hiveHCatJarPath);

                String hiveMetaStoreJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hadoop.hive.metastore.api.Table"));
                kylinDependency.append(hiveMetaStoreJarPath).append(",");
                logger.info("hive-metastore jar file: " + hiveMetaStoreJarPath);
            } catch (ClassNotFoundException e) {
                logger.error("Cannot found hive dependency jars: " + e);
            }
        }

        // for hive dependencies
        if (kylinKafkaDependency != null) {
            kylinKafkaDependency = kylinKafkaDependency.replace(":", ",");

            logger.info("Kafka Dependencies Before Filtered: " + kylinHiveDependency);

            if (kylinDependency.length() > 0)
                kylinDependency.append(",");
            kylinDependency.append(kylinKafkaDependency);
        } else {

            logger.info("No Kafka dependency jars set in the environment, will find them from jvm:");

            try {
                String kafkaClientJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer"));
                kylinDependency.append(kafkaClientJarPath).append(",");
                logger.info("kafka jar file: " + kafkaClientJarPath);

            } catch (ClassNotFoundException e) {
                logger.error("Cannot found kafka dependency jars: " + e);
            }
        }

        // for KylinJobMRLibDir
        String mrLibDir = kylinConf.getKylinJobMRLibDir();
        if (!StringUtils.isBlank(mrLibDir)) {
            File dirFileMRLIB = new File(mrLibDir);
            if (dirFileMRLIB.exists()) {
                if (kylinDependency.length() > 0)
                    kylinDependency.append(",");
                kylinDependency.append(mrLibDir);
            } else {
                logger.info("The directory '" + mrLibDir + "' for 'kylin.job.mr.lib.dir' does not exist!!!");
            }
        }

        setJobTmpJarsAndFiles(job, kylinDependency.toString());

        overrideJobConfig(job.getConfiguration(), kylinConf.getMRConfigOverride());
    }

    //覆盖Configuration的配置信息
    private void overrideJobConfig(Configuration jobConf, Map<String, String> override) {
        for (Entry<String, String> entry : override.entrySet()) {
            jobConf.set(entry.getKey(), entry.getValue());
        }
    }

    //过滤hive的jar包路径集合
    private String filterKylinHiveDependency(String kylinHiveDependency) {
        if (StringUtils.isBlank(kylinHiveDependency))
            return "";

        StringBuilder jarList = new StringBuilder();

        Pattern hivePattern = Pattern.compile(KYLIN_HIVE_DEPENDENCY_JARS);
        Matcher matcher = hivePattern.matcher(kylinHiveDependency);

        while (matcher.find()) {
            if (jarList.length() > 0)
                jarList.append(",");
            jarList.append(matcher.group());
        }

        return jarList.toString();
    }

    //将kylinDependency按照逗号拆分后,是一组路径集合,循环每一个路径,得到子子孙孙的file和jar包.添加到临时文件中
    private void setJobTmpJarsAndFiles(Job job, String kylinDependency) {
        if (StringUtils.isBlank(kylinDependency))
            return;

        String[] fNameList = kylinDependency.split(",");

        try {
            Configuration jobConf = job.getConfiguration();
            FileSystem fs = FileSystem.getLocal(jobConf);

            StringBuilder jarList = new StringBuilder();
            StringBuilder fileList = new StringBuilder();

            for (String fileName : fNameList) {
                Path p = new Path(fileName);
                if (fs.getFileStatus(p).isDirectory()) {
                    appendTmpDir(job, fileName);
                    continue;
                }

                StringBuilder list = (p.getName().endsWith(".jar")) ? jarList : fileList;
                if (list.length() > 0)
                    list.append(",");
                list.append(fs.getFileStatus(p).getPath().toString());
            }

            appendTmpFiles(fileList.toString(), jobConf);
            appendTmpJars(jarList.toString(), jobConf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //解析tmpDir目录下所有子子孙孙文件,将文件加入到appendTmpFiles方法中,将jar包加入到appendTmpJars方法中
    private void appendTmpDir(Job job, String tmpDir) {
        if (StringUtils.isBlank(tmpDir))
            return;

        try {
            Configuration jobConf = job.getConfiguration();
            FileSystem fs = FileSystem.getLocal(jobConf);
            FileStatus[] fList = fs.listStatus(new Path(tmpDir));

            StringBuilder jarList = new StringBuilder();
            StringBuilder fileList = new StringBuilder();

            for (FileStatus file : fList) {
                Path p = file.getPath();
                if (fs.getFileStatus(p).isDirectory()) {
                    appendTmpDir(job, p.toString());
                    continue;
                }

                StringBuilder list = (p.getName().endsWith(".jar")) ? jarList : fileList;
                if (list.length() > 0)
                    list.append(",");
                list.append(fs.getFileStatus(p).getPath().toString());
            }

            appendTmpFiles(fileList.toString(), jobConf);
            appendTmpJars(jarList.toString(), jobConf);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //向job追加jar文件
    private void appendTmpJars(String jarList, Configuration conf) {
        if (StringUtils.isBlank(jarList))
            return;

        String tmpJars = conf.get("tmpjars", null);
        if (tmpJars == null) {
            tmpJars = jarList;
        } else {
            tmpJars += "," + jarList;
        }
        conf.set("tmpjars", tmpJars);
        logger.info("Job 'tmpjars' updated -- " + tmpJars);
    }

    //追加tmpfiles临时文件内容,表示临时文件又增多了
    private void appendTmpFiles(String fileList, Configuration conf) {
        if (StringUtils.isBlank(fileList))
            return;

        String tmpFiles = conf.get("tmpfiles", null);
        if (tmpFiles == null) {
            tmpFiles = fileList;
        } else {
            tmpFiles += "," + fileList;
        }
        conf.set("tmpfiles", tmpFiles);
        logger.info("Job 'tmpfiles' updated -- " + tmpFiles);
    }

    //返回mr的classpath
    private String getDefaultMapRedClasspath() {

        String classpath = "";
        try {
            CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            String output = executor.execute("mapred classpath").getSecond();//执行命令/bin/bash -c mapred classpath 返回输入内容
            classpath = output.trim().replace(':', ',');//输入内容用:转换成,
        } catch (IOException e) {
            logger.error("Failed to run: 'mapred classpath'.", e);
        }

        return classpath;
    }

    //向job中添加输入源,
    public static int addInputDirs(String input, Job job) throws IOException {
        int folderNum = addInputDirs(StringSplitter.split(input, ","), job);
        logger.info("Number of added folders:" + folderNum);
        return folderNum;
    }

    /**
     * 向job中添加输入源,
     * @param inputs 可以是绝对路径,也可以是/*代表的通配符,这个不够好,不如hadoop的通配符好
     * @param job
     * @return
     * @throws IOException
     */
    public static int addInputDirs(String[] inputs, Job job) throws IOException {
        int ret = 0;//return number of added folders
        for (String inp : inputs) {
            inp = inp.trim();
            if (inp.endsWith("/*")) {
                inp = inp.substring(0, inp.length() - 2);
                FileSystem fs = FileSystem.get(job.getConfiguration());
                Path path = new Path(inp);

                if (!fs.exists(path)) {
                    logger.warn("Path not exist:" + path.toString());
                    continue;
                }

                FileStatus[] fileStatuses = fs.listStatus(path);//文件下所有文件
                boolean hasDir = false;
                for (FileStatus stat : fileStatuses) {
                    if (stat.isDirectory() && !stat.getPath().getName().startsWith("_")) {
                        hasDir = true;
                        ret += addInputDirs(new String[] { stat.getPath().toString() }, job);
                    }
                }
                if (fileStatuses.length > 0 && !hasDir) {
                    ret += addInputDirs(new String[] { path.toString() }, job);
                }
            } else {
                logger.debug("Add input " + inp);
                FileInputFormat.addInputPath(job, new Path(inp));
                ret++;
            }
        }
        return ret;
    }

    public static KylinConfig loadKylinPropsAndMetadata() throws IOException {
        File metaDir = new File("meta");
        if (!metaDir.getAbsolutePath().equals(System.getProperty(KylinConfig.KYLIN_CONF))) {
            System.setProperty(KylinConfig.KYLIN_CONF, metaDir.getAbsolutePath());
            logger.info("The absolute path for meta dir is " + metaDir.getAbsolutePath());
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            kylinConfig.setMetadataUrl(metaDir.getAbsolutePath());
            return kylinConfig;
        } else {
            return KylinConfig.getInstanceFromEnv();
        }
    }

    protected void attachKylinPropsAndMetadata(TableDesc table, Configuration conf) throws IOException {
        ArrayList<String> dumpList = new ArrayList<String>();
        dumpList.add(table.getResourcePath());
        attachKylinPropsAndMetadata(dumpList, KylinConfig.getInstanceFromEnv(), conf);
    }

    //为hadoop添加cube的元数据信息
    protected void attachKylinPropsAndMetadata(CubeInstance cube, Configuration conf) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(cube.getConfig());

        // write cube / model_desc / cube_desc / dict / table
        ArrayList<String> dumpList = new ArrayList<String>();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getModel().getResourcePath());//model路径
        dumpList.add(cube.getDescriptor().getResourcePath());//cube的路径

        for (String tableName : cube.getDescriptor().getModel().getAllTables()) {//model依赖的所有table路径
            TableDesc table = metaMgr.getTableDesc(tableName);
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);//以及table依赖的所有table路径
            dumpList.addAll(dependentResources);
        }
        for (CubeSegment segment : cube.getSegments()) {//所有segment依赖的路径
            dumpList.addAll(segment.getDictionaryPaths());
        }

        attachKylinPropsAndMetadata(dumpList, cube.getConfig(), conf);
    }

    /**
     * 1.将KylinConfig配置内容在本地生成文件
     * 2.下载dumpList路径集合对应的资源
     * 3.追加tmpfiles配置文件内容,表示临时文件又增多了
     * @param dumpList
     * @param kylinConfig 配置内容
     * @param conf
     * @throws IOException
     */
    protected void attachKylinPropsAndMetadata(ArrayList<String> dumpList, KylinConfig kylinConfig, Configuration conf) throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");//创建文件
        FileUtils.forceDelete(tmp); // we need a directory, so delete the file first 先删除

        File metaDir = new File(tmp, "meta");//创建目录
        metaDir.mkdirs();

        // write kylin.properties
        File kylinPropsFile = new File(metaDir, "kylin.properties");//创建kylin.properties文件
        kylinConfig.writeProperties(kylinPropsFile);//将所有的配置信息写入到file中

        // write resources 下载资源
        dumpResources(kylinConfig, metaDir, dumpList);

        // hadoop distributed cache 追加tmpfiles配置文件内容,表示临时文件又增多了
        String hdfsMetaDir = OptionsHelper.convertToFileURL(metaDir.getAbsolutePath());
        if (hdfsMetaDir.startsWith("/")) // note Path on windows is like "d:/../..."
            hdfsMetaDir = "file://" + hdfsMetaDir;
        else
            hdfsMetaDir = "file:///" + hdfsMetaDir;
        logger.info("HDFS meta dir is: " + hdfsMetaDir);

        appendTmpFiles(hdfsMetaDir, conf);
    }

    //删除临时文件
    protected void cleanupTempConfFile(Configuration conf) {
        String tempMetaFileString = conf.get("tmpfiles");
        logger.info("tempMetaFileString is : " + tempMetaFileString);
        if (tempMetaFileString != null) {
            if (tempMetaFileString.startsWith("file://")) {
                tempMetaFileString = tempMetaFileString.substring("file://".length());
                File tempMetaFile = new File(tempMetaFileString);
                if (tempMetaFile.exists()) {
                    try {
                        FileUtils.forceDelete(tempMetaFile.getParentFile());

                    } catch (IOException e) {
                        logger.warn("error when deleting " + tempMetaFile, e);
                    }
                } else {
                    logger.info("" + tempMetaFileString + " does not exist");
                }
            } else {
                logger.info("tempMetaFileString is not starting with file:// :" + tempMetaFileString);
            }
        }
    }

    /**
     * 相当于下载资源服务功能
     * @param kylinConfig 远程服务器
     * @param metaDir 本地获取KylinConfig对象方式
     * @param dumpList 要在本地下载下来的文件
     * @throws IOException
     */
    private void dumpResources(KylinConfig kylinConfig, File metaDir, ArrayList<String> dumpList) throws IOException {
        ResourceStore from = ResourceStore.getStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());//获取KylinConfig对象
        ResourceStore to = ResourceStore.getStore(localConfig);
        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }
    }

    //删除path路径内容
    protected void deletePath(Configuration conf, Path path) throws IOException {
        HadoopUtil.deletePath(conf, path);
    }

    //返回输入源一共有多少M数据
    protected double getTotalMapInputMB() throws ClassNotFoundException, IOException, InterruptedException, JobException {
        if (job == null) {
            throw new JobException("Job is null");
        }

        long mapInputBytes = 0;//计算每一个split数据字节总和

        //对数据源进行拆分,计算每一个split数据字节总和
        InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
        for (InputSplit split : input.getSplits(job)) {
            mapInputBytes += split.getLength();
        }
        if (mapInputBytes == 0) {
            throw new IllegalArgumentException("Map input splits are 0 bytes, something is wrong!");
        }

        //将字节转换成M
        double totalMapInputMB = (double) mapInputBytes / 1024 / 1024;
        return totalMapInputMB;
    }

    //返回该输入源对应的job能有多少个数据块
    protected int getMapInputSplitCount() throws ClassNotFoundException, JobException, IOException, InterruptedException {
        if (job == null) {
            throw new JobException("Job is null");
        }
        InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
        return input.getSplits(job).size();
    }

    public void kill() throws JobException {
        if (job != null) {
            try {
                job.killJob();
            } catch (IOException e) {
                throw new JobException(e);
            }
        }
    }

    public Map<String, String> getInfo() throws JobException {
        if (job != null) {
            Map<String, String> status = new HashMap<String, String>();
            if (null != job.getJobID()) {
                status.put(JobInstance.MR_JOB_ID, job.getJobID().toString());
            }
            if (null != job.getTrackingURL()) {
                status.put(JobInstance.YARN_APP_URL, job.getTrackingURL().toString());
            }

            return status;
        } else {
            throw new JobException("Job is null");
        }
    }

    //获取hadoop的计数器对象
    public Counters getCounters() throws JobException {
        if (job != null) {
            try {
                return job.getCounters();
            } catch (IOException e) {
                throw new JobException(e);
            }
        } else {
            throw new JobException("Job is null");
        }
    }

    public void setAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }

    public Job getJob() {
        return this.job;
    }

    // tells MapReduceExecutable to skip this job
    public boolean isSkipped() {
        return false;
    }

}
