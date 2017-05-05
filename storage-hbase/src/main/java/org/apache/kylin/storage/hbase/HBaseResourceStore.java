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

package org.apache.kylin.storage.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

//使用hbase 存储元数据
//rowkey是元数据存储的path,比如 rowkey是/cube/olap_basic_v2_cube_test.json
//scan "kylin_metadata",{LIMIT=>5}  count "kylin_metadata",{LIMIT=>5}
public class HBaseResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HBaseResourceStore.class);

    private static final String DEFAULT_TABLE_NAME = "kylin_metadata";//默认hbase的table

    private static final String FAMILY = "f";
    private static final byte[] B_FAMILY = Bytes.toBytes(FAMILY);

    private static final String COLUMN = "c";
    private static final byte[] B_COLUMN = Bytes.toBytes(COLUMN);

    private static final String COLUMN_TS = "t";
    private static final byte[] B_COLUMN_TS = Bytes.toBytes(COLUMN_TS);

    final String tableNameBase;
    final String hbaseUrl;//hbase的连接串

    private HConnection getConnection() throws IOException {
        return HBaseConnection.get(hbaseUrl);
    }

    public HBaseResourceStore(KylinConfig kylinConfig) throws IOException {
        super(kylinConfig);

        String metadataUrl = kylinConfig.getMetadataUrl();
        // split TABLE@HBASE_URL 使用hbase的table@hbase连接串
        int cut = metadataUrl.indexOf('@');
        tableNameBase = cut < 0 ? DEFAULT_TABLE_NAME : metadataUrl.substring(0, cut);
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);

        createHTableIfNeeded(getAllInOneTableName());
    }

    //如果需要的话创建hbase数据库
    private void createHTableIfNeeded(String tableName) throws IOException {
        HBaseConnection.createHTableIfNeeded(getConnection(), tableName, FAMILY);
    }

    //存储hbase数据的表
    private String getAllInOneTableName() {
        return tableNameBase;
    }

    //是否存在该路径对应的数据,即rowkey存在则返回true
    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Result r = getFromHTable(resPath, false, false);
        return r != null;
    }

    //返回前缀路径为folderPath下所有的文件,返回rowkey按照顺序排序
    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        final TreeSet<String> result = new TreeSet<>();

        //KeyOnlyFilter表示仅仅返回rowkey,value被设置为空字节
        visitFolder(folderPath, new KeyOnlyFilter(), new FolderVisitor() {
            @Override
            public void visit(String childPath, String fullPath, Result hbaseResult) {
                result.add(childPath);
            }
        });
        // return null to indicate not a folder
        return result.isEmpty() ? null : result;
    }

    //扫描folderPath开头的rowkey数据
    private void visitFolder(String folderPath, Filter filter, FolderVisitor visitor) throws IOException {
        //确保folderPath必须以/开头,以/结尾
        assert folderPath.startsWith("/");
        String lookForPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        byte[] startRow = Bytes.toBytes(lookForPrefix);
        byte[] endRow = Bytes.toBytes(lookForPrefix);
        endRow[endRow.length - 1]++;//因为不能开始和结束相同,因此将最后一个字节增加1个

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        Scan scan = new Scan(startRow, endRow);//范围内扫描
        if ((filter != null && filter instanceof KeyOnlyFilter) == false) {
            scan.addColumn(B_FAMILY, B_COLUMN_TS);
            scan.addColumn(B_FAMILY, B_COLUMN);
        }

        //设置过滤器
        if (filter != null) {
            scan.setFilter(filter);
        }

        tuneScanParameters(scan);//设置scan的参数

        try {
            ResultScanner scanner = table.getScanner(scan);//真正去扫描数据
            for (Result r : scanner) {//获取扫描的结果
                String path = Bytes.toString(r.getRow());//rowkey对应的路径
                assert path.startsWith(lookForPrefix);
                int cut = path.indexOf('/', lookForPrefix.length());
                String child = cut < 0 ? path : path.substring(0, cut);
                visitor.visit(child, path, r);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //设置scan的参数
    private void tuneScanParameters(Scan scan) {
        // divide by 10 as some resource like dictionary or snapshot can be very large
        // scan.setCaching(kylinConfig.getHBaseScanCacheRows() / 10);
        scan.setCaching(kylinConfig.getHBaseScanCacheRows());

        scan.setMaxResultSize(kylinConfig.getHBaseScanMaxResultSize());
        scan.setCacheBlocks(true);
    }

    interface FolderVisitor {
        /**
         * 查询每一个hbase的数据
         * @param childPath fullPath上符合条件的第一个path段落  此时主要将rowkey对应的字节数组转换成字符串了
         * @param fullPath 此时查询的rowkey对应的全路径
         * @param hbaseResult hbase上查询的结果集
         */
        void visit(String childPath, String fullPath, Result hbaseResult) throws IOException;
    }

    //返回前缀路径为folderPath下所有的文件,过滤条件是更新时间在timeStart和timeEndExclusive之间
    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        FilterList filter = generateTimeFilterList(timeStart, timeEndExclusive);
        final List<RawResource> result = Lists.newArrayList();
        try {
            visitFolder(folderPath, filter, new FolderVisitor() {
                @Override
                public void visit(String childPath, String fullPath, Result hbaseResult) throws IOException {
                    // is a direct child (not grand child)?
                    if (childPath.equals(fullPath))
                        result.add(new RawResource(getInputStream(childPath, hbaseResult), getTimestamp(hbaseResult)));
                }
            });
        } catch (IOException e) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw e;
        }
        return result;
    }

    //根据时间戳查询,时间戳范围是[timeStart,timeEndExclusive)
    private FilterList generateTimeFilterList(long timeStart, long timeEndExclusive) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (timeStart != Long.MIN_VALUE) {
            SingleColumnValueFilter timeStartFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(timeStart));
            filterList.addFilter(timeStartFilter);
        }
        if (timeEndExclusive != Long.MAX_VALUE) {
            SingleColumnValueFilter timeEndFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS, CompareFilter.CompareOp.LESS, Bytes.toBytes(timeEndExclusive));
            filterList.addFilter(timeEndFilter);
        }
        return filterList.getFilters().size() == 0 ? null : filterList;
    }

    //读取结果内容,转换成字节流
    private InputStream getInputStream(String resPath, Result r) throws IOException {
        if (r == null) {
            return null;
        }
        byte[] value = r.getValue(B_FAMILY, B_COLUMN);//获取hbase的内容-----字节数组
        if (value.length == 0) {//说明内容没有,因为太大了，所以存储成文件了
            Path redirectPath = bigCellHDFSPath(resPath);
            Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
            FileSystem fileSystem = FileSystem.get(hconf);

            return fileSystem.open(redirectPath);
        } else {
            return new ByteArrayInputStream(value);
        }
    }

    //读取时间戳
    private long getTimestamp(Result r) {
        if (r == null || r.getValue(B_FAMILY, B_COLUMN_TS) == null) {
            return 0;
        } else {
            return Bytes.toLong(r.getValue(B_FAMILY, B_COLUMN_TS));
        }
    }

    //查询rowkey对应的内容和最后更改时间
    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        Result r = getFromHTable(resPath, true, true);
        if (r == null)
            return null;
        else
            return new RawResource(getInputStream(resPath, r), getTimestamp(r));//返回字节数组的原始内容
    }

    //查询数据库,返回最后更改时间
    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        return getTimestamp(getFromHTable(resPath, false, true));
    }

    //存储内容到hbase,ts是最新的更改时间
    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        IOUtils.copy(content, bout);
        bout.close();

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            byte[] row = Bytes.toBytes(resPath);//rowkey
            Put put = buildPut(resPath, ts, row, bout.toByteArray(), table);

            table.put(put);
            table.flushCommits();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //保存的时候要校验老的时间戳是否正确,返回新的时间戳
    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            byte[] row = Bytes.toBytes(resPath);//rowkey
            byte[] bOldTS = oldTS == 0 ? null : Bytes.toBytes(oldTS);
            Put put = buildPut(resPath, newTS, row, content, table);

            boolean ok = table.checkAndPut(row, B_FAMILY, B_COLUMN_TS, bOldTS, put);//根据上一次的时间戳去更新数据
            logger.debug("Update row " + resPath + " from oldTs: " + oldTS + ", to newTs: " + newTS + ", operation result: " + ok);
            if (!ok) {//更新失败
                long real = getResourceTimestampImpl(resPath);
                throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but it is " + real);//通知最后更改时间是多少,old时间是有问题的
            }

            table.flushCommits();

            return newTS;
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //删除数据
    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            boolean hdfsResourceExist = false;//hdfs上是否被存储成文件了
            Result result = internalGetFromHTable(table, resPath, true, false);
            if (result != null) {
                byte[] value = result.getValue(B_FAMILY, B_COLUMN);
                if (value != null && value.length == 0) {
                    hdfsResourceExist = true;//说明存储文件了
                }
            }

            Delete del = new Delete(Bytes.toBytes(resPath));//删除该rowkey
            table.delete(del);
            table.flushCommits();

            if (hdfsResourceExist) { // remove hdfs cell value  删除HDFS上的文件
                Path redirectPath = bigCellHDFSPath(resPath);
                Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
                FileSystem fileSystem = FileSystem.get(hconf);

                if (fileSystem.exists(redirectPath)) {
                    fileSystem.delete(redirectPath, true);
                }
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //关于资源存储的可以被人读的描述
    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return getAllInOneTableName() + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    /**
     * 查询hbase数据库
     * @param path rowkey
     * @param fetchContent 是否抓去字段内容
     * @param fetchTimestamp 是否抓去最后更新时间
     */
    private Result getFromHTable(String path, boolean fetchContent, boolean fetchTimestamp) throws IOException {
        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            return internalGetFromHTable(table, path, fetchContent, fetchTimestamp);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //查询hbase中rowkey对应内容
    //path路径作为rowkey
    private Result internalGetFromHTable(HTableInterface table, String path, boolean fetchContent, boolean fetchTimestamp) throws IOException {
        byte[] rowkey = Bytes.toBytes(path);

        Get get = new Get(rowkey);

        if (!fetchContent && !fetchTimestamp) {
            get.setCheckExistenceOnly(true);//仅仅校验是否存在该rowkey
        } else {
            if (fetchContent)
                get.addColumn(B_FAMILY, B_COLUMN);
            if (fetchTimestamp)
                get.addColumn(B_FAMILY, B_COLUMN_TS);
        }

        Result result = table.get(get);
        boolean exists = result != null && (!result.isEmpty() || (result.getExists() != null && result.getExists()));
        return exists ? result : null;
    }

    /**
     * 创建Put对象
     * @param resPath 该路径也是hbase的rowkey
     * @param ts 列族的一个列c对应的更新时间字节数组
     * @param row rowkey对应的字节数组
     * @param content 列族的一个列c对应的值的字节数组
     * @param table HBase对应的table
     * @return
     * @throws IOException
     */
    private Put buildPut(String resPath, long ts, byte[] row, byte[] content, HTableInterface table) throws IOException {
        int kvSizeLimit = this.kylinConfig.getHBaseKeyValueSize();
        if (content.length > kvSizeLimit) {//要存储到磁盘上
            writeLargeCellToHdfs(resPath, content, table);//将二进制字节数组写入到HDFS上,此时写入到文件是因为字节数组太大了
            content = BytesUtil.EMPTY_BYTE_ARRAY;//空的字节数组
        }

        Put put = new Put(row);
        put.add(B_FAMILY, B_COLUMN, content);
        put.add(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(ts));

        return put;
    }

    //大的列值对应存储到hdfs的目录
    public Path bigCellHDFSPath(String resPath) {
        String hdfsWorkingDirectory = this.kylinConfig.getHdfsWorkingDirectory();
        Path redirectPath = new Path(hdfsWorkingDirectory, "resources" + resPath);
        return redirectPath;
    }

    //将二进制字节数组写入到HDFS上,此时写入到文件是因为字节数组太大了
    private Path writeLargeCellToHdfs(String resPath, byte[] largeColumn, HTableInterface table) throws IOException {
        Path redirectPath = bigCellHDFSPath(resPath);
        Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
        FileSystem fileSystem = FileSystem.get(hconf);

        if (fileSystem.exists(redirectPath)) {//删除已经存在的文件
            fileSystem.delete(redirectPath, true);
        }

        FSDataOutputStream out = fileSystem.create(redirectPath);

        try {
            out.write(largeColumn);
        } finally {
            IOUtils.closeQuietly(out);
        }

        return redirectPath;
    }
}
