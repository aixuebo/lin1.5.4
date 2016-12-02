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

package org.apache.kylin.engine.mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.source.ReadableTable;

/**
 * 读取一个文件,该文件是csv格式或者SequenceFile文件,该文件内容就是table的所有信息
 */
public class DFSFileTable implements ReadableTable {

    public static final String DELIM_AUTO = "auto";//如果是auto是期望的拆分符号,则可以按照177  |   \t  ,都可以作为拆分符号
    public static final String DELIM_COMMA = ",";

    String path;
    String delim;
    int nColumns;

    public DFSFileTable(String path, int nColumns) {
        this(path, DELIM_AUTO, nColumns);
    }

    public DFSFileTable(String path, String delim, int nColumns) {
        this.path = path;
        this.delim = delim;
        this.nColumns = nColumns;
    }

    public String getColumnDelimeter() {
        return delim;
    }

    //如何读取
    @Override
    public TableReader getReader() throws IOException {
        return new DFSFileTableReader(path, delim, nColumns);
    }

    //返回表示该表是否有变化的标志
    @Override
    public TableSignature getSignature() throws IOException {
        try {
            Pair<Long, Long> sizeAndLastModified = getSizeAndLastModified(path);
            return new TableSignature(path, sizeAndLastModified.getFirst(), sizeAndLastModified.getSecond());
        } catch (FileNotFoundException ex) {
            return null;
        }
    }

    @Override
    public String toString() {
        return path;
    }

    //返回该路径下文件的最后修改时间和总字节大小
    public static Pair<Long, Long> getSizeAndLastModified(String path) throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem(path);

        // get all contained files if path is directory
        ArrayList<FileStatus> allFiles = new ArrayList<>();
        FileStatus status = fs.getFileStatus(new Path(path));
        if (status.isFile()) {
            allFiles.add(status);
        } else {
            FileStatus[] listStatus = fs.listStatus(new Path(path));
            allFiles.addAll(Arrays.asList(listStatus));
        }

        long size = 0;//总字节
        long lastModified = 0;//最后修改时间
        for (FileStatus file : allFiles) {
            size += file.getLen();
            lastModified = Math.max(lastModified, file.getModificationTime());
        }

        return Pair.newPair(size, lastModified);
    }
}
