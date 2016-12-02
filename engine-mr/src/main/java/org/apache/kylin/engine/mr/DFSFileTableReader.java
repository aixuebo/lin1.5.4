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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.source.ReadableTable.TableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tables are typically CSV or SEQ file.
 * 读取一个文件,该文件是csv格式或者SequenceFile文件,该文件内容就是table的所有信息
 * @author yangli9
 */
public class DFSFileTableReader implements TableReader {

    private static final Logger logger = LoggerFactory.getLogger(DFSFileTableReader.class);
    private static final char CSV_QUOTE = '"';
    private static final String[] DETECT_DELIMS = new String[] { "\177", "|", "\t", "," };//如果是auto是期望的拆分符号,则可以按照177  |   \t  ,都可以作为拆分符号

    private String filePath;//要读取的文件路径
    private String delim;//一行数据拿什么字段进行拆分不同的列记录
    private RowReader reader;//读取文件的流,一行是一跳记录

    private String curLine;//当前读取的文件一行内容
    private String[] curColumns;//当前行使用拆分符号拆分后的列集合
    private int expectedColumnNumber = -1; // helps delimiter detection 期望一行有多少列

    public DFSFileTableReader(String filePath, int expectedColumnNumber) throws IOException {
        this(filePath, DFSFileTable.DELIM_AUTO, expectedColumnNumber);
    }

    public DFSFileTableReader(String filePath, String delim, int expectedColumnNumber) throws IOException {
        filePath = HadoopUtil.fixWindowsPath(filePath);
        this.filePath = filePath;
        this.delim = delim;
        this.expectedColumnNumber = expectedColumnNumber;

        FileSystem fs = HadoopUtil.getFileSystem(filePath);

        try {
            this.reader = new SeqRowReader(HadoopUtil.getCurrentConfiguration(), fs, filePath);

        } catch (IOException e) {
            if (isExceptionSayingNotSeqFile(e) == false)
                throw e;

            this.reader = new CsvRowReader(fs, filePath);
        }
    }

    private boolean isExceptionSayingNotSeqFile(IOException e) {
        if (e.getMessage() != null && e.getMessage().contains("not a SequenceFile"))
            return true;

        if (e instanceof EOFException) // in case the file is very very small
            return true;

        return false;
    }

    @Override
    public boolean next() throws IOException {
        curLine = reader.nextLine();//读取文件一行内容
        curColumns = null;
        return curLine != null;
    }

    //返回当前行内容
    public String getLine() {
        return curLine;
    }

    //返回一行的列集合
    @Override
    public String[] getRow() {
        if (curColumns == null) {
            if (DFSFileTable.DELIM_AUTO.equals(delim))//是否是auto
                delim = autoDetectDelim(curLine);//如果是auto是期望的拆分符号,则可以按照177  |   \t  ,都可以作为拆分符号,返回真正的拆分符号

            if (delim == null)
                curColumns = new String[] { curLine };//,没有拆分符号,则该行只有一列
            else
                curColumns = split(curLine, delim);//拆分当前行
        }
        return curColumns;
    }

    //拆分一行数据
    private String[] split(String line, String delim) {
        // FIXME CVS line should be parsed considering escapes
        String[] str = StringSplitter.split(line, delim);

        // un-escape CSV
        if (DFSFileTable.DELIM_COMMA.equals(delim)) {//如果拆分行是逗号
            for (int i = 0; i < str.length; i++) {
                str[i] = unescapeCsv(str[i]);
            }
        }

        return str;
    }

    //去除包裹列value内容的双引号
    private String unescapeCsv(String str) {
        if (str == null || str.length() < 2)
            return str;

        str = StringEscapeUtils.unescapeCsv(str);

        // unescapeCsv may not remove the outer most quotes
        if (str.charAt(0) == CSV_QUOTE && str.charAt(str.length() - 1) == CSV_QUOTE)//移除包裹列value内容的双引号
            str = str.substring(1, str.length() - 1);

        return str;
    }

    @Override
    public void close() throws IOException {
        if (reader != null)
            reader.close();
    }

    //如果是auto是期望的拆分符号,则可以按照177  |   \t  ,都可以作为拆分符号
    private String autoDetectDelim(String line) {
        if (expectedColumnNumber > 0) {
            for (String delim : DETECT_DELIMS) {
                if (StringSplitter.split(line, delim).length == expectedColumnNumber) {//只要拆分的结果与期望的列相同即可返回拆分符号
                    logger.info("Auto detect delim to be '" + delim + "', split line to " + expectedColumnNumber + " columns -- " + line);
                    return delim;
                }
            }
        }

        logger.info("Auto detect delim to be null, will take THE-WHOLE-LINE as a single value, for " + filePath);
        return null;
    }

    // ============================================================================

    private interface RowReader extends Closeable {
        String nextLine() throws IOException; // return null on EOF
    }

    private class SeqRowReader implements RowReader {
        Reader reader;//读取SequenceFile文件的流
        Writable key;//可序列化的Key
        Text value;//序列化后的value

        SeqRowReader(Configuration hconf, FileSystem fs, String path) throws IOException {
            reader = new Reader(hconf, SequenceFile.Reader.file(new Path(path)));//SequenceFile的Reader去读取path路径下内容
            key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), hconf);
            value = new Text();
        }

        //返回每一行的value信息
        @Override
        public String nextLine() throws IOException {
            boolean hasNext = reader.next(key, value);
            if (hasNext)
                return Bytes.toString(value.getBytes(), 0, value.getLength());
            else
                return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    //csv方式的文件流
    private class CsvRowReader implements RowReader {
        BufferedReader reader;

        CsvRowReader(FileSystem fs, String path) throws IOException {
            FSDataInputStream in = fs.open(new Path(path));
            reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        }

        //每一行是一条记录
        @Override
        public String nextLine() throws IOException {
            return reader.readLine();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

}
