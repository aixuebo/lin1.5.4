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

package org.apache.kylin.source;

import java.io.Closeable;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 如何读取一个table数据的接口
 */
public interface ReadableTable {

    /** Returns a reader to read the table. 返回一个读取table的对象*/
    public TableReader getReader() throws IOException;

    /** Used to detect table modifications mainly. Return null in case table does not exist.
     * 被使用去检测table修改情况
     * 返回null,说明table不存在
     **/
    public TableSignature getSignature() throws IOException;

    //如何读取table的接口
    public interface TableReader extends Closeable {

        /** Move to the next row, return false if no more record.
         * 一行一行移动,如果返回false,说明没有数据了
         **/
        public boolean next() throws IOException;

        /** Get the current row.
         * 获取当前行的数据集合
         **/
        public String[] getRow();

    }

    // ============================================================================

    //记录一个文件路径 以及大小和最后修改时间,用于判断该文件是否有变更
    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public class TableSignature {

        @JsonProperty("path")
        private String path;
        @JsonProperty("size")
        private long size;
        @JsonProperty("last_modified_time")
        private long lastModifiedTime;

        // for JSON serialization
        public TableSignature() {
        }

        public TableSignature(String path, long size, long lastModifiedTime) {
            super();
            this.path = path;
            this.size = size;
            this.lastModifiedTime = lastModifiedTime;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public void setLastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
        }

        public String getPath() {
            return path;
        }

        public long getSize() {
            return size;
        }

        public long getLastModifiedTime() {
            return lastModifiedTime;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (lastModifiedTime ^ (lastModifiedTime >>> 32));
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            result = prime * result + (int) (size ^ (size >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TableSignature other = (TableSignature) obj;
            if (lastModifiedTime != other.lastModifiedTime)
                return false;
            if (path == null) {
                if (other.path != null)
                    return false;
            } else if (!path.equals(other.path))
                return false;
            if (size != other.size)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "FileSignature [path=" + path + ", size=" + size + ", lastModifiedTime=" + lastModifiedTime + "]";
        }

    }

}
