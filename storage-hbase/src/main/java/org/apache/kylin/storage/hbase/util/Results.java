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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.common.util.Bytes;

/**
 * The helper class is introduced because {@link Result#binarySearch(Cell[], byte[], int, int, byte[], int, int)} 
 * is found to be problematic in concurrent environments, and unfortunately  {@link Result#getValueAsByteBuffer(byte[], byte[])} 
 * calls it.
 */
public class Results {

    /**
     *
     * @param hbaseRow 查询结果
     * @param cf 要查询的是哪个列族,此时指代一个列族
     * @param cq 要查询的是哪个属性
     * @return 返回该列族+属性对应的属性值对应的字节数组
     */
    public static ByteBuffer getValueAsByteBuffer(Result hbaseRow, byte[] cf, byte[] cq) {
        List<Cell> cells = hbaseRow.listCells();
        if (cells == null || cells.size() == 0) {
            return null;
        } else {
            for (Cell c : cells) {//循环每一个cell
                if (Bytes.compareTo(cf, 0, cf.length, c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength()) == 0 && //说明该cell的列族是参数给的列族
                        Bytes.compareTo(cq, 0, cq.length, c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength()) == 0) {//属性也对上了
                    return ByteBuffer.wrap(c.getValueArray(), c.getValueOffset(), c.getValueLength());//获取该属性对应的属性值
                }
            }
        }
        return null;
    }

}
