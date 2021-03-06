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

package org.apache.kylin.cube.kv;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * 对cube的每一个列对应的字典等描述信息的映射
 */
public class CubeDimEncMap implements IDimensionEncodingMap {

    private static final Logger logger = LoggerFactory.getLogger(CubeDimEncMap.class);

    final private CubeDesc cubeDesc;
    final private CubeSegment seg;
    final private Map<TblColRef, Dictionary<String>> dictionaryMap;//描述每一个字段对应的字典对象的映射
    final private Map<TblColRef, DimensionEncoding> encMap = Maps.newHashMap();//描述每一个字段对应的编码映射

    public CubeDimEncMap(CubeSegment seg) {
        this.cubeDesc = seg.getCubeDesc();
        this.seg = seg;
        this.dictionaryMap = null;
    }

    public CubeDimEncMap(CubeDesc cubeDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.cubeDesc = cubeDesc;
        this.seg = null;
        this.dictionaryMap = dictionaryMap;
    }

    //如何对该字段的值进行编码
    @Override
    public DimensionEncoding get(TblColRef col) {
        DimensionEncoding result = encMap.get(col);
        if (result == null) {
            RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(col);//在rowkey中获取该列对象
            if (colDesc.isUsingDictionary()) {//说明该列使用了字典方式编码
                // special dictionary encoding
                Dictionary<String> dict = getDictionary(col);//获取该列对应的字典
                if (dict == null) {
                    logger.warn("No dictionary found for dict-encoding column " + col + ", segment " + seg);
                    result = new FixedLenDimEnc(0);
                } else {
                    result = new DictionaryDimEnc(dict);//使用字典进行编码
                }
            } else {//说明该列直接编码,不需要字典
                // normal case
                result = DimensionEncodingFactory.create(colDesc.getEncodingName(), colDesc.getEncodingArgs());
            }
            encMap.put(col, result);
        }
        return result;
    }

    //返回该字段对应的字典对象
    @Override
    public Dictionary<String> getDictionary(TblColRef col) {
        if (seg == null)
            return dictionaryMap.get(col);
        else
            return seg.getDictionary(col);
    }

}
