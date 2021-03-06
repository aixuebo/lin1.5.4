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

package org.apache.kylin.measure;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.measure.dim.DimCountDistinctMeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.measure.raw.RawMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Factory for MeasureType.
 * 
 * The factory registers itself by claiming the aggregation function and data type it supports,
 * to match a measure descriptor in cube definition.
 * 
 * E.g. HyperLogLog measure type claims "COUNT_DISCINT" as function and "hllc" as data type to
 * match measure descriptor:
 * <pre>
  {
    "name" : "SELLER_CNT_HLL",
    "function" : {
      "expression" : "COUNT_DISTINCT",        <----  function name
      "parameter" : {
        "type" : "column",
        "value" : "SELLER_ID",
        "next_parameter" : null
      },
      "returntype" : "hllc(10)"               <----  data type
    }
  }
</pre>
 * 
 * @param <T> the Java type of aggregation data object, e.g. HyperLogLogPlusCounter
 */
abstract public class MeasureTypeFactory<T> {

    private static final Logger logger = LoggerFactory.getLogger(MeasureTypeFactory.class);

    /**
     * Create a measure type with specified aggregation function and data type.
     * 
     * @param funcName should always match this factory's claim <code>getAggrFunctionName()</code>
     * @param dataType should always match this factory's claim <code>getAggrDataTypeName()</code>
     */
    abstract public MeasureType<T> createMeasureType(String funcName, DataType dataType);

    /** Return the aggregation function this factory supports, like "COUNT_DISTINCT"
     * 名字必须是大写的字母
     * 表示聚合函数的名字,比如SUM
     **/
    abstract public String getAggrFunctionName();

    /** Return the aggregation data type name this factory supports, like "hllc"
     * 函数的返回类型,必须小写字母表示
     **/
    abstract public String getAggrDataTypeName();

    /** Return the Serializer for aggregation data object. Note a Serializer implementation must be thread-safe!
     * getAggrDataTypeName类型的反序列化对象
     **/
    abstract public Class<? extends DataTypeSerializer<T>> getAggrDataTypeSerializer();

    // ============================================================================

    //key是函数名称getAggrFunctionName,比如SUM,VALUE是HLLCMeasureType.Factory()方式
    private static Map<String, List<MeasureTypeFactory<?>>> factories = Maps.newHashMap();
    private static List<MeasureTypeFactory<?>> defaultFactory = Lists.newArrayListWithCapacity(2);//默认的基本工厂

    static {
        init();
    }

    public static synchronized void init() {
        if (!factories.isEmpty()) {
            return;
        }

        List<MeasureTypeFactory<?>> factoryInsts = Lists.newArrayList();

        // five built-in advanced measure types
        factoryInsts.add(new HLLCMeasureType.Factory());
        factoryInsts.add(new BitmapMeasureType.Factory());
        factoryInsts.add(new TopNMeasureType.Factory());
        factoryInsts.add(new RawMeasureType.Factory());
        factoryInsts.add(new ExtendedColumnMeasureType.Factory());

        logger.info("Checking custom measure types from kylin config");

        try {
            //自定义的度量类型的class全路径集合
            for (String customFactory : KylinConfig.getInstanceFromEnv().getCubeCustomMeasureTypes().values()) {
                try {
                    logger.info("Checking custom measure types from kylin config: " + customFactory);
                    factoryInsts.add((MeasureTypeFactory<?>) Class.forName(customFactory).newInstance());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unrecognized MeasureTypeFactory classname: " + customFactory, e);
                }
            }
        } catch (KylinConfigCannotInitException e) {
            logger.warn("Will not add custome MeasureTypeFactory as KYLIN_CONF nor KYLIN_HOME is set");
        }

        // register factories & data type serializers
        for (MeasureTypeFactory<?> factory : factoryInsts) {
            String funcName = factory.getAggrFunctionName();
            if (funcName.equals(funcName.toUpperCase()) == false) //名字必须是大写的字母
                throw new IllegalArgumentException("Aggregation function name '" + funcName + "' must be in upper case");
            String dataTypeName = factory.getAggrDataTypeName();
            if (dataTypeName.equals(dataTypeName.toLowerCase()) == false)
                throw new IllegalArgumentException("Aggregation data type name '" + dataTypeName + "' must be in lower case");
            Class<? extends DataTypeSerializer<?>> serializer = factory.getAggrDataTypeSerializer();

            logger.info("registering " + dataTypeName);
            DataType.register(dataTypeName);
            DataTypeSerializer.register(dataTypeName, serializer);//注册数据类型和数据类型对应的序列化类

            List<MeasureTypeFactory<?>> list = factories.get(funcName);
            if (list == null)
                factories.put(funcName, list = Lists.newArrayListWithCapacity(2));
            list.add(factory);
        }

        defaultFactory.add(new BasicMeasureType.Factory());
    }

    //通过函数名字和函数的返回值,可以确定唯一的MeasureType对象
    public static MeasureType<?> create(String funcName, String dataType) {
        return create(funcName, DataType.getType(dataType));
    }

    public static MeasureType<?> createNoRewriteFieldsMeasureType(String funcName, DataType dataType) {
        // currently only has DimCountDistinctAgg
        if (funcName.equalsIgnoreCase("COUNT_DISTINCT")) {
            return new DimCountDistinctMeasureType.DimCountDistinctMeasureTypeFactory().createMeasureType(funcName, dataType);
        }

        throw new UnsupportedOperationException("No measure type found.");
    }

    //通过函数名字和函数的返回值,可以确定唯一的MeasureType对象
    public static MeasureType<?> create(String funcName, DataType dataType) {
        funcName = funcName.toUpperCase();

        List<MeasureTypeFactory<?>> factory = factories.get(funcName);
        if (factory == null)
            factory = defaultFactory;

        // a special case where in early stage of sql parsing, the data type is unknown; only needRewrite() is required at that stage
        if (dataType == null) {
            return new NeedRewriteOnlyMeasureType(funcName, factory);
        }

        // the normal case, only one factory for a function
        if (factory.size() == 1) {
            return factory.get(0).createMeasureType(funcName, dataType);
        }

        // sometimes multiple factories are registered for the same function, then data types must tell them apart
        for (MeasureTypeFactory<?> f : factory) {
            if (f.getAggrDataTypeName().equals(dataType.getName()))
                return f.createMeasureType(funcName, dataType);
        }
        throw new IllegalStateException();
    }

    @SuppressWarnings("rawtypes")
    private static class NeedRewriteOnlyMeasureType extends MeasureType {

        private Boolean needRewrite;

        public NeedRewriteOnlyMeasureType(String funcName, List<MeasureTypeFactory<?>> factory) {
            for (MeasureTypeFactory<?> f : factory) {
                boolean b = f.createMeasureType(funcName, null).needRewrite();
                if (needRewrite == null)
                    needRewrite = Boolean.valueOf(b);
                else if (needRewrite.booleanValue() != b)
                    throw new IllegalStateException("needRewrite() of factorys " + factory + " does not have consensus");
            }
        }

        @Override
        public MeasureIngester newIngester() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MeasureAggregator newAggregator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean needRewrite() {
            return needRewrite;
        }

        @Override
        public Class getRewriteCalciteAggrFunctionClass() {
            throw new UnsupportedOperationException();
        }

    }
}
