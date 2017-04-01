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

package org.apache.kylin.metadata.model;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author xduo
 * 
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc {

    public static enum PartitionType {
        APPEND, //追加
        UPDATE_INSERT // not used since 0.7.1 更新 和 新增
    }

    //日期相关联的属性name以及属性对象,以及格式
    @JsonProperty("partition_date_column")
    private String partitionDateColumn;//日期列  database.table.column组成格式
    private TblColRef partitionDateColumnRef;//partitionDateColumn 日期列对应的对象
    @JsonProperty("partition_date_format")
    private String partitionDateFormat = DateFormat.DEFAULT_DATE_PATTERN;//yyyy-MM-dd

    //时间相关的属性name以及格式
    @JsonProperty("partition_time_column")
    private String partitionTimeColumn;//时间列
    @JsonProperty("partition_time_format")
    private String partitionTimeFormat = DateFormat.DEFAULT_TIME_PATTERN;//HH:mm:ss

    //已经意义不大了,因为每次分区都是通过api方式调用的,因此不需要偏移量了
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;//Deprecated

    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;//追加

    //默认分区实现类
    @JsonProperty("partition_condition_builder")
    private String partitionConditionBuilderClz = DefaultPartitionConditionBuilder.class.getName();//具体的class全路径
    private IPartitionConditionBuilder partitionConditionBuilder;//partitionConditionBuilderClz对应的class的实例类


    public void init(Map<String, TableDesc> tables) {
        if (StringUtils.isEmpty(partitionDateColumn))//必须要有日期分区列
            return;

        partitionDateColumn = partitionDateColumn.toUpperCase();//大写

        String[] columns = StringSplitter.split(partitionDateColumn, ".");

        if (null != columns && columns.length == 3) {
            String tableName = columns[0].toUpperCase() + "." + columns[1].toUpperCase();//说明是数据库.table.column形式

            TableDesc table = tables.get(tableName);//列所属表
            ColumnDesc col = table.findColumnByName(columns[2]);//找到列对象
            if (col != null) {
                partitionDateColumnRef = new TblColRef(col);
            } else {
                throw new IllegalStateException("The column '" + partitionDateColumn + "' provided in 'partition_date_column' doesn't exist.");
            }
        } else {
            throw new IllegalStateException("The 'partition_date_column' format is invalid: " + partitionDateColumn + ", it should be {db}.{table}.{column}.");
        }

        partitionConditionBuilder = (IPartitionConditionBuilder) ClassUtil.newInstance(partitionConditionBuilderClz);//实例化
    }

    //判断是否是整数类型 ymd,因为日期对应的是int类型的
    public boolean partitionColumnIsYmdInt() {
        if (partitionDateColumnRef == null)
            return false;
        
        DataType type = partitionDateColumnRef.getType();//判断该分区属性的类型
        return type.isInt();
    }

    //判断是否是bigint类型,因为日期可以对应long类型存储
    public boolean partitionColumnIsTimeMillis() {
        if (partitionDateColumnRef == null)
            return false;
        
        DataType type = partitionDateColumnRef.getType();
        return type.isBigInt();
    }

    //true表示有日期分区,false表示不支持分区
    public boolean isPartitioned() {
        return partitionDateColumnRef != null;
    }

    //获取分区字段
    public String getPartitionDateColumn() {
        return partitionDateColumn;
    }

    public void setPartitionDateColumn(String partitionDateColumn) {
        this.partitionDateColumn = partitionDateColumn;
    }

    public String getPartitionTimeColumn() {
        return partitionTimeColumn;
    }

    public void setPartitionTimeColumn(String partitionTimeColumn) {
        this.partitionTimeColumn = partitionTimeColumn;
    }

    @Deprecated
    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    @Deprecated
    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public String getPartitionDateFormat() {
        return partitionDateFormat;
    }

    public void setPartitionDateFormat(String partitionDateFormat) {
        this.partitionDateFormat = partitionDateFormat;
    }

    public String getPartitionTimeFormat() {
        return partitionTimeFormat;
    }

    public void setPartitionTimeFormat(String partitionTimeFormat) {
        this.partitionTimeFormat = partitionTimeFormat;
    }

    public PartitionType getCubePartitionType() {
        return partitionType;
    }

    public void setCubePartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public IPartitionConditionBuilder getPartitionConditionBuilder() {
        return partitionConditionBuilder;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

    // ============================================================================

    public static interface IPartitionConditionBuilder {
        //根据分区字段信息以及分区的日期区间,转换sql的一个where条件,即字符串返回
        String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias) {
            StringBuilder builder = new StringBuilder();
            String partitionDateColumnName = partDesc.getPartitionDateColumn();//分区的日期列
            String partitionTimeColumnName = partDesc.getPartitionTimeColumn();//分区的时间列

            if (partDesc.partitionColumnIsYmdInt()) {//说明日期列数据类型是int
                buildSingleColumnRangeCondAsYmdInt(builder, partitionDateColumnName, startInclusive, endExclusive, tableAlias);
            } else if (partDesc.partitionColumnIsTimeMillis()) {//说明日期列数据类型是long
                buildSingleColumnRangeCondAsTimeMillis(builder, partitionDateColumnName, startInclusive, endExclusive, tableAlias);
            } else if (partitionDateColumnName != null && partitionTimeColumnName == null) {//说明仅有日期分区
                buildSingleColumnRangeCondition(builder, partitionDateColumnName, startInclusive, endExclusive, partDesc.getPartitionDateFormat(), tableAlias);
            } else if (partitionDateColumnName == null && partitionTimeColumnName != null) {//说明只有时间分区
                buildSingleColumnRangeCondition(builder, partitionTimeColumnName, startInclusive, endExclusive, partDesc.getPartitionTimeFormat(), tableAlias);
            } else if (partitionDateColumnName != null && partitionTimeColumnName != null) {//说明日期和时间两个字段进行分区
                buildMultipleColumnRangeCondition(builder, partitionDateColumnName, partitionTimeColumnName, startInclusive, endExclusive, partDesc.getPartitionDateFormat(), partDesc.getPartitionTimeFormat(), tableAlias);
            }

            return builder.toString();
        }

        /**
         * Convert to use table alias
         * 使用别名替换columnName的表名
         */
        private static String replaceColumnNameWithAlias(String columnName, Map<String, String> tableAlias) {
            int indexOfDot = columnName.lastIndexOf(".");
            if (indexOfDot > 0) {
                String partitionTableName = columnName.substring(0, indexOfDot);//获取表名字
                if (tableAlias != null && tableAlias.containsKey(partitionTableName))//替换别名
                    columnName = tableAlias.get(partitionTableName) + columnName.substring(indexOfDot);
            }
            return columnName;
        }

        //说明日期列数据类型是long
        private static void buildSingleColumnRangeCondAsTimeMillis(StringBuilder builder, String partitionColumnName, long startInclusive, long endExclusive, Map<String, String> tableAlias) {
            partitionColumnName = replaceColumnNameWithAlias(partitionColumnName, tableAlias);
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= " + startInclusive);
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < " + endExclusive);
        }

        //说明日期列数据类型是int
        private static void buildSingleColumnRangeCondAsYmdInt(StringBuilder builder, String partitionColumnName, long startInclusive, long endExclusive, Map<String, String> tableAlias) {
            partitionColumnName = replaceColumnNameWithAlias(partitionColumnName, tableAlias);
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= " + DateFormat.formatToDateStr(startInclusive, DateFormat.COMPACT_DATE_PATTERN));
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < " + DateFormat.formatToDateStr(endExclusive, DateFormat.COMPACT_DATE_PATTERN));
        }

        //定义了日期格式partitionColumnDateFormat
        //说明仅有日期分区 或者 时间分区
        private static void buildSingleColumnRangeCondition(StringBuilder builder, String partitionColumnName, long startInclusive, long endExclusive, String partitionColumnDateFormat, Map<String, String> tableAlias) {
            partitionColumnName = replaceColumnNameWithAlias(partitionColumnName, tableAlias);
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'");
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'");
        }

        //说明日期和时间两个字段进行分区
        //即 ( (日期=startInclusive and 时间>= startInclusive) or 日期 > startInclusive ) and （ (日期=endExclusive and 时间< endExclusive) or 日期 < endExclusive）
        private static void buildMultipleColumnRangeCondition(StringBuilder builder, String partitionDateColumnName, String partitionTimeColumnName, long startInclusive, long endExclusive, String partitionColumnDateFormat, String partitionColumnTimeFormat, Map<String, String> tableAlias) {
            partitionDateColumnName = replaceColumnNameWithAlias(partitionDateColumnName, tableAlias);
            partitionTimeColumnName = replaceColumnNameWithAlias(partitionTimeColumnName, tableAlias);
            if (startInclusive > 0) {
                builder.append("(");
                builder.append("(");
                builder.append(partitionDateColumnName + " = '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'").append(" AND ").append(partitionTimeColumnName + " >= '" + DateFormat.formatToDateStr(startInclusive, partitionColumnTimeFormat) + "'");
                builder.append(")");
                builder.append(" OR ");
                builder.append("(");
                builder.append(partitionDateColumnName + " > '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'");
                builder.append(")");
                builder.append(")");
                builder.append(" AND ");
            }

            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'").append(" AND ").append(partitionTimeColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnTimeFormat) + "'");
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'");
            builder.append(")");
            builder.append(")");
        }
    }

    /**
     * Another implementation of IPartitionConditionBuilder, for the fact tables which have three partition columns "YEAR", "MONTH", and "DAY"; This
     * class will concat the three columns into yyyy-MM-dd format for query hive;
     */
    public static class YearMonthDayPartitionConditionBuilder implements PartitionDesc.IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias) {

            String partitionColumnName = partDesc.getPartitionDateColumn();//日期字段列
            String partitionTableName;//日期分区列所在的表名

            // convert to use table alias
            int indexOfDot = partitionColumnName.lastIndexOf(".");
            if (indexOfDot > 0) {
                partitionTableName = partitionColumnName.substring(0, indexOfDot).toUpperCase(); //解析日期分区列所在的表名
            } else {
                throw new IllegalStateException("The partitionColumnName is invalid: " + partitionColumnName);
            }

            //别名替换
            if (tableAlias.containsKey(partitionTableName)) {
                partitionTableName = tableAlias.get(partitionTableName);
            }

            String concatField = String.format("CONCAT(%s.YEAR,'-',%s.MONTH,'-',%s.DAY)", partitionTableName, partitionTableName, partitionTableName);
            StringBuilder builder = new StringBuilder();

            //转换sql
            if (startInclusive > 0) {
                builder.append(concatField + " >= '" + DateFormat.formatToDateStr(startInclusive) + "' ");
                builder.append("AND ");
            }
            builder.append(concatField + " < '" + DateFormat.formatToDateStr(endExclusive) + "'");

            return builder.toString();
        }
    }

    public static PartitionDesc getCopyOf(PartitionDesc partitionDesc) {
        PartitionDesc newPartDesc = new PartitionDesc();
        newPartDesc.setCubePartitionType(partitionDesc.getCubePartitionType());
        newPartDesc.setPartitionDateColumn(partitionDesc.getPartitionDateColumn());
        newPartDesc.setPartitionDateFormat(partitionDesc.getPartitionDateFormat());
        newPartDesc.setPartitionDateStart(partitionDesc.getPartitionDateStart());
        return newPartDesc;
    }

}
