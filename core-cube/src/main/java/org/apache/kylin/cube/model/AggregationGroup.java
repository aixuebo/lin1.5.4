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

package org.apache.kylin.cube.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class AggregationGroup {
    //用于继承设置中
    public static class HierarchyMask {
        public long fullMask;// 00000111 如果有3个属性安排了继承关系,因此该字段可以得到是哪三个属性安排了继承关系
        public long[] allMasks;// 00000100,00000110,00000111 用于表示继承关系中的顺序,是先谁后谁
        public long[] dims;// 00000100,00000010,00000001  比如有三个继承关系,因此该size为3,每一个元素表示每一个字段在rowkey中的对应的值,即只有一个位置是1
    }

    @JsonProperty("includes")
    private String[] includes;//该维度组包含哪些列
    /**
     json内容
     "select_rule": {
     "hierarchy_dims": [],
     "mandatory_dims": [],
     "joint_dims": []
     }
     */
    @JsonProperty("select_rule")
    private SelectRule selectRule;//维度组内的列是什么方式可以优化

    //computed 计算编码----在hbase的rowkey的位置
    //比如rowkey有8个字段,因此是111111110
    //如果iclude值有3个字段,并且相对位置也知道,因此该值为100000110
    private long partialCubeFullMask;//includes中字段,即全部维度组合
    private long mandatoryColumnMask;//mandatory中字段,即必须存在的维度组合
    private List<HierarchyMask> hierarchyMasks;//每一个继承关系都维护了一个HierarchyMask对象
    private List<Long> joints;//each long is a group,joint中字段,要么同时存在,要么同时不存在的维度组合集合


    private long jointDimsMask;//所有参与joint的字段集合,该参数的字段位置是1
    private long hierarchyDimsMask;//所有参数继承的字段集合,该参数的字段位置是1
    private long normalDimsMask;//includes中字段 除了参与mandatory、joint、hierarch字段外的字段的位置设置为1
    private List<Long> normalDims;//each long is a single dim  将每一个normal的字段的位置 转换成整数,即该集合内的元素,每一个元素都只有一个位置是1,所有的元素都表示为normal的字段

    private CubeDesc cubeDesc;//该聚合组所属的cube对象
    private boolean isMandatoryOnlyValid;//false表示如果cuboid与mandatoryColumnMask相同,则校验不合格,即默认是不允许includes中字段都是mandatory_dims维度

    public void init(CubeDesc cubeDesc, RowKeyDesc rowKeyDesc) {
        this.cubeDesc = cubeDesc;
        this.isMandatoryOnlyValid = cubeDesc.getConfig().getCubeAggrGroupIsMandatoryOnlyValid();
        Map<String, TblColRef> colNameAbbr = cubeDesc.buildColumnNameAbbreviation();//刨除derived的列,以及额外的列,返回剩余列的集合,即参与builder的维度集合

        if (this.includes == null || this.includes.length == 0 || this.selectRule == null) {
            throw new IllegalStateException("AggregationGroup incomplete");
        }

        //分别对include、Mandatory、Hierarchy、joint所在的维度进行编码
        buildPartialCubeFullMask(colNameAbbr, rowKeyDesc);
        buildMandatoryColumnMask(colNameAbbr, rowKeyDesc);
        buildHierarchyMasks(colNameAbbr, rowKeyDesc);
        buildJointColumnMask(colNameAbbr, rowKeyDesc);

        //设置所有参与joint的字段   所有参与继承相关的字段  所有什么都没参与优化的字段
        buildJointDimsMask();
        buildHierarchyDimsMask();
        buildNormalDimsMask();

    }

    //对include所在的维度进行编码
    private void buildPartialCubeFullMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        Preconditions.checkState(this.includes != null);
        Preconditions.checkState(this.includes.length != 0);

        partialCubeFullMask = 0L;
        for (String dim : this.includes) {
            TblColRef hColumn = colNameAbbr.get(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);//该列的对应的序号
            long bit = 1L << index;//2的index次方
            partialCubeFullMask |= bit;
        }
    }


    private void buildMandatoryColumnMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        mandatoryColumnMask = 0L;

        String[] mandatory_dims = this.selectRule.mandatory_dims;//必须存在的维度集合
        if (mandatory_dims == null || mandatory_dims.length == 0) {
            return;
        }

        for (String dim : mandatory_dims) {
            TblColRef hColumn = colNameAbbr.get(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
            mandatoryColumnMask |= 1 << index;
        }

    }

    private void buildHierarchyMasks(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        this.hierarchyMasks = new ArrayList<HierarchyMask>();

        if (this.selectRule.hierarchy_dims == null || this.selectRule.hierarchy_dims.length == 0) {
            return;
        }

        for (String[] hierarchy_dims : this.selectRule.hierarchy_dims) {
            HierarchyMask mask = new HierarchyMask();//每一组继承关系创建一个该对象
            if (hierarchy_dims == null || hierarchy_dims.length == 0) {
                continue;
            }

            ArrayList<Long> allMaskList = new ArrayList<Long>();//用于表示继承关系中的顺序,是先谁后谁
            ArrayList<Long> dimList = new ArrayList<Long>();//比如有三个继承关系,因此该size为3,每一个元素表示每一个字段在rowkey中的对应的值,即只有一个位置是1
            for (int i = 0; i < hierarchy_dims.length; i++) {//每一个继承中的元素
                TblColRef hColumn = colNameAbbr.get(hierarchy_dims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;

                //                if ((tailMask & bit) > 0)
                //                    continue; // ignore levels in tail, they don't participate
                //                // aggregation group combination anyway

                mask.fullMask |= bit;
                allMaskList.add(mask.fullMask);
                dimList.add(bit);
            }

            //将List转换成数组
            Preconditions.checkState(allMaskList.size() == dimList.size());
            mask.allMasks = new long[allMaskList.size()];
            mask.dims = new long[dimList.size()];
            for (int i = 0; i < allMaskList.size(); i++) {
                mask.allMasks[i] = allMaskList.get(i);
                mask.dims[i] = dimList.get(i);
            }

            this.hierarchyMasks.add(mask);

        }

    }

    private void buildJointColumnMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        joints = Lists.newArrayList();

        if (this.selectRule.joint_dims == null || this.selectRule.joint_dims.length == 0) {
            return;
        }

        for (String[] joint_dims : this.selectRule.joint_dims) {
            if (joint_dims == null || joint_dims.length == 0) {
                continue;
            }

            long joint = 0L;
            for (int i = 0; i < joint_dims.length; i++) {
                TblColRef hColumn = colNameAbbr.get(joint_dims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;
                joint |= bit;
            }

            Preconditions.checkState(joint != 0);
            joints.add(joint);
        }
    }

    //-------------------------
    public void buildJointDimsMask() {
        long ret = 0;
        for (long x : joints) {
            ret |= x;
        }
        this.jointDimsMask = ret;
    }

    private void buildHierarchyDimsMask() {
        long ret = 0;
        for (HierarchyMask mask : hierarchyMasks) {
            ret |= mask.fullMask;
        }
        this.hierarchyDimsMask = ret;
    }

    private void buildNormalDimsMask() {
        //no joint, no hierarchy, no mandatory
        /**
         * 1.~mandatoryColumnMask;表示反转,即1转换成0,0转换成1
         * 2.与该组内所有的字段全都是1的进行&,即返回依然是1的,此时返回的值就是:includes中字段 刨除 mandatory的字段
         * 3.再继续刨除jointDimsMask字段
         * 4.再继续刨除继承的字段
         */
        long leftover = partialCubeFullMask & ~mandatoryColumnMask;
        leftover &= ~this.jointDimsMask;
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            leftover &= ~hierarchyMask.fullMask;
        }

        this.normalDimsMask = leftover;
        this.normalDims = bits(leftover);
    }
    //-------------------------

    //将每一个normal的字段的位置 转换成整数,即该集合内的元素,每一个元素都只有一个位置是1,所有的元素都表示为normal的字段
    private List<Long> bits(long x) {
        List<Long> r = Lists.newArrayList();
        long l = x;
        while (l != 0) {
            long bit = Long.lowestOneBit(l);//获取长整数二进制最低位1的索引
            r.add(bit);
            l ^= bit;
        }
        return r;
    }

    public long getMandatoryColumnMask() {
        return mandatoryColumnMask;
    }

    public List<HierarchyMask> getHierarchyMasks() {
        return hierarchyMasks;
    }

    //返回的结果就是有多少个字段

    /**
     比如json---includes有18个字段,其中参与hierarchy_dims的字段一共4个,参与mandatory_dims必须存在的字段1个,参与joint_dims的字段一共11个
计算规则:normal的字段为18-4-1-11= 2个
因为joint_dims每一个组相当一个字段,因此4个组,相当于4个字段
hierarchy_dims的字段需要各种组合中过滤,但是最终还是需要每一个字段的,因此是字段的所有个数,即4

因此人工口算原来需要2^18的组合,现在因为有一个必须字段,因此变成2^17次方,又因为有4组joint_dims,而4组joint_dims一个使用11个字段,因此剩余字段为17-11=6个字段,+4组joint_dims,因此是2^10次方

程序算法规则,normal + joint_dims.length + 剩余的继承相关的字段数量,返回值就是结果需要2的多少次方
     "aggregation_groups": [
     {
     "includes": [
     "CREATE_YEAR",
     "CREATE_MONTH",
     "CREATE_DAY",
     "PROVINCE",
     "CHANNEL",
     "RFM",
     "MEAN_PREMIUM",
     "UTILITY",
     "FREQUENCY",
     "ACT_INVITE",
     "ACT_TERM",
     "ACT_DEMAND",
     "INVITE_QUALITY",
     "INVITE_QUANTITY",
     "PLATFORM",
     "CITY",
     "AGE",
     "SEX"
     ],
     "select_rule": {
         "hierarchy_dims": [
             [
             "CREATE_MONTH",
             "CREATE_DAY"
             ],
             [
             "PROVINCE",
             "CITY"
             ]
         ],
         "mandatory_dims": [
            "CREATE_YEAR"
         ],
         "joint_dims": [
             [
             "SEX",
             "AGE",
             "PLATFORM"
             ],
             [
             "ACT_TERM",
             "ACT_DEMAND"
             ],
             [
             "INVITE_QUANTITY",
             "INVITE_QUALITY",
             "ACT_INVITE"
             ],
             [
             "MEAN_PREMIUM",
             "UTILITY",
             "FREQUENCY"
             ]
        ]
     }
     */
    public int getBuildLevel() {
        int ret = 1;//base cuboid => partial cube root 因为不论怎么优化,都要把rowkey所有的字段跑一次,因此默认是1
        if (this.getPartialCubeFullMask() == Cuboid.getBaseCuboidId(cubeDesc)) {//不考虑base级别的---即如果该分组内所有字段就是rowkey的字段集合,即返回1-1=0,也就是说本来该group就包含了basecuboid,因此就不用提前+1了
            ret -= 1;//if partial cube's root is base cuboid, then one round less agg
        }

        ret += getNormalDims().size();//表示有多少个Normal字段---每一个normal字段都是一个字段

        //每一个继承字段也是一个字段
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            ret += hierarchyMask.allMasks.length;//有多少个字段参与了继承
        }

        //每一组联合字段作为一个字段
        for (Long joint : joints) {
            if ((joint & this.getHierarchyDimsMask()) == 0) {//说明两个字段没有公共部分,这个应该是必须的
                ret += 1;//有多少个join部分
            }
        }

        return ret;
    }

    public void setIncludes(String[] includes) {
        this.includes = includes;
    }

    public void setSelectRule(SelectRule selectRule) {
        this.selectRule = selectRule;
    }

    public List<Long> getJoints() {
        return joints;
    }

    public long getJointDimsMask() {
        return jointDimsMask;
    }

    public long getNormalDimsMask() {
        return normalDimsMask;
    }

    public long getHierarchyDimsMask() {
        return hierarchyDimsMask;
    }

    public List<Long> getNormalDims() {
        return normalDims;
    }

    public long getPartialCubeFullMask() {
        return partialCubeFullMask;
    }

    public String[] getIncludes() {
        return includes;
    }

    public SelectRule getSelectRule() {
        return selectRule;
    }

    public boolean isMandatoryOnlyValid() {
        return isMandatoryOnlyValid;
    }
}
