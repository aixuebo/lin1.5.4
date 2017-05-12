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

package org.apache.kylin.cube;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 校验cube的有效性
 */
public class CubeValidator {
    private static final Logger logger = LoggerFactory.getLogger(CubeValidator.class);

    /**
     * Validates:校验内容
     * - consistent isOffsetsOn() 只能使用offset或者分区的一种,不允许混合使用
     * - for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes,所有ready的segment是不允许有交集的,但是可以有空隙,但是会发出警告日志
     * - for all new segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it 所有的new状态的segment不允许有交集,同时满足new和ready的segment有交集,但是不是包含关系,因此是不允许的,即有交集可以,但是必须new的范围要包含ready的setment范围
     * - for all new segments, sourceOffset SHOULD fit/connect another segments
     * - dateRange does not matter any more
     * 校验segment集合
     */
    public static void validate(Collection<CubeSegment> segments) {
        if (segments == null || segments.isEmpty())
            return;

        // make a copy, don't modify given list 做一个copy,不去修改原始值
        List<CubeSegment> all = Lists.newArrayList(segments);
        Collections.sort(all);//排序

        // check consistent isOffsetsOn()
        boolean isOffsetsOn = all.get(0).isSourceOffsetsOn();//true表示如果使用的是offset
        for (CubeSegment seg : all) {
            seg.validate();//说明segment本身合法
            if (seg.isSourceOffsetsOn() != isOffsetsOn)//说明比较的混用了offset和date,因此是有问题的---生产中应该也不会混合使用
                throw new IllegalStateException("Inconsistent isOffsetsOn for segment " + seg);
        }

        List<CubeSegment> ready = Lists.newArrayListWithCapacity(all.size());
        List<CubeSegment> news = Lists.newArrayListWithCapacity(all.size());
        for (CubeSegment seg : all) {
            if (seg.getStatus() == SegmentStatusEnum.READY)
                ready.add(seg);
            else
                news.add(seg);
        }

        // for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
        CubeSegment pre = null;
        for (CubeSegment seg : ready) {
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg))//说明两个segment有交集,因此抛异常
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
                if (pre.getSourceOffsetEnd() < seg.getSourceOffsetStart())//第二个开始位置比前一个结束位置还大,而不是等于,因此说明有空隙
                    logger.warn("Hole between adjacent READY segments " + pre + " and " + seg);//说明相邻的两个segment之间有空隙,即有没有覆盖到的分区,发出警告日志
            }
            pre = seg;
        }

        // for all other segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
        pre = null;
        for (CubeSegment seg : news) {//循环每一个new状态的segment
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg)) //有交集不允许
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
            }
            pre = seg;

            for (CubeSegment aReady : ready) {
                if (seg.sourceOffsetOverlaps(aReady) && !seg.sourceOffsetContains(aReady)) //new和ready的segment有交集,但是不是包含关系,因此是不允许的,即有交集可以,但是必须new的范围要包含ready的setment范围
                    throw new IllegalStateException("Segments overlap: " + aReady + " and " + seg);
            }
        }

        //该校验规则不太懂,应该是有空隙的意思吧。反正就是警告而已
        // for all other segments, sourceOffset SHOULD fit/connect other segments
        for (CubeSegment seg : news) {
            Pair<Boolean, Boolean> pair = fitInSegments(all, seg);//新增的segment是否存在
            boolean startFit = pair.getFirst();
            boolean endFit = pair.getSecond();

            if (!startFit)
                logger.warn("NEW segment start does not fit/connect with other segments: " + seg);//说明新增的开始位置不存在
            if (!endFit)
                logger.warn("NEW segment end does not fit/connect with other segments: " + seg);//说明新增的结束位置不存在
        }
    }

    //确保该segment的范围是存在合法的,因此才可以进行重新build
    public static Pair<Boolean, Boolean> fitInSegments(List<CubeSegment> segments, CubeSegment newOne) {
        if (segments == null || segments.isEmpty())
            return null;

        //第一个和最后一个CubeSegment
        CubeSegment first = segments.get(0);
        CubeSegment last = segments.get(segments.size() - 1);

        //新的segment
        long start = newOne.getSourceOffsetStart();
        long end = newOne.getSourceOffsetEnd();

        boolean startFit = false;
        boolean endFit = false;
        for (CubeSegment sss : segments) {//循环每一个segment
            if (sss == newOne) //因为有时候segment的uuid不同.因此该数据也会不同
                continue;

            //以下两个判断的是,该新的segment的开始和结束位置,是否已经存在了
            //注意 此时的存在不是开始和结束都存在同一个segment中,而是存在不同segment的开始和结束位置也可以,只要新的segment的开始和结束 在历史中存在,即会设置为true
            startFit = startFit || (start == sss.getSourceOffsetStart() || start == sss.getSourceOffsetEnd());
            endFit = endFit || (end == sss.getSourceOffsetStart() || end == sss.getSourceOffsetEnd());
        }

        //此时说明新的segment的开始或者结束,在以前历史segment中是不存在的,但是对应的结束或者开始是存在的,
        if (!startFit && endFit && newOne == first) //开始不存在,但是结束存在,因此新增的又是第一个,因此第一个segment是不需要开始的,因此说明开始也是存在的
            startFit = true;
        if (!endFit && startFit && newOne == last) //同理最后一个是不需要结束存在的,因此说明结束也存在
            endFit = true;

        return Pair.newPair(startFit, endFit);
    }

}
