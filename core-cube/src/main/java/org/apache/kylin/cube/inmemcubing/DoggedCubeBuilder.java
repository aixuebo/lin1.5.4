/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.kylin.cube.inmemcubing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * When base cuboid does not fit in memory, cut the input into multiple splits and merge the split outputs at last.
 */
public class DoggedCubeBuilder extends AbstractInMemCubeBuilder {

    private static Logger logger = LoggerFactory.getLogger(DoggedCubeBuilder.class);

    private int splitRowThreshold = Integer.MAX_VALUE;//一个数据块存放的数据条数的上限伐值
    private int unitRows = 1000;//每次读取行数

    /**
     * @param cubeDesc cube对象
     * @param flatDesc cube要处理的宽表
     * @param dictionaryMap cube需要的字典对象映射
     */
    public DoggedCubeBuilder(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        super(cubeDesc, flatDesc, dictionaryMap);

        // check memory more often if a single row is big
        if (cubeDesc.hasMemoryHungryMeasures()) //true表示cube有耗费内存的度量
            unitRows /= 10;//每次少读取一部分数据
    }

    public void setSplitRowThreshold(int rowThreshold) {
        this.splitRowThreshold = rowThreshold;
        this.unitRows = Math.min(unitRows, rowThreshold);
    }

    @Override
    public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
        new BuildOnce().build(input, output);
    }

    private class BuildOnce {

        BuildOnce() {
        }

        /**
         * 从队列中不断的获取元素
         * @param input 队列的元素List<String> 表示一行的数据具体内容,因为一行是由多个列组成的,因此元素是List<String>表示列
         * @param output 最终如何输出builder后的结果
         */
        public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
            final List<SplitThread> splits = new ArrayList<SplitThread>();//已经拆分出来的数据块集合
            final Merger merger = new Merger();

            long start = System.currentTimeMillis();
            logger.info("Dogged Cube Build start");

            try {
                SplitThread last = null;
                boolean eof = false;

                while (!eof) {

                    if (last != null && shouldCutSplit(splits)) {//true说明应该拆分数据块了
                        cutSplit(last);//重新切分一个数据
                        last = null;
                    }

                    checkException(splits);

                    if (last == null) {//创建新的数据块
                        last = new SplitThread();
                        splits.add(last);
                        last.start();//开启该线程任务
                        logger.info("Split #" + splits.size() + " kickoff");
                    }

                    eof = feedSomeInput(input, last, unitRows);//false说明成功读取了数据
                }

                for (SplitThread split : splits) {
                    split.join();
                }
                checkException(splits);
                logger.info("Dogged Cube Build splits complete, took " + (System.currentTimeMillis() - start) + " ms");

                merger.mergeAndOutput(splits, output);

            } catch (Throwable e) {
                logger.error("Dogged Cube Build error", e);
                if (e instanceof Error)
                    throw (Error) e;
                else if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                else
                    throw new IOException(e);
            } finally {
                output.close();
                closeGirdTables(splits);
                logger.info("Dogged Cube Build end, totally took " + (System.currentTimeMillis() - start) + " ms");
                ensureExit(splits);
                logger.info("Dogged Cube Build return");
            }
        }

        private void closeGirdTables(List<SplitThread> splits) {
            for (SplitThread split : splits) {
                if (split.buildResult != null) {
                    for (CuboidResult r : split.buildResult.values()) {
                        try {
                            r.table.close();
                        } catch (Throwable e) {
                            logger.error("Error closing grid table " + r.table, e);
                        }
                    }
                }
            }
        }

        //终止线程
        private void ensureExit(List<SplitThread> splits) throws IOException {
            try {
                for (int i = 0; i < splits.size(); i++) {
                    SplitThread split = splits.get(i);
                    if (split.isAlive()) {
                        abort(splits);
                    }
                }
            } catch (Throwable e) {
                logger.error("Dogged Cube Build error", e);
            }
        }

        private void checkException(List<SplitThread> splits) throws IOException {
            for (int i = 0; i < splits.size(); i++) {
                SplitThread split = splits.get(i);
                if (split.exception != null)
                    abort(splits);
            }
        }

        private void abort(List<SplitThread> splits) throws IOException {
            for (SplitThread split : splits) {
                split.builder.abort();
            }

            ArrayList<Throwable> errors = new ArrayList<Throwable>();
            for (SplitThread split : splits) {
                try {
                    split.join();
                } catch (InterruptedException e) {
                    errors.add(e);
                }
                if (split.exception != null)
                    errors.add(split.exception);
            }

            if (errors.isEmpty()) {
                return;
            } else if (errors.size() == 1) {
                Throwable t = errors.get(0);
                if (t instanceof IOException)
                    throw (IOException) t;
                else
                    throw new IOException(t);
            } else {
                for (Throwable t : errors)
                    logger.error("Exception during in-mem cube build", t);
                throw new IOException(errors.size() + " exceptions during in-mem cube build, cause set to the first, check log for more", errors.get(0));
            }
        }

        //不断的从队列中读取一行数据,使用split对象处理该行数据
        //n表示每次读取行数
        //返回值 true表示 处理数据时候有异常,或者读取到的一行数据是null

        /**
         * @param input 总的数据集合
         * @param split 要向哪个线程分配数据
         * @param n 本次读取多少行数据-----最多分配这么多条数据
         */
        private boolean feedSomeInput(BlockingQueue<List<String>> input, SplitThread split, int n) {
            try {
                int i = 0;//已经分配了多少条数据
                while (i < n) {//只要数据还没有分足,就不断分配
                    List<String> record = input.take();//获取一行数据
                    i++;

                    while (split.inputQueue.offer(record, 1, TimeUnit.SECONDS) == false) {//false说明队列已经满了
                        if (split.exception != null) //有异常则返回true
                            return true; // got some error 该线程的队列已经满了,则可以退出了
                    }

                    //到这里说明已经添加成功
                    split.inputRowCount++;//处理一行数据

                    if (record == null || record.isEmpty()) {//说明已经数据源已经没有数据了,因此不需要继续添加数据了
                        return true;
                    }
                }
                return false;

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //结束该线程任务
        private void cutSplit(SplitThread last) {
            try {
                // signal the end of input
                while (last.isAlive()) {
                    if (last.inputQueue.offer(Collections.<String> emptyList())) {
                        break;
                    }
                    Thread.sleep(1000);
                }

                // wait cuboid build done
                last.join();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //true表示需要切一个新的块
        private boolean shouldCutSplit(List<SplitThread> splits) {
            int systemAvailMB = MemoryBudgetController.getSystemAvailMB();//系统可用内存--单位是M
            int nSplit = splits.size();
            long splitRowCount = nSplit == 0 ? 0 : splits.get(nSplit - 1).inputRowCount;//获取最后一个数据块的记录数

            logger.info(splitRowCount + " records went into split #" + nSplit + "; " + systemAvailMB + " MB left, " + reserveMemoryMB + " MB threshold");

            if (splitRowCount >= splitRowThreshold) {//超出伐值了
                logger.info("Split cut due to hitting splitRowThreshold " + splitRowThreshold);
                return true;
            }

            if (systemAvailMB <= reserveMemoryMB * 1.5) {//保留内存的1.5倍  已经比系统剩余内存大了,因此已经没内存了,要重新切一个块
                logger.info("Split cut due to hitting memory threshold, system avail " + systemAvailMB + " MB <= reserve " + reserveMemoryMB + "*1.5 MB");
                return true;
            }

            return false;
        }
    }

    //表示一个数据块的拆分
    private class SplitThread extends Thread {
        final BlockingQueue<List<String>> inputQueue = new ArrayBlockingQueue<List<String>>(16);//内部处理一行数据的队列
        final InMemCubeBuilder builder;

        ConcurrentNavigableMap<Long, CuboidResult> buildResult;
        long inputRowCount = 0;//该数据块已经处理的记录数
        RuntimeException exception;//添加处理数据时候的异常

        public SplitThread() {
            this.builder = new InMemCubeBuilder(cubeDesc, flatDesc, dictionaryMap);
            this.builder.setConcurrentThreads(taskThreadCount);
            this.builder.setReserveMemoryMB(reserveMemoryMB);
        }

        @Override
        public void run() {
            try {
                buildResult = builder.build(inputQueue);
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    this.exception = (RuntimeException) e;
                else
                    this.exception = new RuntimeException(e);
            }
        }
    }

    private class Merger {

        MeasureAggregators reuseAggrs;
        Object[] reuseMetricsArray;
        ByteArray reuseMetricsSpace;

        long lastCuboidColumnCount;
        ImmutableBitSet lastMetricsColumns;

        Merger() {
            reuseAggrs = new MeasureAggregators(cubeDesc.getMeasures());
            reuseMetricsArray = new Object[cubeDesc.getMeasures().size()];
        }

        public void mergeAndOutput(List<SplitThread> splits, ICuboidWriter output) throws IOException {
            if (splits.size() == 1) {
                for (CuboidResult cuboidResult : splits.get(0).buildResult.values()) {
                    outputCuboid(cuboidResult.cuboidId, cuboidResult.table, output);
                    cuboidResult.table.close();
                }
                return;
            }

            LinkedList<MergeSlot> open = Lists.newLinkedList();
            for (SplitThread split : splits) {
                open.add(new MergeSlot(split));
            }

            PriorityQueue<MergeSlot> heap = new PriorityQueue<MergeSlot>();

            while (true) {
                // ready records in open slots and add to heap
                while (!open.isEmpty()) {
                    MergeSlot slot = open.removeFirst();
                    if (slot.fetchNext()) {
                        heap.add(slot);
                    }
                }

                // find the smallest on heap
                MergeSlot smallest = heap.poll();
                if (smallest == null)
                    break;
                open.add(smallest);

                // merge with slots having the same key
                if (smallest.isSameKey(heap.peek())) {
                    Object[] metrics = getMetricsValues(smallest.currentRecord);
                    reuseAggrs.reset();
                    reuseAggrs.aggregate(metrics);
                    do {
                        MergeSlot slot = heap.poll();
                        open.add(slot);
                        metrics = getMetricsValues(slot.currentRecord);
                        reuseAggrs.aggregate(metrics);
                    } while (smallest.isSameKey(heap.peek()));

                    reuseAggrs.collectStates(metrics);
                    setMetricsValues(smallest.currentRecord, metrics);
                }

                output.write(smallest.currentCuboidId, smallest.currentRecord);
            }
        }

        private void setMetricsValues(GTRecord record, Object[] metricsValues) {
            ImmutableBitSet metrics = getMetricsColumns(record);

            if (reuseMetricsSpace == null) {
                reuseMetricsSpace = new ByteArray(record.getInfo().getMaxColumnLength(metrics));
            }

            record.setValues(metrics, reuseMetricsSpace, metricsValues);
        }

        private Object[] getMetricsValues(GTRecord record) {
            ImmutableBitSet metrics = getMetricsColumns(record);
            return record.getValues(metrics, reuseMetricsArray);
        }

        private ImmutableBitSet getMetricsColumns(GTRecord record) {
            // metrics columns always come after dimension columns
            if (lastCuboidColumnCount == record.getInfo().getColumnCount())
                return lastMetricsColumns;

            int to = record.getInfo().getColumnCount();
            int from = to - reuseMetricsArray.length;
            lastCuboidColumnCount = record.getInfo().getColumnCount();
            lastMetricsColumns = new ImmutableBitSet(from, to);
            return lastMetricsColumns;
        }
    }

    private static class MergeSlot implements Comparable<MergeSlot> {

        final Iterator<CuboidResult> cuboidIterator;
        IGTScanner scanner;
        Iterator<GTRecord> recordIterator;

        long currentCuboidId;
        GTRecord currentRecord;

        public MergeSlot(SplitThread split) {
            cuboidIterator = split.buildResult.values().iterator();
        }

        public boolean fetchNext() throws IOException {
            if (recordIterator == null) {
                if (cuboidIterator.hasNext()) {
                    CuboidResult cuboid = cuboidIterator.next();
                    currentCuboidId = cuboid.cuboidId;
                    scanner = cuboid.table.scan(new GTScanRequestBuilder().setInfo(cuboid.table.getInfo()).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
                    recordIterator = scanner.iterator();
                } else {
                    return false;
                }
            }

            if (recordIterator.hasNext()) {
                currentRecord = recordIterator.next();
                return true;
            } else {
                scanner.close();
                recordIterator = null;
                return fetchNext();
            }
        }

        @Override
        public int compareTo(MergeSlot o) {
            long cuboidComp = this.currentCuboidId - o.currentCuboidId;
            if (cuboidComp != 0)
                return cuboidComp < 0 ? -1 : 1;

            // note GTRecord.equals() don't work because the two GTRecord comes from different GridTable
            ImmutableBitSet pk = this.currentRecord.getInfo().getPrimaryKey();
            for (int i = 0; i < pk.trueBitCount(); i++) {
                int c = pk.trueBitAt(i);
                int comp = this.currentRecord.get(c).compareTo(o.currentRecord.get(c));
                if (comp != 0)
                    return comp;
            }
            return 0;
        }

        public boolean isSameKey(MergeSlot o) {
            if (o == null)
                return false;
            else
                return this.compareTo(o) == 0;
        }

    };
}
