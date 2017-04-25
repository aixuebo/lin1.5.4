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

package org.apache.kylin.measure.topn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Modified from the StreamSummary.java in https://github.com/addthis/stream-lib
 * 
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 *
 * @param <T> type of data in the stream to be summarized
 */
public class TopNCounter<T> implements Iterable<Counter<T>> {

    public static final int EXTRA_SPACE_RATE = 50;

    protected int capacity;//最大容量
    private HashMap<T, ListNode2<Counter<T>>> counterMap;//每一个value值和对象组成的链表节点
    protected DoublyLinkedList<Counter<T>> counterList;//每一个元素值和count组成的对象集合,按照顺序已经排序好了,因此获取topN的时候,从header开始获取n个元素即可

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounter(int capacity) {
        this.capacity = capacity;
        counterMap = new HashMap<T, ListNode2<Counter<T>>>();
        counterList = new DoublyLinkedList<Counter<T>>();
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     * 添加一个元素到流中
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise ,false说明该元素已经在流中存在了
     */
    public boolean offer(T item) {
        return offer(item, 1.0);
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise,false说明该元素已经在流中存在了
     * 返回是否新增成功
     */
    public boolean offer(T item, double incrementCount) {
        return offerReturnAll(item, incrementCount).getFirst();
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return item dropped from summary if an item was dropped, null otherwise
     * 返回删除的元素
     */
    public T offerReturnDropped(T item, double incrementCount) {
        return offerReturnAll(item, incrementCount).getSecond();
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return Pair<isNewItem, itemDropped> where isNewItem is the return value of offer() and itemDropped is null if no item was dropped
     * 返回值 boolean表示是否是新增加的元素,第二个参数表示丢弃的元素
     */
    public Pair<Boolean, T> offerReturnAll(T item, double incrementCount) {
        ListNode2<Counter<T>> counterNode = counterMap.get(item);
        boolean isNewItem = (counterNode == null);//true说明该元素是新元素
        T droppedItem = null;
        if (isNewItem) {
            if (size() < capacity) {
                counterNode = counterList.enqueue(new Counter<T>(item));//向后追加一个元素
            } else {//要删除最小的元素
                counterNode = counterList.tail();//拿到最小的元素
                Counter<T> counter = counterNode.getValue();//最小的元素值
                droppedItem = counter.item;//最小的value

                counterMap.remove(droppedItem);//移除

                //将该值添加到最后一个count位置
                counter.item = item;
                counter.count = 0.0;
            }
            counterMap.put(item, counterNode);
        }

        incrementCounter(counterNode, incrementCount);

        return Pair.newPair(isNewItem, droppedItem);
    }

    /**
     * 为该元素增加incrementCount次数
     * 并且进行排序
     */
    protected void incrementCounter(ListNode2<Counter<T>> counterNode, double incrementCount) {

        //节点增加count
        Counter<T> counter = counterNode.getValue();
        counter.count += incrementCount;

        ListNode2<Counter<T>> nodeNext;

        if (incrementCount > 0) {
            nodeNext = counterNode.getNext();
        } else {
            nodeNext = counterNode.getPrev();
        }
        counterList.remove(counterNode);
        counterNode.prev = null;
        counterNode.next = null;

        if (incrementCount > 0) {
            while (nodeNext != null && counter.count >= nodeNext.getValue().count) {
                nodeNext = nodeNext.getNext();
            }
            if (nodeNext != null) {
                counterList.addBefore(nodeNext, counterNode);
            } else {
                counterList.add(counterNode);
            }

        } else {
            while (nodeNext != null && counter.count < nodeNext.getValue().count) {
                nodeNext = nodeNext.getPrev();
            }
            if (nodeNext != null) {
                counterList.addAfter(nodeNext, counterNode);
            } else {
                counterList.enqueue(counterNode);
            }
        }
    }

    //获取topk个元素对象
    public List<T> peek(int k) {
        List<T> topK = new ArrayList<T>(k);

        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
            if (topK.size() == k) {
                return topK;
            }
            topK.add(b.item);
        }

        return topK;
    }

    //以此获取k个元素
    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<Counter<T>>(k);

        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
            if (topK.size() == k) {
                return topK;
            }
            topK.add(b);
        }

        return topK;
    }

    /**
     * @return number of items stored
     */
    public int size() {
        return counterMap.size();
    }

    //按照顺序打印每一个元素以及对应的count
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
            sb.append(b.item);
            sb.append(':');
            sb.append(b.count);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Put element to the head position;
     * The consumer should call this method with count in ascending way; the item will be directly put to the head of the list, without comparison for best performance;
     * @param item
     * @param count
     * 初始化一个节点,添加到head上
     */
    public void offerToHead(T item, double count) {
        Counter<T> c = new Counter<T>(item);
        c.count = count;
        ListNode2<Counter<T>> node = counterList.add(c);//直接追加到头上
        counterMap.put(c.item, node);
    }

    /**
     * Merge another counter into this counter;
     * @param another
     * @return
     * 合并
     */
    public TopNCounter<T> merge(TopNCounter<T> another) {
        double m1 = 0.0, m2 = 0.0;//队列1的最小值和队列2的最小值
        if (this.size() >= this.capacity) {
            m1 = this.counterList.tail().getValue().count;//获取队列1的最小值
        }

        if (another.size() >= another.capacity) {
            m2 = another.counterList.tail().getValue().count;//获取队列2的最小值
        }

        Set<T> duplicateItems = Sets.newHashSet();//说明两个队列都存在的元素
        List<T> notDuplicateItems = Lists.newArrayList();//该元素不在参数队列存在

        for (Map.Entry<T, ListNode2<Counter<T>>> entry : this.counterMap.entrySet()) {//循环数据
            T item = entry.getKey();
            ListNode2<Counter<T>> existing = another.counterMap.get(item);//队列2是否存在
            if (existing != null) {//说明存在
                duplicateItems.add(item);
            } else {//说明不存在
                notDuplicateItems.add(item);
            }
        }

        for (T item : duplicateItems) {//重复的元素,增加对应的count即可
            this.offer(item, another.counterMap.get(item).getValue().count);
        }


        for (T item : notDuplicateItems) {
            this.offer(item, m2);
        }

        for (Map.Entry<T, ListNode2<Counter<T>>> entry : another.counterMap.entrySet()) {
            T item = entry.getKey();
            if (duplicateItems.contains(item) == false) {//说明不包含
                double counter = entry.getValue().getValue().count;
                this.offer(item, counter + m1);
            }
        }

        return this;
    }

    /**
     * Retain the capacity to the given number; The extra counters will be cut off
     * @param newCapacity
     * 创建newCapacity大小的队列
     */
    public void retain(int newCapacity) {
        assert newCapacity > 0;
        this.capacity = newCapacity;//新的队列大小
        if (newCapacity < this.size()) {//说明要取消一部分元素
            ListNode2<Counter<T>> tail = counterList.tail();//从最小的开始取消
            while (tail != null && this.size() > newCapacity) {//不断循环,抽取元素进行取消
                Counter<T> bucket = tail.getValue();
                //删除元素
                this.counterMap.remove(bucket.getItem());
                this.counterList.remove(tail);
                tail = this.counterList.tail();
            }
        }
    }

    /**
     * Get the counter values in ascending order
     * 从小到大的顺序,把所有的count收集成数组
     */
    public double[] getCounters() {
        double[] counters = new double[size()];
        int index = 0;

        for (ListNode2<Counter<T>> bNode = counterList.tail(); bNode != null; bNode = bNode.getNext()) {
            Counter<T> b = bNode.getValue();
            counters[index] = b.count;
            index++;
        }

        assert index == size();
        return counters;
    }

    //循环所有的元素---该元素循环的是有大小顺序的
    @Override
    public Iterator<Counter<T>> iterator() {
        return new TopNCounterIterator();
    }

    /**
     * Iterator from the tail (smallest) to head (biggest);
     * 从小到大的顺序迭代每一个元素
     */
    private class TopNCounterIterator implements Iterator<Counter<T>> {

        private ListNode2<Counter<T>> currentBNode;//每一个节点的迭代器

        private TopNCounterIterator() {
            currentBNode = counterList.tail();//从小到大的顺序
        }

        @Override
        public boolean hasNext() {
            return currentBNode != null;

        }

        @Override
        public Counter<T> next() {
            Counter<T> counter = currentBNode.getValue();
            currentBNode = currentBNode.getNext();//获取下一个元素
            return counter;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
