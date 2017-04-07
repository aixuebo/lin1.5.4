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

package org.apache.kylin.dict;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedList;

import org.apache.kylin.common.util.BytesUtil;

/**
 * Builds a dictionary using Trie structure. All values are taken in byte[] form
 * and organized in a Trie with ordering. Then numeric IDs are assigned in
 * sequence.
 * 
 * @author yangli9
 */
public class TrieDictionaryBuilder<T> {
    
    private static final int _2GB = 2000000000;

    public static class Node {
        public byte[] part;
        public boolean isEndOfValue;//true表明该node是一个单词的结尾,即表示存在这样的一个单词  默认boolean类型返回的是false
        public ArrayList<Node> children;

        public int nValuesBeneath; // only present after stats() 在stats方法之后,存在该属性对应的值,该属性是说该node节点的子子孙孙,包含本node节点对象,一共多少个单词结尾的单词

        Node(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue);
        }

        Node(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            reset(value, isEndOfValue, children);
        }

        void reset(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue, new ArrayList<Node>());
        }

        void reset(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            this.part = value;
            this.isEndOfValue = isEndOfValue;
            this.children = children;
        }
    }

    public static interface Visitor {
        void visit(Node n, int level);
    }

    // ============================================================================

    private Node root;
    private BytesConverter<T> bytesConverter;

    public TrieDictionaryBuilder(BytesConverter<T> bytesConverter) {
        this.root = new Node(new byte[0], false);
        this.bytesConverter = bytesConverter;
    }

    //添加一个对象,将对象转换成字节数组再添加
    //插入后就组成了一个字典树结构
    public void addValue(T value) {
        addValue(bytesConverter.convertToBytes(value));
    }

    //添加一个字节数组
    public void addValue(byte[] value) {
        addValueR(root, value, 0);
    }

    //向一个节点添加一个字节数组
    private void addValueR(Node node, byte[] value, int start) {
        // match the value part of current node
        int i = 0, j = start;
        int n = node.part.length, nn = value.length;
        int comp = 0;
        for (; i < n && j < nn; i++, j++) {
            comp = BytesUtil.compareByteUnsigned(node.part[i], value[j]);
            if (comp != 0)
                break;
        }

        if (j == nn) {//说明value全部内容都匹配当前node,即j的位置等于全部value的内容,因此是匹配完成
            // if value fully matched within the current node
            if (i == n) {//而当前node节点也被循环完了,说明这两个value是相同的,i是当前位置,n是总位置
                // if equals to current node, just mark end of value
                node.isEndOfValue = true;
            } else {//说明此时value是node的一部分
                // otherwise, split the current node into two
                Node c = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);//将i后面的元素单独拿出来,组成一个Node
                node.reset(BytesUtil.subarray(node.part, 0, i), true);//0到i的部分,就是匹配到一个单词了,因此该node就是一个单词内容,因此设置成true
                node.children.add(c);//单独拿出来的作为子节点
            }
            return;
        }

        // if partially matched the current, split the current node, add the new value, make a 3-way
        //说明只是匹配了一部分,拆分当前节点,
        if (i < n) {
            Node c1 = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);//i后面的部分单独拿出来
            Node c2 = new Node(BytesUtil.subarray(value, j, nn), true);//n后面的内容单独拿出来,并且nn是整个单词,因此设置为true,表示单词结尾
            node.reset(BytesUtil.subarray(node.part, 0, i), false);//重置0到i,非单词结尾
            if (comp < 0) {//按照顺序设置子节点
                node.children.add(c1);
                node.children.add(c2);
            } else {
                node.children.add(c2);
                node.children.add(c1);
            }
            return;
        }

        // out matched the current, binary search the next byte for a child node to continue
        //说明value比当前node还要大
        byte lookfor = value[j];//查找value的下一个字节
        //二分法查找
        int lo = 0;
        int hi = node.children.size() - 1;
        int mid = 0;
        boolean found = false;
        comp = 0;
        while (!found && lo <= hi) {
            mid = lo + (hi - lo) / 2;
            comp = BytesUtil.compareByteUnsigned(lookfor, node.children.get(mid).part[0]);//比较第0个字节
            if (comp < 0)
                hi = mid - 1;
            else if (comp > 0)
                lo = mid + 1;
            else
                found = true;
        }


        if (found) {//说明找到了
            // found a child node matching the first byte, continue in that child
            addValueR(node.children.get(mid), value, j);//在子字符串内继续追加,value从j位置开始计算
        } else {//说明没找到,则添加新的子节点
            // otherwise, make the value a new child
            Node c = new Node(BytesUtil.subarray(value, j, nn), true);//并且是单词结尾,因此是true
            node.children.add(comp <= 0 ? mid : mid + 1, c);//在适当位置上插入该值
        }
    }

    //从前往后循环每一个node
    public void traverse(Visitor visitor) {
        traverseR(root, visitor, 0);
    }

    private void traverseR(Node node, Visitor visitor, int level) {
        visitor.visit(node, level);
        for (Node c : node.children)
            traverseR(c, visitor, level + 1);
    }

    //从后往前循环每一个node
    public void traversePostOrder(Visitor visitor) {
        traversePostOrderR(root, visitor, 0);
    }

    private void traversePostOrderR(Node node, Visitor visitor, int level) {
        for (Node c : node.children)
            traversePostOrderR(c, visitor, level + 1);
        visitor.visit(node, level);
    }

    public static class Stats {
        public int nValues; // number of values in total 总的单词数
        public int nValueBytesPlain; // number of bytes for all values
                                     // uncompressed 所有的单词占用总字节(没有压缩前)
        public int nValueBytesCompressed; // number of values bytes in Trie
                                          // (compressed) 压缩后所有单词占用总字节
        public int maxValueLength; // size of longest value in bytes 最长的单词占用多少个字节

        // the trie is multi-byte-per-node
        public int mbpn_nNodes; // number of nodes in trie 有多少个node节点
        public int mbpn_trieDepth; // depth of trie ,node节点的最大深度
        public int mbpn_maxFanOut; // the maximum no. children ,node中最多有多少个子节点
        public long mbpn_nChildLookups; // number of child lookups during lookup every value once
        public long mbpn_nTotalFanOut; // the sum of fan outs during lookup every value once


        public int mbpn_sizeValueTotal; // the sum of value space in all nodes
        public int mbpn_sizeNoValueBytes; // size of field noValueBytes
        public int mbpn_sizeNoValueBeneath; // size of field noValuesBeneath, depends on cardinality
        public int mbpn_sizeChildOffset; // size of field childOffset, points to first child in flattened array
        public long mbpn_footprint; // MBPN footprint in bytes

        // stats for one-byte-per-node as well, so there's comparison
        public int obpn_sizeValue; // size of value per node, always 1
        public int obpn_sizeNoValuesBeneath; // size of field noValuesBeneath, depends on cardinality
        public int obpn_sizeChildCount; // size of field childCount, enables binary search among children
        public int obpn_sizeChildOffset; // size of field childOffset, points to first child in flattened array
        public int obpn_nNodes; // no. nodes in OBPN trie
        public long obpn_footprint; // OBPN footprint in bytes

        public void print() {
            PrintStream out = System.out;
            out.println("============================================================================");
            out.println("No. values:             " + nValues);
            out.println("No. bytes raw:          " + nValueBytesPlain);
            out.println("No. bytes in trie:      " + nValueBytesCompressed);
            out.println("Longest value length:   " + maxValueLength);

            // flatten trie footprint calculation, case of One-Byte-Per-Node
            out.println("----------------------------------------------------------------------------");
            out.println("OBPN node size:  " + (obpn_sizeValue + obpn_sizeNoValuesBeneath + obpn_sizeChildCount + obpn_sizeChildOffset) + " = " + obpn_sizeValue + " + " + obpn_sizeNoValuesBeneath + " + " + obpn_sizeChildCount + " + " + obpn_sizeChildOffset);
            out.println("OBPN no. nodes:  " + obpn_nNodes);
            out.println("OBPN trie depth: " + maxValueLength);
            out.println("OBPN footprint:  " + obpn_footprint + " in bytes");

            // flatten trie footprint calculation, case of Multi-Byte-Per-Node
            out.println("----------------------------------------------------------------------------");
            out.println("MBPN max fan out:       " + mbpn_maxFanOut);
            out.println("MBPN no. child lookups: " + mbpn_nChildLookups);
            out.println("MBPN total fan out:     " + mbpn_nTotalFanOut);
            out.println("MBPN average fan out:   " + (double) mbpn_nTotalFanOut / mbpn_nChildLookups);
            out.println("MBPN values size total: " + mbpn_sizeValueTotal);
            out.println("MBPN node size:         " + (mbpn_sizeNoValueBytes + mbpn_sizeNoValueBeneath + mbpn_sizeChildOffset) + " = " + mbpn_sizeNoValueBytes + " + " + mbpn_sizeNoValueBeneath + " + " + mbpn_sizeChildOffset);
            out.println("MBPN no. nodes:         " + mbpn_nNodes);
            out.println("MBPN trie depth:        " + mbpn_trieDepth);
            out.println("MBPN footprint:         " + mbpn_footprint + " in bytes");
        }
    }

    /** out print some statistics of the trie and the dictionary built from it */
    public Stats stats() {
        // calculate nEndValueBeneath
        //从最底层开始计算,计算每一个node有多少个单词结尾的完整单词
        traversePostOrder(new Visitor() {
            @Override
            public void visit(Node n, int level) {
                n.nValuesBeneath = n.isEndOfValue ? 1 : 0;//有多少个单词结尾
                for (Node c : n.children)
                    n.nValuesBeneath += c.nValuesBeneath;//因为底层的子节点已经计算完成了,因此当前节点是可以直接与子节点的求和的
            }
        });

        // run stats
        final Stats s = new Stats();
        final ArrayList<Integer> lenAtLvl = new ArrayList<Integer>();
        //从头到尾循环
        traverse(new Visitor() {
            @Override
            public void visit(Node n, int level) {
                if (n.isEndOfValue) //是单词结尾
                    s.nValues++; //总的单词数++
                s.nValueBytesPlain += n.part.length * n.nValuesBeneath;//所有的单词占用总字节(没有压缩前)
                s.nValueBytesCompressed += n.part.length;//压缩后所有单词占用总字节
                s.mbpn_nNodes++;//有多少个node节点
                if (s.mbpn_trieDepth < level + 1)
                    s.mbpn_trieDepth = level + 1;//node节点的最大深度
                if (n.children.size() > 0) {
                    if (s.mbpn_maxFanOut < n.children.size())
                        s.mbpn_maxFanOut = n.children.size();//node中最多有多少个子节点
                    int childLookups = n.nValuesBeneath - (n.isEndOfValue ? 1 : 0);//该node节点下所有子子孙孙,不包含node本身,有多少个完整的单词
                    s.mbpn_nChildLookups += childLookups;
                    s.mbpn_nTotalFanOut += childLookups * n.children.size();
                }

                if (level < lenAtLvl.size())
                    lenAtLvl.set(level, n.part.length);
                else
                    lenAtLvl.add(n.part.length);
                int lenSoFar = 0;
                for (int i = 0; i <= level; i++) //计算从头到现在的级别,一共有多少个字节
                    lenSoFar += lenAtLvl.get(i);
                if (lenSoFar > s.maxValueLength)
                    s.maxValueLength = lenSoFar; //最长的单词占用多少个字节
            }
        });

        // flatten trie footprint calculation, case of One-Byte-Per-Node
        s.obpn_sizeValue = 1;
        s.obpn_sizeNoValuesBeneath = BytesUtil.sizeForValue(s.nValues);
        s.obpn_sizeChildCount = 1;
        s.obpn_sizeChildOffset = 5; // MSB used as isEndOfValue flag
        s.obpn_nNodes = s.nValueBytesCompressed; // no. nodes is the total number of compressed bytes in OBPN
        s.obpn_footprint = s.obpn_nNodes * (long) (s.obpn_sizeValue + s.obpn_sizeNoValuesBeneath + s.obpn_sizeChildCount + s.obpn_sizeChildOffset);
        while (true) { // minimize the offset size to match the footprint
            long t = s.obpn_nNodes * (long) (s.obpn_sizeValue + s.obpn_sizeNoValuesBeneath + s.obpn_sizeChildCount + s.obpn_sizeChildOffset - 1);
            if (BytesUtil.sizeForValue(t * 2) <= s.obpn_sizeChildOffset - 1) { // *2 because MSB of offset is used for isEndOfValue flag
                s.obpn_sizeChildOffset--;
                s.obpn_footprint = t;
            } else
                break;
        }

        // flatten trie footprint calculation, case of Multi-Byte-Per-Node
        s.mbpn_sizeValueTotal = s.nValueBytesCompressed;
        s.mbpn_sizeNoValueBytes = 1;
        s.mbpn_sizeNoValueBeneath = BytesUtil.sizeForValue(s.nValues);
        s.mbpn_sizeChildOffset = 5;
        s.mbpn_footprint = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (long) (s.mbpn_sizeNoValueBytes + s.mbpn_sizeNoValueBeneath + s.mbpn_sizeChildOffset);
        while (true) { // minimize the offset size to match the footprint
            long t = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (long) (s.mbpn_sizeNoValueBytes + s.mbpn_sizeNoValueBeneath + s.mbpn_sizeChildOffset - 1);
            if (BytesUtil.sizeForValue(t * 4) <= s.mbpn_sizeChildOffset - 1) { // *4 because 2 MSB of offset is used for isEndOfValue & isEndChild flag
                s.mbpn_sizeChildOffset--;
                s.mbpn_footprint = t;
            } else
                break;
        }

        return s;
    }

    /** out print trie for debug */
    public void print() {
        print(System.out);
    }

    public void print(final PrintStream out) {
        traverse(new Visitor() {
            @Override
            public void visit(Node n, int level) {
                try {
                    for (int i = 0; i < level; i++){//每一层循环.打印若干个空格
                        out.print("  ");
                    }
                    out.print(new String(n.part, "UTF-8"));
                    out.print(" - ");
                    if (n.nValuesBeneath > 0)
                        out.print(n.nValuesBeneath);
                    if (n.isEndOfValue)
                        out.print("*");
                    out.print("\n");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //缓冲区间对象
    private CompleteParts completeParts = new CompleteParts();

    private class CompleteParts {
        byte[] data = new byte[4096];//缓冲池
        int current = 0;//当前已经在缓冲池中写到哪个位置了

        //追加参数字节数组到data中
        public void append(byte[] part) {
            while (current + part.length > data.length)
                expand();//扩容

            System.arraycopy(part, 0, data, current, part.length);//复制part的数据,到data数组中,从current位置开始写数据
            current += part.length;//增加current位置
        }

        //丢弃最后若干个字节位置
        public void withdraw(int size) {
            current -= size;
        }

        //获取可用的字节数组
        public byte[] retrieve() {
            return Arrays.copyOf(data, current);
        }

        //扩容
        private void expand() {
            byte[] temp = new byte[2 * data.length];
            System.arraycopy(data, 0, temp, 0, data.length);
            data = temp;
        }
    }

    // there is a 255 limitation of length for each node's part.
    // we interpolate nodes to satisfy this when a node's part becomes
    // too long(overflow)
    //限制255是每一个node的字节数组上限,我们篡改这个节点的内容,以满足255的规则
    private void checkOverflowParts(Node node) {
        LinkedList<Node> childrenCopy = new LinkedList<Node>(node.children);
        for (Node child : childrenCopy) {
            if (child.part.length > 255) {
                byte[] first255 = Arrays.copyOf(child.part, 255);//选择前面255个字节

                completeParts.append(node.part);//因为所有child都与父组合的,因此先组装父亲的part
                completeParts.append(first255);//在追加儿子的255
                byte[] visited = completeParts.retrieve();//获取全部信息
                this.addValue(visited);//重新添加
                //清空添加的字符内容
                completeParts.withdraw(255);
                completeParts.withdraw(node.part.length);//清空父Node的内容
            }
        }

        completeParts.append(node.part); // by here the node.children may have been changed
        for (Node child : node.children) {
            checkOverflowParts(child);//递归子子孙孙(这部分日后可以详细研究,因为这么写很容易产生bug)
        }
        completeParts.withdraw(node.part.length);
    }

    /**
     * Flatten the trie into a byte array for a minimized memory footprint.
     * Lookup remains fast. Cost is inflexibility to modify (becomes immutable).
     * 
     * Flattened node structure is HEAD + NODEs, for each node:
     * - o byte, offset to child node, o = stats.mbpn_sizeChildOffset
     *    - 1 bit, isLastChild flag, the 1st MSB of o
     *    - 1 bit, isEndOfValue flag, the 2nd MSB of o
     * - c byte, number of values beneath, c = stats.mbpn_sizeNoValueBeneath
     * - 1 byte, number of value bytes
     * - n byte, value bytes
     */
    public TrieDictionary<T> build(int baseId) {
        byte[] trieBytes = buildTrieBytes(baseId);
        TrieDictionary<T> r = new TrieDictionary<T>(trieBytes);
        return r;
    }

    protected byte[] buildTrieBytes(int baseId) {
        checkOverflowParts(this.root);

        Stats stats = stats();//统计
        int sizeNoValuesBeneath = stats.mbpn_sizeNoValueBeneath;
        int sizeChildOffset = stats.mbpn_sizeChildOffset;
        
        if (stats.mbpn_footprint > _2GB)
            throw new RuntimeException("Too big dictionary, dictionary cannot be bigger than 2GB");

        // write head 写入head头信息
        byte[] head;
        try {
            ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
            DataOutputStream headOut = new DataOutputStream(byteBuf);
            headOut.write(TrieDictionary.MAGIC);
            headOut.writeShort(0); // head size, will back fill ,head头的字节总数
            headOut.writeInt((int) stats.mbpn_footprint); // body size
            headOut.write(sizeChildOffset);
            headOut.write(sizeNoValuesBeneath);
            headOut.writeShort(baseId);
            headOut.writeShort(stats.maxValueLength);
            headOut.writeUTF(bytesConverter == null ? "" : bytesConverter.getClass().getName());
            headOut.close();
            head = byteBuf.toByteArray();
            BytesUtil.writeUnsigned(head.length, head, TrieDictionary.MAGIC_SIZE_I, 2);//将head头字节数写入到输出流中
        } catch (IOException e) {
            throw new RuntimeException(e); // shall not happen, as we are writing in memory
        }

        byte[] trieBytes = new byte[(int) stats.mbpn_footprint + head.length];//最终要的字节数组
        System.arraycopy(head, 0, trieBytes, 0, head.length);//先复制header信息

        LinkedList<Node> open = new LinkedList<Node>();
        IdentityHashMap<Node, Integer> offsetMap = new IdentityHashMap<Node, Integer>();//每一个node对应的offset位置

        // write body
        int o = head.length;
        offsetMap.put(root, o);
        o = build_writeNode(root, o, true, sizeNoValuesBeneath, sizeChildOffset, trieBytes);//返回值是位置,因为root就一个node,因此他就是最后一个node,所以参数设置为true
        if (root.children.isEmpty() == false) //不是空
            open.addLast(root);//追加root到队列

        while (open.isEmpty() == false) {//只要不是null,队列有内容,则不断迭代循环
            Node parent = open.removeFirst();//获取一个元素
            build_overwriteChildOffset(offsetMap.get(parent), o - head.length, sizeChildOffset, trieBytes);
            for (int i = 0; i < parent.children.size(); i++) {//所有的子node
                Node c = parent.children.get(i);
                boolean isLastChild = (i == parent.children.size() - 1);
                offsetMap.put(c, o);//每一个node对应offset位置
                o = build_writeNode(c, o, isLastChild, sizeNoValuesBeneath, sizeChildOffset, trieBytes);
                if (c.children.isEmpty() == false)//如果是有子节点的,因此将子节点追加到open集合中
                    open.addLast(c);
            }
        }

        if (o != trieBytes.length)
            throw new RuntimeException();
        return trieBytes;
    }

    private void build_overwriteChildOffset(int parentOffset, int childOffset, int sizeChildOffset, byte[] trieBytes) {
        int flags = (int) trieBytes[parentOffset] & (TrieDictionary.BIT_IS_LAST_CHILD | TrieDictionary.BIT_IS_END_OF_VALUE);
        BytesUtil.writeUnsigned(childOffset, trieBytes, parentOffset, sizeChildOffset);
        trieBytes[parentOffset] |= flags;
    }

    /**
     * 将node写入到字节数组中
     * @param n 要准备写入的node
     * @param offset 在trieBytes中开始从哪个偏移写入数据
     * @param isLastChild 是否是child中最后一个节点,即是否下一个是另外node了
     * @param sizeNoValuesBeneath
     * @param sizeChildOffset
     * @param trieBytes 最终大数组,向该数组中写入字节内容
     * @return
     */
    private int build_writeNode(Node n, int offset, boolean isLastChild, int sizeNoValuesBeneath, int sizeChildOffset, byte[] trieBytes) {
        int o = offset;//开始位置是否超过了20G了
        if (o > _2GB)
            throw new IllegalStateException();

        //设置状态,使用一个字节
        if (isLastChild)
            trieBytes[o] |= TrieDictionary.BIT_IS_LAST_CHILD;//是最后一个节点
        if (n.isEndOfValue)
            trieBytes[o] |= TrieDictionary.BIT_IS_END_OF_VALUE;//是单词结尾
        o += sizeChildOffset;

        // nValuesBeneath
        BytesUtil.writeUnsigned(n.nValuesBeneath, trieBytes, o, sizeNoValuesBeneath);//将一共多少个单词结尾的单词这个int写入到字节数组中
        o += sizeNoValuesBeneath;

        // nValueBytes
        if (n.part.length > 255)
            throw new RuntimeException();
        BytesUtil.writeUnsigned(n.part.length, trieBytes, o, 1);//因为不超过255,因此字节长度就用一个字节表示,即写入字节长度
        o++;

        // valueBytes
        System.arraycopy(n.part, 0, trieBytes, o, n.part.length);//写入字节内容
        o += n.part.length;

        return o;
    }

}
