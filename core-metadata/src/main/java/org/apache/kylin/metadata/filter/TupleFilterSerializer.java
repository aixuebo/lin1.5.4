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

package org.apache.kylin.metadata.filter;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http://eli.thegreenplace.net/2011/09/29/an-interesting-tree-serialization-algorithm-from-dwarf
 * 
 * @author xjiang
 * TupleFilter的序列化与反序列化,可以允许filter传输
 */
public class TupleFilterSerializer {

    private static final Logger logger = LoggerFactory.getLogger(TupleFilterSerializer.class);

    public interface Decorator {
        TupleFilter onSerialize(TupleFilter filter);
    }

    private static final int BUFFER_SIZE = 65536;

    //所有支持的关联操作集合--ID和操作的映射
    private static final Map<Integer, TupleFilter.FilterOperatorEnum> ID_OP_MAP = new HashMap<Integer, TupleFilter.FilterOperatorEnum>();

    static {
        for (TupleFilter.FilterOperatorEnum op : TupleFilter.FilterOperatorEnum.values()) {
            ID_OP_MAP.put(op.getValue(), op);
        }
    }

    public static byte[] serialize(TupleFilter rootFilter, IFilterCodeSystem<?> cs) {
        return serialize(rootFilter, null, cs);
    }

    public static byte[] serialize(TupleFilter rootFilter, Decorator decorator, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer;
        int bufferSize = BUFFER_SIZE;
        while (true) {
            try {
                buffer = ByteBuffer.allocate(bufferSize);
                internalSerialize(rootFilter, decorator, buffer, cs);
                break;
            } catch (BufferOverflowException e) {
                logger.info("Buffer size {} cannot hold the filter, resizing to 4 times", bufferSize);
                bufferSize *= 4;
            }
        }
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    private static void internalSerialize(TupleFilter filter, Decorator decorator, ByteBuffer buffer, IFilterCodeSystem<?> cs) {
        if (decorator != null) { // give decorator a chance to manipulate the output filter
            filter = decorator.onSerialize(filter);
        }

        if (filter == null) {
            return;
        }

        if (filter.hasChildren()) {
            // serialize filter+true
            serializeFilter(1, filter, buffer, cs);//1表示开始序列化filter本身
            // serialize children
            for (TupleFilter child : filter.getChildren()) {
                internalSerialize(child, decorator, buffer, cs);
            }
            // serialize none
            serializeFilter(-1, filter, buffer, cs);//-1表示序列化完成filter
        } else {
            // serialize filter+false
            serializeFilter(0, filter, buffer, cs);
        }
    }

    /**
     *
     * @param flag  0 表示没有子filter,1表示开始序列化有子类的filter,-1表示已经序列化完成有子类的filter
     * @param filter
     * @param buffer
     * @param cs
     */
    private static void serializeFilter(int flag, TupleFilter filter, ByteBuffer buffer, IFilterCodeSystem<?> cs) {
        if (flag < 0) {
            BytesUtil.writeVInt(-1, buffer);
        } else {
            int opVal = filter.getOperator().getValue();
            BytesUtil.writeVInt(opVal, buffer);//序列化该filter的操作
            filter.serialize(cs, buffer);//序列化filter本身
            BytesUtil.writeVInt(flag, buffer);//序列化filter的父子关系
        }
    }

    //反序列化
    public static TupleFilter deserialize(byte[] bytes, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        TupleFilter rootFilter = null;//根过滤器
        Stack<TupleFilter> parentStack = new Stack<TupleFilter>();//堆栈用于过滤器操作的产生
        while (buffer.hasRemaining()) {
            int opVal = BytesUtil.readVInt(buffer);//读取flag
            if (opVal < 0) {//说明一个filter结束了
                parentStack.pop();//弹出堆栈
                continue;
            }

            // deserialize filter
            TupleFilter filter = createTupleFilter(opVal);//创建一个过滤器
            filter.deserialize(cs, buffer);//反序列化该过滤器

            if (rootFilter == null) {//首先要创建一个根过滤器
                // push root to stack
                rootFilter = filter;
                parentStack.push(filter);//压入堆栈
                BytesUtil.readVInt(buffer);//读取该flag,但是根的flag没意义,所以不作处理,只是读取出来丢弃掉即可
                continue;
            }

            // add filter to parent
            TupleFilter parentFilter = parentStack.peek();//获取父filter
            if (parentFilter != null) {
                parentFilter.addChild(filter);//向父filter中加入该filter
            }

            // push filter to stack or not based on having children or not
            int hasChild = BytesUtil.readVInt(buffer);//读取flag
            if (hasChild == 1) {//表示有子类
                parentStack.push(filter);//向堆栈中添加该
            }
        }
        return rootFilter;
    }

    //根据操作符不同,创建不同的过滤器
    private static TupleFilter createTupleFilter(int opVal) {
        TupleFilter.FilterOperatorEnum op = ID_OP_MAP.get(opVal);
        if (op == null) {
            throw new IllegalStateException("operator value is " + opVal);
        }
        TupleFilter filter = null;
        switch (op) {
        case AND:
        case OR:
        case NOT:
            filter = new LogicalTupleFilter(op);
            break;
        case EQ:
        case NEQ:
        case LT:
        case LTE:
        case GT:
        case GTE:
        case IN:
        case ISNULL:
        case ISNOTNULL:
            filter = new CompareTupleFilter(op);
            break;
        case EXTRACT:
            filter = new ExtractTupleFilter(op);
            break;
        case CASE:
            filter = new CaseTupleFilter();
            break;
        case COLUMN:
            filter = new ColumnTupleFilter(null);
            break;
        case CONSTANT:
            filter = new ConstantTupleFilter();
            break;
        case DYNAMIC:
            filter = new DynamicTupleFilter(null);
            break;
        case FUNCTION:
            filter = new BuiltInFunctionTupleFilter(null);
            break;
        case UNSUPPORTED:
            filter = new UnsupportedTupleFilter(op);
            break;
        case EVAL_FUNC:
            filter = new EvaluatableFunctionTupleFilter(null);
            break;
        case MASSIN:
            filter = new MassInTupleFilter();
            break;
        default:
            throw new IllegalStateException("Error FilterOperatorEnum: " + op.getValue());
        }

        return filter;
    }
}
