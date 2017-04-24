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

package org.apache.kylin.metadata.tuple;

import java.util.Iterator;

/**
 * 从集合中迭代数据
 */
public class SimpleTupleIterator implements ITupleIterator {

    private Iterator<ITuple> backend;

    public SimpleTupleIterator(Iterator<ITuple> backend) {
        this.backend = backend;
    }

    @Override
    public boolean hasNext() {
        return backend.hasNext();
    }

    @Override
    public ITuple next() {
        return backend.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

}
