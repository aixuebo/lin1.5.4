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

/**
 * This class uses MAX_DIGITS_BEFORE_DECIMAL_POINT (=19) instead of legacy (=16).
 * 该类就是让正数部分长度是19,而不是16
 */
@SuppressWarnings("serial")
public class NumberDictionary2<T> extends NumberDictionary<T> {

    static ThreadLocal<NumberBytesCodec> localCodec = new ThreadLocal<NumberBytesCodec>();

    // ============================================================================

    public NumberDictionary2() { // default constructor for Writable interface
        super();
    }

    public NumberDictionary2(byte[] trieBytes) {
        super(trieBytes);
    }

    protected NumberBytesCodec getCodec() {
        NumberBytesCodec codec = localCodec.get();
        if (codec == null) {
            codec = new NumberBytesCodec(MAX_DIGITS_BEFORE_DECIMAL_POINT);
            localCodec.set(codec);
        }
        return codec;
    }

}
