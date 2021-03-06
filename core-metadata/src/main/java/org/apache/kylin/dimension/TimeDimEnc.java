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

package org.apache.kylin.dimension;

/**
 * This encoding is meant to be IDENTICAL to TimeStrDictionary for 100% backward compatibility.
 */
public class TimeDimEnc extends AbstractDateDimEnc {
    private static final long serialVersionUID = 1L;

    public static final String ENCODING_NAME = "time";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new TimeDimEnc();
        }
    };

    //将long类型的转换成4个字节长度
    public TimeDimEnc() {
        super(4, new IMillisCodec() {
            private static final long serialVersionUID = 1L;

            @Override
            public long millisToCode(long millis) {
                return millis / 1000;
            }

            @Override
            public long codeToMillis(long code) {
                return code * 1000;
            }
        });
    }

}
