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

package org.apache.kylin.source;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;

//Source接口的实现类工厂方式
public class SourceFactory {

    private static ImplementationSwitch<ISource> sources;
    static {
        //输入源引擎默认只有HiveSource引擎
        //r.put(0, "org.apache.kylin.source.hive.HiveSource");
        Map<Integer, String> impls = KylinConfig.getInstanceFromEnv().getSourceEngines();
        sources = new ImplementationSwitch<ISource>(impls, ISource.class);
    }

    public static ISource tableSource(ISourceAware aware) {
        return sources.get(aware.getSourceType());
    }

    public static ReadableTable createReadableTable(TableDesc table) {
        return tableSource(table).createReadableTable(table);
    }

    public static <T> T createEngineAdapter(ISourceAware table, Class<T> engineInterface) {
        return tableSource(table).adaptToBuildEngine(engineInterface);
    }

    public static List<String> getMRDependentResources(TableDesc table) {
        return tableSource(table).getMRDependentResources(table);
    }

}
