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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;

/**
 * @author yangli9
 */
public class AggregateRegionObserver extends BaseRegionObserver {

    // HBase uses common logging (vs. Kylin uses slf4j)
    static final Log LOG = LogFactory.getLog(AggregateRegionObserver.class);

    static final String COPROCESSOR_ENABLE = "_Coprocessor_Enable";
    static final String TYPE = "_Type";//返回rowkey中的列,每一个列属于table中的位置
    static final String PROJECTOR = "_Projector";
    static final String AGGREGATORS = "_Aggregators";
    static final String FILTER = "_Filter";
    static final String BEHAVIOR = "_Behavior";

    //是使用RegionObserver在PostScannerOpen Hook中将RegionScanner替换成支持Aggregation工作的定制化的Scanner
    @Override
    public final RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> ctxt, final Scan scan, final RegionScanner innerScanner) throws IOException {

        boolean copAbortOnError = ctxt.getEnvironment().getConfiguration().getBoolean(RegionCoprocessorHost.ABORT_ON_ERROR_KEY, RegionCoprocessorHost.DEFAULT_ABORT_ON_ERROR);

        // never throw out exception that could abort region server
        if (copAbortOnError) {
            try {
                return doPostScannerObserver(ctxt, scan, innerScanner);
            } catch (Throwable e) {
                LOG.error("Kylin Coprocessor Error", e);
                return innerScanner;
            }
        } else {
            return doPostScannerObserver(ctxt, scan, innerScanner);
        }
    }

    private RegionScanner doPostScannerObserver(final ObserverContext<RegionCoprocessorEnvironment> ctxt, final Scan scan, final RegionScanner innerScanner) throws IOException {
        byte[] coprocessorEnableBytes = scan.getAttribute(COPROCESSOR_ENABLE);
        if (coprocessorEnableBytes == null || coprocessorEnableBytes.length == 0 || coprocessorEnableBytes[0] == 0) {
            return innerScanner;
        }

        //返回rowkey中的列,每一个列属于table中的位置
        byte[] typeBytes = scan.getAttribute(TYPE);
        CoprocessorRowType type = CoprocessorRowType.deserialize(typeBytes);

        byte[] projectorBytes = scan.getAttribute(PROJECTOR);
        CoprocessorProjector projector = CoprocessorProjector.deserialize(projectorBytes);

        byte[] aggregatorBytes = scan.getAttribute(AGGREGATORS);
        ObserverAggregators aggregators = ObserverAggregators.deserialize(aggregatorBytes);

        byte[] filterBytes = scan.getAttribute(FILTER);
        CoprocessorFilter filter = CoprocessorFilter.deserialize(filterBytes);

        StorageSideBehavior storageSideBehavior = StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM;
        try {
            byte[] behavior = scan.getAttribute(BEHAVIOR);
            if (behavior != null && behavior.length != 0) {
                storageSideBehavior = StorageSideBehavior.valueOf(new String(behavior));
            }
        } catch (Exception e) {
            LOG.error("failed to parse behavior,using default behavior SCAN_FILTER_AGGR_CHECKMEM", e);
            storageSideBehavior = StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM;
        }

        // start/end region operation & sync on scanner is suggested by the
        // javadoc of RegionScanner.nextRaw()
        // FIXME: will the lock still work when a iterator is returned? is it safe? Is readonly attribute helping here? by mhb
        HRegion region = ctxt.getEnvironment().getRegion();
        region.startRegionOperation();
        try {
            synchronized (innerScanner) {
                return new AggregationScanner(type, filter, projector, aggregators, innerScanner, storageSideBehavior);
            }
        } finally {
            region.closeRegionOperation();
        }
    }
}
