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

package org.apache.kylin.storage.hbase;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput2Transition;

@SuppressWarnings("unused")
//used by reflection
public class HBaseStorage implements IStorage {

    public final static String v2CubeStorageQuery = "org.apache.kylin.storage.hbase.cube.v2.CubeStorageQuery";
    public final static String v1CubeStorageQuery = "org.apache.kylin.storage.hbase.cube.v1.CubeStorageQuery";
    public static String overwriteStorageQuery = null;//for test case

    @Override
    public IStorageQuery createQuery(IRealization realization) {

        if (realization.getType() == RealizationType.CUBE) {

            CubeInstance cubeInstance = (CubeInstance) realization;
            String cubeStorageQuery;
            if (cubeInstance.getStorageType() == IStorageAware.ID_HBASE) {//v2 query engine cannot go with v1 storage now
                cubeStorageQuery = v1CubeStorageQuery;
            } else if (overwriteStorageQuery != null) {
                cubeStorageQuery = overwriteStorageQuery;
            } else if ("v1".equalsIgnoreCase(BackdoorToggles.getHbaseCubeQueryVersion())) {
                cubeStorageQuery = v1CubeStorageQuery;
            } else {
                cubeStorageQuery = v2CubeStorageQuery;//by default use v2
            }

            IStorageQuery ret;
            try {
                ret = (IStorageQuery) Class.forName(cubeStorageQuery).getConstructor(CubeInstance.class).newInstance((CubeInstance) realization);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize storage query for " + cubeStorageQuery, e);
            }

            return ret;
        } else {
            throw new IllegalArgumentException("Unknown realization type " + realization.getType());
        }
    }

    //获取partition分区列
    private static TblColRef getPartitionCol(IRealization realization) {
        String modelName = realization.getDataModelDesc().getName();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getDataModelDesc(modelName);
        PartitionDesc partitionDesc = dataModelDesc.getPartitionDesc();
        Preconditions.checkArgument(partitionDesc != null, "PartitionDesc for " + realization + " is null!");
        TblColRef partitionColRef = partitionDesc.getPartitionDateColumnRef();
        Preconditions.checkArgument(partitionColRef != null, "getPartitionDateColumnRef for " + realization + " is null");
        return partitionColRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput.class) {
            return (I) new HBaseMROutput();
        } else if (engineInterface == IMROutput2.class) {
            return (I) new HBaseMROutput2Transition();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
