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

package org.apache.kylin.cube.cli;

import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * used to bulk refresh the cube's signature in metadata store.
 * won't be useful unless something went wrong.
 * 将cube的描述内容对应的MD5签名重新更新一下
 * 该类正常不会有什么用,除非有什么错误发生
 */
public class CubeSignatureRefresher {
    private static final Logger logger = LoggerFactory.getLogger(CubeSignatureRefresher.class);

    private KylinConfig config = null;
    private ResourceStore store;
    private String[] cubeNames;
    private List<String> updatedResources = Lists.newArrayList();//已经更新的cube描述对象路径
    private List<String> errorMsgs = Lists.newArrayList();//过程中产生的错误信息

    public CubeSignatureRefresher(String[] cubes) {
        config = KylinConfig.getInstanceFromEnv();
        store = ResourceStore.getStore(config);
        cubeNames = cubes;
    }

    public void update() {
        logger.info("Reloading Cube Metadata from store: " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));//存储一个web页面中配置的cube信息 的根目录
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(config);
        List<CubeDesc> cubeDescs;
        if (ArrayUtils.isEmpty(cubeNames)) {//获取所有的cube描述内容
            cubeDescs = cubeDescManager.listAllDesc();
        } else {
            String[] names = cubeNames[0].split(",");
            if (ArrayUtils.isEmpty(names))
                return;
            cubeDescs = Lists.newArrayListWithCapacity(names.length);
            for (String name : names) {
                cubeDescs.add(cubeDescManager.getCubeDesc(name));
            }
        }
        for (CubeDesc cubeDesc : cubeDescs) {
            updateCubeDesc(cubeDesc);
        }

        verify();
    }

    private void verify() {
        MetadataManager.getInstance(config).reload();
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(config);
        CubeManager.getInstance(config);
        ProjectManager.getInstance(config);
    }

    public List<String> getErrorMsgs() {
        return errorMsgs;
    }

    //更新一个cube对象
    private void updateCubeDesc(CubeDesc cubeDesc) {
        try {
            String calculatedSign = cubeDesc.calculateSignature();//计算新的签名
            if (cubeDesc.getSignature() == null || (!cubeDesc.getSignature().equals(calculatedSign))) {//说明签名不一致
                cubeDesc.setSignature(calculatedSign);//设置新的签名
                store.putResource(cubeDesc.getResourcePath(), cubeDesc, CubeDescManager.CUBE_DESC_SERIALIZER);//重新更新
                updatedResources.add(cubeDesc.getResourcePath());
            }
        } catch (Exception e) {
            logger.error("error", e);
            errorMsgs.add("Update CubeDesc[" + cubeDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        if (args != null && args.length > 1) {
            System.out.println("Usage: java CubeSignatureRefresher [Cubes]; e.g, cube1,cube2 ");
            return;
        }

        CubeSignatureRefresher metadataUpgrade = new CubeSignatureRefresher(args);
        metadataUpgrade.update();//重新更新签名

        logger.info("=================================================================");
        logger.info("Run CubeSignatureRefresher completed;");

        if (!metadataUpgrade.updatedResources.isEmpty()) {//打印已经重新变更签名的cube路径
            logger.info("Following resources are updated successfully:");
            for (String s : metadataUpgrade.updatedResources) {
                logger.info(s);
            }
        } else {//说明没有签名变更
            logger.warn("No resource updated.");
        }

        if (!metadataUpgrade.errorMsgs.isEmpty()) {//打印所有的错误信息
            logger.info("Here are the error/warning messages, you may need to check:");
            for (String s : metadataUpgrade.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The update succeeds.");
        }

        logger.info("=================================================================");
    }

}
