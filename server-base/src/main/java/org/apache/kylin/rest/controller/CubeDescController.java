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

package org.apache.kylin.rest.controller;

import java.io.IOException;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 * 该接口获取的是cube在web页面上填写的表单内容,即cube的描述内容
 */
@Controller
@RequestMapping(value = "/cube_desc")
public class CubeDescController {

    @Autowired
    private CubeService cubeService;

    /**
     * Get detail information of the "Cube ID"
     * 通过cube的name获取cube对象,返回值是数组,其实真实的返回值也是一个对象,即数组里面就有一个元素
     * @param cubeName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET })
    @ResponseBody
    public CubeDesc[] getCube(@PathVariable String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            return null;
        }
        CubeDesc cSchema = cubeInstance.getDescriptor();
        if (cSchema != null) {
            return new CubeDesc[] { cSchema };
        } else {
            return null;
        }
    }

    /**
     * Get detail information of the "Cube ID"
     * return CubeDesc instead of CubeDesc[]
     * 通过cube的name获取cube对象,返回值就是一个对象
     * @param cubeName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/desc", method = { RequestMethod.GET })
    @ResponseBody
    public CubeDesc getDesc(@PathVariable String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            return null;
        }
        CubeDesc cSchema = cubeInstance.getDescriptor();
        if (cSchema != null) {
            return cSchema;
        } else {
            return null;
        }
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

}
