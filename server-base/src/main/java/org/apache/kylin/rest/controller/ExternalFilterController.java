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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.rest.request.ExternalFilterRequest;
import org.apache.kylin.rest.service.ExtFilterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

/**
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/extFilter")
public class ExternalFilterController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ExternalFilterController.class);

    @Autowired
    private ExtFilterService extFilterService;

    //根据请求创建filter对象
    @RequestMapping(value = "/saveExtFilter", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, String> saveExternalFilter(@RequestBody ExternalFilterRequest request) throws IOException {
        Map<String, String> result = new HashMap();
        String filterProject = request.getProject();//获取哪个project
        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        desc.setUuid(UUID.randomUUID().toString());
        extFilterService.saveExternalFilter(desc);//真正意义上的保存到磁盘
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, filterProject); //对一个project 增加一组filter
        result.put("success", "true");
        return result;
    }

    //更新filter对象
    @RequestMapping(value = "/updateExtFilter", method = { RequestMethod.PUT })
    @ResponseBody
    public Map<String, String> updateExternalFilter(@RequestBody ExternalFilterRequest request) throws IOException {
        Map<String, String> result = new HashMap();
        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        extFilterService.updateExternalFilter(desc);//更新磁盘内容
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, request.getProject()); //对一个project 增加一组filter
        result.put("success", "true");
        return result;
    }

    //删除一个filter
    @RequestMapping(value = "/{filter}/{project}", method = { RequestMethod.DELETE })
    @ResponseBody
    public Map<String, String> removeFilter(@PathVariable String filter, @PathVariable String project) throws IOException {
        Map<String, String> result = new HashMap<String, String>();
        extFilterService.removeExtFilterFromProject(filter, project);//删除一个project下的filter
        extFilterService.removeExternalFilter(filter);//删除一个filter
        result.put("success", "true");
        return result;
    }

    //获取所有的filter
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<ExternalFilterDesc> getExternalFilters(@RequestParam(value = "project", required = true) String project) throws IOException {
        List<ExternalFilterDesc> filterDescs = Lists.newArrayList();
        filterDescs.addAll(extFilterService.getProjectManager().listExternalFilterDescs(project).values());
        return filterDescs;
    }

}
