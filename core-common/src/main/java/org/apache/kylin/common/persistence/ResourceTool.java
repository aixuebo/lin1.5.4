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

package org.apache.kylin.common.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;

/**
 * 操作实例化资源的工具类
 */
public class ResourceTool {

    private static String[] includes = null;//包含的集合
    private static String[] excludes = null;//排除的集合

    public static void main(String[] args) throws IOException {
        args = StringUtil.filterSystemArgs(args);

        if (args.length == 0) {
            System.out.println("Usage: ResourceTool list  RESOURCE_PATH");//list 目录path  :打印path目录下所有的子目录信息
            System.out.println("Usage: ResourceTool download  LOCAL_DIR");
            System.out.println("Usage: ResourceTool upload    LOCAL_DIR");
            System.out.println("Usage: ResourceTool reset");//reset   删除/根目录下所有元素,只要满足includes的都会删除,只要不excludes的也会会删除
            System.out.println("Usage: ResourceTool remove RESOURCE_PATH");//remove path   删除path目录下所有元素,只要满足includes的都会删除,只要不excludes的也会会删除
            System.out.println("Usage: ResourceTool cat RESOURCE_PATH");//cat 文件path          :path是一个文件路径,读取文件内容打印到控制台
            return;
        }

        String include = System.getProperty("include");//按照逗号拆分
        if (include != null) {
            includes = include.split("\\s*,\\s*");
        }
        String exclude = System.getProperty("exclude");//按照逗号拆分
        if (exclude != null) {
            excludes = exclude.split("\\s*,\\s*");
        }

        String cmd = args[0];
        switch (cmd) {
        case "list":
            list(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        case "cat":
            cat(KylinConfig.getInstanceFromEnv(), args[1]);
            break;

        case "reset":
            reset(args.length == 1 ? KylinConfig.getInstanceFromEnv() : KylinConfig.createInstanceFromUri(args[1]));
            break;
        case "remove":
            remove(KylinConfig.getInstanceFromEnv(), args[1]);
            break;

        case "download":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]));
            break;
        case "fetch":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), args[2]);
            break;

        case "upload":
            copy(KylinConfig.createInstanceFromUri(args[1]), KylinConfig.getInstanceFromEnv());
            break;

        default:
            System.out.println("Unknown cmd: " + cmd);
        }
    }

    //path是一个文件路径,读取文件内容打印到控制台
    public static void cat(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        InputStream is = store.getResource(path).inputStream;
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(br);
        }
    }

    //打印path目录下所有的子目录信息
    public static void list(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        NavigableSet<String> result = store.listResources(path);
        System.out.println("" + result);
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        copyR(src, dst, path);
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);
        for (String path : paths) {
            copyR(src, dst, path);
        }
    }

    //存储系统之间全部copy信息
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {

        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);
        copyR(src, dst, "/");
    }

    /**
     * 不断循环path下所有文件,符合条件的,就复制到另外一个存储系统上
     */
    public static void copyR(ResourceStore src, ResourceStore dst, String path) throws IOException {
        NavigableSet<String> children = src.listResources(path);//path下所有子文件

        if (children == null) {
            // case of resource (not a folder)
            if (matchFilter(path)) {
                try {
                    RawResource res = src.getResource(path);//获取对应的文件
                    if (res != null) {
                        dst.putResource(path, res.inputStream, res.timestamp);//向目录存储系统写入该文件内容
                        res.inputStream.close();
                    } else {
                        System.out.println("Resource not exist for " + path);
                    }
                } catch (Exception ex) {
                    System.err.println("Failed to open " + path);
                    ex.printStackTrace();
                }
            }
        } else {
            // case of folder
            for (String child : children)
                copyR(src, dst, child);
        }
    }

    private static boolean matchFilter(String path) {
        if (includes != null) {//path是以include中元素开头的,则返回true,否则返回false
            boolean in = false;
            for (String include : includes) {
                in = in || path.startsWith(include);
            }
            if (!in)
                return false;
        }
        if (excludes != null) {
            for (String exclude : excludes) {//path是以excludes中元素开头的,则返回false,否则返回true
                if (path.startsWith(exclude))
                    return false;
            }
        }
        return true;
    }

    //删除/根目录下所有元素,只要满足includes的都会删除,只要不excludes的也会会删除
    public static void reset(KylinConfig config) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, "/");
    }

    public static void resetR(ResourceStore store, String path) throws IOException {
        NavigableSet<String> children = store.listResources(path);
        if (children == null) { // path is a resource (not a folder)
            if (matchFilter(path)) {//说明匹配成功
                store.deleteResource(path);//删除路径下内容
            }
        } else {
            for (String child : children)
                resetR(store, child);
        }
    }

    //删除path下所有元素,只要满足includes的都会删除,只要不excludes的也会会删除
    private static void remove(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, path);
    }
}
