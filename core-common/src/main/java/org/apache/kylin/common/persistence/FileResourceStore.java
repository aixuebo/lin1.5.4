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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FileResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(FileResourceStore.class);

    File root;

    public FileResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        root = new File(kylinConfig.getMetadataUrl()).getAbsoluteFile();
        if (root.exists() == false)
            throw new IllegalArgumentException("File not exist by '" + kylinConfig.getMetadataUrl() + "': " + root.getAbsolutePath());
    }

    //返回目录的子目录路径集合
    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        String[] names = file(folderPath).list();
        if (names == null) // not a directory
            return null;

        TreeSet<String> r = new TreeSet<>();
        String prefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        for (String n : names) {
            r.add(prefix + n);
        }
        return r;
    }

    //路径存在,并且是一个文件--返回true
    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        File f = file(resPath);
        return f.exists() && f.isFile(); // directory is not considered a resource
    }

    /**
     * 返回folderPath文件夹下文件集合,要求过滤最后修改时间在 timeStart 和 timeEndExclusive时间的文件
     */
    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        NavigableSet<String> resources = listResources(folderPath);//获取folderPath下面所有的文件
        if (resources == null)
            return Collections.emptyList();

        List<RawResource> result = Lists.newArrayListWithCapacity(resources.size());
        try {
            for (String res : resources) {//循环每一个文件
                long ts = getResourceTimestampImpl(res);//获取最后修改时间
                if (timeStart <= ts && ts < timeEndExclusive) {//在时间范围内的文件
                    RawResource resource = getResourceImpl(res);//获取文件内容
                    if (resource != null) // can be null if is a sub-folder
                        result.add(resource);
                }
            }
        } catch (IOException ex) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw ex;
        }
        return result;
    }

    //获取一个文件的流和最后修改时间
    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        File f = file(resPath);
        if (f.exists() && f.isFile()) {
            if (f.length() == 0) {
                logger.warn("Zero length file: " + f.getAbsolutePath());
            }
            return new RawResource(new FileInputStream(f), f.lastModified());
        } else {
            return null;
        }
    }

    //文件的最后修改时间
    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        File f = file(resPath);
        if (f.exists() && f.isFile())
            return f.lastModified();
        else
            return 0;
    }

    //将流的内容输出到resPath路径对应的文件中,并且最后修改最后访问时间为ts
    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        File f = file(resPath);
        f.getParentFile().mkdirs();
        FileOutputStream out = new FileOutputStream(f);
        try {
            IOUtils.copy(content, out);
        } finally {
            IOUtils.closeQuietly(out);
        }

        f.setLastModified(ts);
    }

    /**
     * 校验 并且存放数据
     * 校验
     * 1.路径resPath必须存在,并且最后修改时间是oldTS
     * 2.将content的内容写入到resPath路径对应的文件中
     * 3.返回最新的时间戳
     */
    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        File f = file(resPath);
        if ((f.exists() && f.lastModified() != oldTS) || (f.exists() == false && oldTS != 0))
            throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but found " + f.lastModified());

        //将content的内容写入到resPath路径对应的文件中
        putResourceImpl(resPath, new ByteArrayInputStream(content), newTS);

        // some FS lose precision on given time stamp
        return f.lastModified();
    }

    //删除路径对应的文件
    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        File f = file(resPath);
        f.delete();
    }

    //返回path对应的全路径
    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return file(resPath).toString();
    }

    private File file(String resPath) {
        if (resPath.equals("/"))
            return root;
        else
            return new File(root, resPath);
    }

}
