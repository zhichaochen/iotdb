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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.commons.file.SystemFileFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;

/**
 * 触发器类加载器
 */
public class TriggerClassLoader extends URLClassLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerClassLoader.class);

  // 触发器根目录，iotdb-server-0.13.0-SNAPSHOT/ext/trigger/， 开发人员需要将触发器打好的jar放在这个目录下面
  private final String libRoot;

  TriggerClassLoader(String libRoot) throws IOException {
    // 先添加一个空集合，然后将所有
    super(new URL[0]);
    this.libRoot = libRoot;
    LOGGER.info("Trigger lib root: {}", libRoot);
    // 添加URL到URL类加载器
    addURLs();
  }

  private void addURLs() throws IOException {

    // 触发器根目录下的所有文件
    HashSet<File> fileSet =
        new HashSet<>(FileUtils.listFiles(SystemFileFactory.INSTANCE.getFile(libRoot), null, true));
    // 将file转换成url
    URL[] urls = FileUtils.toURLs(fileSet.toArray(new File[0]));
    // 将其加入到触发器类加载器中
    for (URL url : urls) {
      super.addURL(url);
    }
  }
}
