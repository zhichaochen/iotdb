/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.file;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import java.io.File;
import java.net.URI;

/**
 * 系统文件工厂, 用于通过路径获取文件
 */
public enum SystemFileFactory {
  INSTANCE;

  private static FSType fsType =
      CommonDescriptor.getInstance().getConfig().getSystemFileStorageFs();
  private static final String UNSUPPORT_FILE_SYSTEM = "Unsupported file system: ";

  /**
   * 通过路径创建一个文件
   * 比如触发器根目录对应的File
   * @param pathname
   * @return
   */
  public File getFile(String pathname) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(uri);
    } else {
      return new File(uri);
    }
  }
}
