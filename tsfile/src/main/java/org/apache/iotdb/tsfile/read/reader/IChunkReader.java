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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.List;

/**
 * chunk读取器
 * 1、接受一个chunk，对chunk的数据结构进行解析
 * 2、主要对chunk中的page进行处理，比如：迭代chunk中的page
 */
public interface IChunkReader {

  /**
   * 是否有下一个满足条件的page
   * @return
   * @throws IOException
   */
  boolean hasNextSatisfiedPage() throws IOException;

  /**
   * 下一个page
   * @return
   * @throws IOException
   */
  BatchData nextPageData() throws IOException;

  /**
   * 关闭chunk读取器
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * 获取多个page对应的page读取器列表
   * @return
   * @throws IOException
   */
  List<IPageReader> loadPageReaderList() throws IOException;
}
