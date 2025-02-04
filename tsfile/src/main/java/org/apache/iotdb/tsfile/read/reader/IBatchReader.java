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

/**
 * 批量读取器
 * 本质来说是读取一个page的数据，一个page就是一批数据
 * Page是数据读取的最小单位
 */
public interface IBatchReader {

  /**
   * 是否有下一批次
   * @return
   * @throws IOException
   */
  boolean hasNextBatch() throws IOException;

  /**
   * 下一批次
   * @return
   * @throws IOException
   */
  BatchData nextBatch() throws IOException;

  /**
   * 关闭读取器
   * @throws IOException
   */
  void close() throws IOException;
}
