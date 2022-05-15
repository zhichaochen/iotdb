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

package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.tsfile.read.reader.IBatchReader;

/**
 * 托管序列的读取器
 * 提供额外的接口，使其能够在查询内的线程池中并发运行。
 * TODO  Managed 被管理的，在什么所管理呢，被线程池所管理
 * provides additional interfaces to make it able to be run in a thread pool concurrently within a
 * query.
 */
public interface ManagedSeriesReader extends IBatchReader {

  boolean isManagedByQueryManager();

  void setManagedByQueryManager(boolean managedByQueryManager);

  boolean hasRemaining();

  void setHasRemaining(boolean hasRemaining);
}
