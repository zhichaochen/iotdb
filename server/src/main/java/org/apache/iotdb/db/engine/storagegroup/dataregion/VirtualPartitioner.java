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
package org.apache.iotdb.db.engine.storagegroup.dataregion;

import org.apache.iotdb.commons.path.PartialPath;

/**
 * 虚拟存储组分区器
 * 在存储组下面一层，还有虚拟存储组，会通过该分区器进行hash分区，得到虚拟存储组
 */
public interface VirtualPartitioner {

  /**
   * 用设备ID去决定存储组ID
   * use device id to determine storage group id
   *
   * @param deviceId device id
   * @return data region id
   */
  int deviceToDataRegionId(PartialPath deviceId);

  /**
   * 获取分区总数
   * get total number of data region
   *
   * @return total number of data region
   */
  int getPartitionCount();
}
