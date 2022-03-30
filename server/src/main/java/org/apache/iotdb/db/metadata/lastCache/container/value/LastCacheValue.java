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

package org.apache.iotdb.db.metadata.lastCache.container.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

/**
 * 最新的缓存值
 */
public class LastCacheValue implements ILastCacheValue {

  private long timestamp; // 时间戳

  private TsPrimitiveType value; // 数据值

  public LastCacheValue(long timestamp, TsPrimitiveType value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public void setValue(TsPrimitiveType value) {
    this.value = value;
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    return new TimeValuePair(timestamp, value);
  }
}
