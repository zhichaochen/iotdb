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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;

/**
 * 叶子节点
 */
public class LeafNode implements Node {

  // TsFileSequenceReader
  private IBatchReader reader; // 批量读取器，比如：FileSeriesReader

  private BatchData cacheData; // 缓存数据
  private boolean hasCached; // 是否有已经缓存的数据

  private long cachedTime;
  private Object cachedValue;

  public LeafNode(IBatchReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {
    // 有缓存则返回true
    if (hasCached) {
      return true;
    }
    // 如果缓存数据当前有数据，也返回true
    if (cacheData != null && cacheData.hasCurrent()) {
      cachedTime = cacheData.currentTime();
      cachedValue = cacheData.currentValue();
      hasCached = true;
      return true;
    }
    // 走到这里就需要从reader中读取数据了
    while (reader.hasNextBatch()) {
      // 使用reader读取一批数据
      cacheData = reader.nextBatch();
      if (cacheData.hasCurrent()) {
        cachedTime = cacheData.currentTime();
        cachedValue = cacheData.currentValue();
        hasCached = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public long next() throws IOException {
    if ((hasCached || hasNext())) {
      hasCached = false;
      cacheData.next();
      return cachedTime;
    }
    throw new IOException("no more data");
  }

  /**
   * Check whether the current time equals the given time.
   *
   * @param time the given time
   * @return True if the current time equals the given time. False if not.
   */
  public boolean currentTimeIs(long time) {
    return cachedTime == time;
  }

  /** Function for getting the value at the given time. */
  public Object currentValue() {
    return cachedValue;
  }

  @Override
  public NodeType getType() {
    return NodeType.LEAF;
  }
}
