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

package org.apache.iotdb.db.engine.trigger.api;

import org.apache.iotdb.tsfile.utils.Binary;

/**
 * 触发器接口，自定义的触发器都需要实现该接口
 * 其中：fire ：侦听数据变动的钩子，目前仅仅能侦听插入的操作，其中入参入参timestamp和value即是这一次插入数据点的时间和数据值。
 *
 */
/** User Guide: docs/UserGuide/Operation Manual/Triggers.md */
public interface Trigger {

  /*当创建触发器的时候做些什么事情*/
  @SuppressWarnings("squid:S112")
  default void onCreate(TriggerAttributes attributes) throws Exception {}

  /*当删除触发器的时候做些什么*/
  @SuppressWarnings("squid:S112")
  default void onDrop() throws Exception {}

  /*手动启动触发器后，该方法会被调用*/
  @SuppressWarnings("squid:S112")
  default void onStart() throws Exception {}

  /*当手动停止触发器后，该方法会被调用*/
  @SuppressWarnings("squid:S112")
  default void onStop() throws Exception {}

  @SuppressWarnings("squid:S112")
  default Integer fire(long timestamp, Integer value) throws Exception {
    return value;
  }

  default int[] fire(long[] timestamps, int[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Long fire(long timestamp, Long value) throws Exception {
    return value;
  }

  default long[] fire(long[] timestamps, long[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Float fire(long timestamp, Float value) throws Exception {
    return value;
  }

  default float[] fire(long[] timestamps, float[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Double fire(long timestamp, Double value) throws Exception {
    return value;
  }

  default double[] fire(long[] timestamps, double[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Boolean fire(long timestamp, Boolean value) throws Exception {
    return value;
  }

  default boolean[] fire(long[] timestamps, boolean[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Binary fire(long timestamp, Binary value) throws Exception {
    return value;
  }

  default Binary[] fire(long[] timestamps, Binary[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }
}
