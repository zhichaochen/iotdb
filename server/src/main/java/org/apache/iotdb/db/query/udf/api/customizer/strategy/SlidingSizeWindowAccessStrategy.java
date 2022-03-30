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

package org.apache.iotdb.db.query.udf.api.customizer.strategy;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * When the access strategy of a UDTF is set to an instance of this class, the method {@link
 * UDTF#transform(RowWindow, PointCollector)} of the UDTF will be called to transform the original
 * data. You need to override the method in your own UDTF class.
 * <p>
 * Sliding size window is a kind of size-based window. Except for the last call, each call of the
 * method {@link UDTF#transform(RowWindow, PointCollector)} processes a window with {@code
 * windowSize} rows (aligned by time) of the original data and can generate any number of data
 * points.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT32)
 *       .setAccessStrategy(new SlidingSizeWindowAccessStrategy(10000)); // window size
 * }</pre>
 *
 * @see UDTF
 * @see UDTFConfigurations
 *
 * 滑动尺寸的窗口访问策略
 *
 * 以固定行数的方式处理原始数据，即每个数据处理窗口都会包含固定行数的数据（最后一个窗口除外）。
 * 框架会为每一个原始数据输入窗口调用一次transform方法。一个窗口可能存在多行数据，每一行数据对应的是输
 * 入序列按时间对齐后的结果（一行数据中，可能存在某一列为null值，但不会全部都是null）。
 */
public class SlidingSizeWindowAccessStrategy implements AccessStrategy {

  private final int windowSize; // 窗口大小，类似于行数据的范围
  private final int slidingStep; // 滑动步长，也就是处理几行数据

  /**
   * Constructor. You need to specify the number of rows in each sliding size window (except for the
   * last window) and the sliding step to the next window.
   *
   * <p>Example
   *
   * <p>Original data points (time, s1, s2):
   *
   * <ul>
   *   <li>(1, 100, null )
   *   <li>(2, 100, null )
   *   <li>(3, 100, null )
   *   <li>(4, 100, 'error')
   *   <li>(5, 100, null )
   *   <li>(6, 101, 'error')
   *   <li>(7, 102, 'error')
   * </ul>
   *
   * Set windowSize to 2 and set slidingStep to 3, windows will be generated as below: Window 0:
   * [(1, 100, null ), (2, 100, null)] Window 1: [(4, 100, 'error'), (5, 100, null)] Window 2: [(7,
   * 102, 'error')]
   *
   * @param windowSize the number of rows in each sliding size window (0 < windowSize)
   * @param slidingStep the number of rows between the first point of the next window and the first
   *     point of the current window (0 < slidingStep)
   */
  public SlidingSizeWindowAccessStrategy(int windowSize, int slidingStep) {
    this.windowSize = windowSize;
    this.slidingStep = slidingStep;
  }

  /**
   * Constructor. You need to specify the number of rows in each sliding size window (except for the
   * last window). The sliding step will be set to the same as the window size.
   *
   * @param windowSize the number of rows in each sliding size window (0 < windowSize)
   */
  public SlidingSizeWindowAccessStrategy(int windowSize) {
    this.windowSize = windowSize;
    this.slidingStep = windowSize;
  }

  @Override
  public void check() throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException(
          String.format("Parameter windowSize(%d) should be positive.", windowSize));
    }
    if (slidingStep <= 0) {
      throw new QueryProcessException(
          String.format("Parameter slidingStep(%d) should be positive.", slidingStep));
    }
  }

  public int getWindowSize() {
    return windowSize;
  }

  public int getSlidingStep() {
    return slidingStep;
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.SLIDING_SIZE_WINDOW;
  }
}
