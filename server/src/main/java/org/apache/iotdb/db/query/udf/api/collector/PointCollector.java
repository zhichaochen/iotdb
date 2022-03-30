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

package org.apache.iotdb.db.query.udf.api.collector;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

/**
 * 用于收集时间序列数据点
 * Used to collect time series data points generated by {@link UDTF#transform(Row, PointCollector)},
 * {@link UDTF#transform(RowWindow, PointCollector)} or {@link UDTF#terminate(PointCollector)}.
 */
public interface PointCollector {

  /**
   * Collects an int data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.INT32} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value int value to collect
   * @throws IOException if any I/O errors occur
   * @see TSDataType
   */
  void putInt(long timestamp, int value) throws IOException;

  /**
   * Collects a long data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.INT64} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value long value to collect
   * @throws IOException if any I/O errors occur
   * @see TSDataType
   */
  void putLong(long timestamp, long value) throws IOException;

  /**
   * Collects a float data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.FLOAT} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value float value to collect
   * @throws IOException if any I/O errors occur
   * @see TSDataType
   */
  void putFloat(long timestamp, float value) throws IOException;

  /**
   * Collects a double data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.DOUBLE} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)}
   * in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value double value to collect
   * @throws IOException if any I/O errors occur
   * @see TSDataType
   */
  void putDouble(long timestamp, double value) throws IOException;

  /**
   * Collects a boolean data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.BOOLEAN} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)}
   * in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value boolean value to collect
   * @throws IOException if any I/O errors occur
   * @see TSDataType
   */
  void putBoolean(long timestamp, boolean value) throws IOException;

  /**
   * Collects a Binary data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.TEXT} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value Binary value to collect
   * @throws IOException if any I/O errors occur
   * @throws QueryProcessException if memory is not enough to continue collecting data points
   * @see TSDataType
   */
  void putBinary(long timestamp, Binary value) throws IOException, QueryProcessException;

  /**
   * Collects a String data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.TEXT} by calling {@link UDTFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value String value to collect
   * @throws IOException if any I/O errors occur
   * @throws QueryProcessException if memory is not enough to continue collecting data points
   * @see TSDataType
   */
  void putString(long timestamp, String value) throws IOException, QueryProcessException;
}
