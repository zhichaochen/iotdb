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
package org.apache.iotdb.tsfile.write.page;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * 页写入器
 * 此书写器用于将时间值写入页面。它由时间编码器、值编码器和各自的输出流组成。
 * This writer is used to write time-value into a page. It consists of a time encoder, a value
 * encoder and respective OutputStream.
 */
public class PageWriter {

  private static final Logger logger = LoggerFactory.getLogger(PageWriter.class);

  private ICompressor compressor; // 压缩器

  // time
  private Encoder timeEncoder; // 时间编码器
  private PublicBAOS timeOut; // 时间的字节输出数组
  // value
  private Encoder valueEncoder; // 值的编码器
  private PublicBAOS valueOut; // 值的字节输出数组

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<? extends Serializable> statistics; // 当前页的统计信息

  public PageWriter() {
    this(null, null);
  }

  public PageWriter(IMeasurementSchema measurementSchema) {
    this(measurementSchema.getTimeEncoder(), measurementSchema.getValueEncoder());
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
    this.compressor = ICompressor.getCompressor(measurementSchema.getCompressor());
  }

  private PageWriter(Encoder timeEncoder, Encoder valueEncoder) {
    this.timeOut = new PublicBAOS();
    this.valueOut = new PublicBAOS();
    this.timeEncoder = timeEncoder;
    this.valueEncoder = valueEncoder;
  }

  /**
   * 将时间和值写入编码器
   * write a time value pair into encoder */
  public void write(long time, boolean value) {
    // 对时间进行编码
    timeEncoder.encode(time, timeOut);
    // 对值进行编码
    valueEncoder.encode(value, valueOut);
    // 更新统计信息
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, short value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, int value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, long value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, float value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, double value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, Binary value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, int[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, long[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, float[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage() throws IOException {
    timeEncoder.flush(timeOut);
    valueEncoder.flush(valueOut);
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes() throws IOException {
    prepareEndWriteOnePage();
    ByteBuffer buffer = ByteBuffer.allocate(timeOut.size() + valueOut.size() + 4);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(timeOut.size(), buffer);
    buffer.put(timeOut.getBuf(), 0, timeOut.size());
    buffer.put(valueOut.getBuf(), 0, valueOut.size());
    buffer.flip();
    return buffer;
  }

  /**
   * 写入page的头和数据信息到PageWriter的输出流
   * write the page header and data into the PageWriter's output stream. */
  public int writePageHeaderAndDataIntoBuff(PublicBAOS pageBuffer, boolean first)
      throws IOException {
    if (statistics.getCount() == 0) {
      return 0;
    }

    // 未压缩的page数据
    ByteBuffer pageData = getUncompressedBytes();
    // 未压缩的字节大小
    int uncompressedSize = pageData.remaining();
    int compressedSize; // 压缩后的大小
    byte[] compressedBytes = null; // 压缩后的数据

    // 如果需要压缩，则进行压缩
    // 如果是不压缩类型
    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = uncompressedSize;
    }
    // gzip压缩类型
    else if (compressor.getType().equals(CompressionType.GZIP)) {
      compressedBytes =
          compressor.compress(pageData.array(), pageData.position(), uncompressedSize);
      compressedSize = compressedBytes.length;
    }
    // 其他压缩类型
    else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =
          compressor.compress(
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter
    // 写入page的头信息
    int sizeWithoutStatistic = 0;
    if (first) {
      // 第一个page
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
    } else {
      // 写入未压缩的大小
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      // 写入压缩的size
      ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
      // 序列化
      statistics.serialize(pageBuffer);
    }

    // write page content to temp PBAOS
    // 写入page内容
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else {
      pageBuffer.write(compressedBytes, 0, compressedSize);
    }
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    return sizeWithoutStatistic;
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value
   * outputStream, because size outputStream is never used until flushing.
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() {
    return timeOut.size()
        + valueOut.size()
        + timeEncoder.getMaxByteSize()
        + valueEncoder.getMaxByteSize();
  }

  /** reset this page */
  public void reset(IMeasurementSchema measurementSchema) {
    timeOut.reset();
    valueOut.reset();
    statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  public void setTimeEncoder(Encoder encoder) {
    this.timeEncoder = encoder;
  }

  public void setValueEncoder(Encoder encoder) {
    this.valueEncoder = encoder;
  }

  public void initStatistics(TSDataType dataType) {
    statistics = Statistics.getStatsByType(dataType);
  }

  public long getPointNumber() {
    return statistics.getCount();
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }
}
