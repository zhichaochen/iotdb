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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 时间序列索引元数据
 */
public class TimeseriesMetadata implements ITimeSeriesMetadata {

  /** used for old version tsfile */
  private long startOffsetOfChunkMetaDataList;
  /**
   * 0表示此时间序列只有一个区块，无需在区块元数据中再次保存统计信息；
   * 0 means this time series has only one chunk, no need to save the statistic again in chunk
   * metadata;
   *
   * 1表示此时间序列有多个区块，应在区块中再次保存统计数据元数据；
   * <p>1 means this time series has more than one chunk, should save the statistic again in chunk
   * metadata;
   *
   * <p>if the 8th bit is 1, it means it is the time column of a vector series;
   *
   * <p>if the 7th bit is 1, it means it is the value column of a vector series
   */
  private byte timeSeriesMetadataType;

  private int chunkMetaDataListDataSize;

  private String measurementId;
  private TSDataType dataType;

  private Statistics<? extends Serializable> statistics;

  // modified is true when there are modifications of the series, or from unseq file
  private boolean modified;

  private IChunkMetadataLoader chunkMetadataLoader;

  private long ramSize;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;

  // used to save chunk metadata list while serializing
  private PublicBAOS chunkMetadataListBuffer;

  private ArrayList<IChunkMetadata> chunkMetadataList; // chunk索引列表

  public TimeseriesMetadata() {}

  public TimeseriesMetadata(
      byte timeSeriesMetadataType,
      int chunkMetaDataListDataSize,
      String measurementId,
      TSDataType dataType,
      Statistics<? extends Serializable> statistics,
      PublicBAOS chunkMetadataListBuffer) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
    this.chunkMetaDataListDataSize = chunkMetaDataListDataSize;
    this.measurementId = measurementId;
    this.dataType = dataType;
    this.statistics = statistics;
    this.chunkMetadataListBuffer = chunkMetadataListBuffer;
  }

  public TimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) {
    this.timeSeriesMetadataType = timeseriesMetadata.timeSeriesMetadataType;
    this.chunkMetaDataListDataSize = timeseriesMetadata.chunkMetaDataListDataSize;
    this.measurementId = timeseriesMetadata.measurementId;
    this.dataType = timeseriesMetadata.dataType;
    this.statistics = timeseriesMetadata.statistics;
    this.modified = timeseriesMetadata.modified;
    this.chunkMetadataList = new ArrayList<>(timeseriesMetadata.chunkMetadataList);
  }

  /**
   * 反序列化
   */
  public static TimeseriesMetadata deserializeFrom(ByteBuffer buffer, boolean needChunkMetadata) {
    // 创建时间序列对象
    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    // 读取一个字节，该索引的索引类型
    timeseriesMetaData.setTimeSeriesMetadataType(ReadWriteIOUtils.readByte(buffer));
    // 读取四个字节，物理量ID
    timeseriesMetaData.setMeasurementId(ReadWriteIOUtils.readVarIntString(buffer));
    // 读取一个字节，数据类型
    timeseriesMetaData.setTSDataType(ReadWriteIOUtils.readDataType(buffer));
    // 读取一个int值，chunk索引的列表的数据长度
    int chunkMetaDataListDataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(chunkMetaDataListDataSize);
    timeseriesMetaData.setStatistics(Statistics.deserialize(buffer, timeseriesMetaData.dataType));
    // 是否需要读取chunk的索引信息
    if (needChunkMetadata) {
      // 从原先的大的bytebuffer中分出一个子ByteBuffer，那么这个bytebuffer中保存的全部是chunk的索引数据
      ByteBuffer byteBuffer = buffer.slice();
      // 设置limit值
      byteBuffer.limit(chunkMetaDataListDataSize);
      // chunk索引列表
      timeseriesMetaData.chunkMetadataList = new ArrayList<>();
      // 如果有生
      while (byteBuffer.hasRemaining()) {
        // 添加chunk索引数据到timeseriesMetaData
        timeseriesMetaData.chunkMetadataList.add(
            ChunkMetadata.deserializeFrom(byteBuffer, timeseriesMetaData));
      }
      // minimize the storage of an ArrayList instance.
      // 删除动态增长的多余容量，减少内存占用
      timeseriesMetaData.chunkMetadataList.trimToSize();
    }
    buffer.position(buffer.position() + chunkMetaDataListDataSize);
    return timeseriesMetaData;
  }

  /**
   * 序列化时间序列元数据
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return byte length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    // 字节长度
    int byteLen = 0;
    //
    byteLen += ReadWriteIOUtils.write(timeSeriesMetadataType, outputStream);
    byteLen += ReadWriteIOUtils.writeVar(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(dataType, outputStream);
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(chunkMetaDataListDataSize, outputStream);
    byteLen += statistics.serialize(outputStream);
    chunkMetadataListBuffer.writeTo(outputStream);
    byteLen += chunkMetadataListBuffer.size();
    return byteLen;
  }

  public byte getTimeSeriesMetadataType() {
    return timeSeriesMetadataType;
  }

  public void setTimeSeriesMetadataType(byte timeSeriesMetadataType) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
  }

  public long getOffsetOfChunkMetaDataList() {
    return startOffsetOfChunkMetaDataList;
  }

  public void setOffsetOfChunkMetaDataList(long position) {
    this.startOffsetOfChunkMetaDataList = position;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public int getDataSizeOfChunkMetaDataList() {
    return chunkMetaDataListDataSize;
  }

  public void setDataSizeOfChunkMetaDataList(int size) {
    this.chunkMetaDataListDataSize = size;
  }

  public TSDataType getTSDataType() {
    return dataType;
  }

  public void setTSDataType(TSDataType tsDataType) {
    this.dataType = tsDataType;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.statistics = statistics;
  }

  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    this.chunkMetadataLoader = chunkMetadataLoader;
  }

  public IChunkMetadataLoader getChunkMetadataLoader() {
    return chunkMetadataLoader;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList() throws IOException {
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public void setModified(boolean modified) {
    this.modified = modified;
  }

  @Override
  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  @Override
  public boolean isSeq() {
    return isSeq;
  }

  // For Test Only
  public void setChunkMetadataListBuffer(PublicBAOS chunkMetadataListBuffer) {
    this.chunkMetadataListBuffer = chunkMetadataListBuffer;
  }

  // For reading version-2 only
  public void setChunkMetadataList(ArrayList<ChunkMetadata> chunkMetadataList) {
    this.chunkMetadataList = new ArrayList<>(chunkMetadataList);
  }

  @Override
  public String toString() {
    return "TimeseriesMetadata{"
        + "startOffsetOfChunkMetaDataList="
        + startOffsetOfChunkMetaDataList
        + ", timeSeriesMetadataType="
        + timeSeriesMetadataType
        + ", chunkMetaDataListDataSize="
        + chunkMetaDataListDataSize
        + ", measurementId='"
        + measurementId
        + '\''
        + ", dataType="
        + dataType
        + ", statistics="
        + statistics
        + ", modified="
        + modified
        + ", isSeq="
        + isSeq
        + ", chunkMetadataList="
        + chunkMetadataList
        + '}';
  }
}
