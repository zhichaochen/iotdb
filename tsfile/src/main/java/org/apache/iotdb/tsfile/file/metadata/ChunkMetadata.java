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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * chunk元数据，其实是索引
 * Metadata of one chunk. */
public class ChunkMetadata implements IChunkMetadata {

  private String measurementUid; // 物理量ID

  /**
   * 文件通知中对应数据的字节偏移量：包括块头和标记。
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader; // chunk头的偏移量

  private TSDataType tsDataType; // 当前chunk的数据类型

  /**
   * version is used to define the order of operations(insertion, deletion, update). version is set
   * according to its belonging ChunkGroup only when being queried, so it is not persisted.
   */
  private long version; // 版本

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private boolean modified; //

  /** ChunkLoader of metadata, used to create ChunkReaderWrap */
  private IChunkLoader chunkLoader; // chunk加载器

  private Statistics<? extends Serializable> statistics;

  private boolean isFromOldTsFile = false;

  private long ramSize;

  private static final int CHUNK_METADATA_FIXED_RAM_SIZE = 93;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;
  private boolean isClosed;
  private String filePath;

  // 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
  private byte mask;

  // used for ChunkCache, Eg:"root.sg1/0/0"
  private String tsFilePrefixPath;
  // high 32 bit is compaction level, low 32 bit is merge count
  private long compactionVersion;

  public ChunkMetadata() {}

  /**
   * 构造chunk元数据
   * constructor of ChunkMetaData.
   *
   * @param measurementUid measurement id
   * @param tsDataType time series data type
   * @param fileOffset file offset
   * @param statistics value statistics
   */
  public ChunkMetadata(
      String measurementUid,
      TSDataType tsDataType,
      long fileOffset,
      Statistics<? extends Serializable> statistics) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return String.format(
        "measurementId: %s, datatype: %s, version: %d, Statistics: %s, deleteIntervalList: %s, filePath: %s",
        measurementUid, tsDataType, version, statistics, deleteIntervalList, filePath);
  }

  public long getNumOfPoints() {
    return statistics.getCount();
  }

  /**
   * get offset of chunk header.
   *
   * @return Byte offset of header of this chunk (includes the marker)
   */
  @Override
  public long getOffsetOfChunkHeader() {
    return offsetOfChunkHeader;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public TSDataType getDataType() {
    return tsDataType;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream, boolean serializeStatistic) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
    if (serializeStatistic) {
      byteLen += statistics.serialize(outputStream);
    }
    return byteLen;
  }

  /**
   * 反序列化
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer ： chunk索引的数据
   * @return ChunkMetaData object ： 时间序列元数据
   */
  public static ChunkMetadata deserializeFrom(
      ByteBuffer buffer, TimeseriesMetadata timeseriesMetadata) {
    // chunk索引
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    // 物理量ID
    chunkMetaData.measurementUid = timeseriesMetadata.getMeasurementId();
    // 数据类型
    chunkMetaData.tsDataType = timeseriesMetadata.getTSDataType();
    // chunk头的偏移量
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    // if the TimeSeriesMetadataType is not 0, it means it has more than one chunk
    // and each chunk's metadata has its own statistics
    // 如果TimeSeriesMetadataType不是0，意味着它有超过一个chunk，每个chunk metadata有他自己的统计信息
    if ((timeseriesMetadata.getTimeSeriesMetadataType() & 0x3F) != 0) {
      // 反序列化统计信息
      chunkMetaData.statistics = Statistics.deserialize(buffer, chunkMetaData.tsDataType);
    } else {
      // if the TimeSeriesMetadataType is 0, it means it has only one chunk
      // and that chunk's metadata has no statistic
      // 如果TimeSeriesMetadataType是0，表示只有一个chunk，那么chunk没有统计信息
      // 换句话说，chunk的统计信息等同于timeseries的统计信息，因为只有一个嘛。
      chunkMetaData.statistics = timeseriesMetadata.getStatistics();
    }
    return chunkMetaData;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public void setVersion(long version) {
    this.version = version;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public void insertIntoSortedDeletions(long startTime, long endTime) {
    List<TimeRange> resultInterval = new ArrayList<>();
    if (deleteIntervalList != null) {
      for (TimeRange interval : deleteIntervalList) {
        if (interval.getMax() < startTime) {
          resultInterval.add(interval);
        } else if (interval.getMin() > endTime) {
          resultInterval.add(new TimeRange(startTime, endTime));
          startTime = interval.getMin();
          endTime = interval.getMax();
        } else if (interval.getMax() >= startTime || interval.getMin() <= endTime) {
          startTime = Math.min(interval.getMin(), startTime);
          endTime = Math.max(interval.getMax(), endTime);
        }
      }
    }

    resultInterval.add(new TimeRange(startTime, endTime));
    deleteIntervalList = resultInterval;
  }

  public IChunkLoader getChunkLoader() {
    return chunkLoader;
  }

  @Override
  public boolean needSetChunkLoader() {
    return chunkLoader == null;
  }

  public void setChunkLoader(IChunkLoader chunkLoader) {
    this.chunkLoader = chunkLoader;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkMetadata that = (ChunkMetadata) o;
    return offsetOfChunkHeader == that.offsetOfChunkHeader
        && version == that.version
        && compactionVersion == that.compactionVersion
        && tsFilePrefixPath.equals(that.tsFilePrefixPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tsFilePrefixPath, version, compactionVersion, offsetOfChunkHeader);
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public boolean isFromOldTsFile() {
    return isFromOldTsFile;
  }

  public void setFromOldTsFile(boolean isFromOldTsFile) {
    this.isFromOldTsFile = isFromOldTsFile;
  }

  public long calculateRamSize() {
    return CHUNK_METADATA_FIXED_RAM_SIZE
        + RamUsageEstimator.sizeOf(tsFilePrefixPath)
        + RamUsageEstimator.sizeOf(measurementUid)
        + statistics.calculateRamSize();
  }

  public static long calculateRamSize(String measurementId, TSDataType dataType) {
    return CHUNK_METADATA_FIXED_RAM_SIZE
        + RamUsageEstimator.sizeOf(measurementId)
        + Statistics.getSizeByType(dataType);
  }

  public void mergeChunkMetadata(ChunkMetadata chunkMetadata) {
    Statistics<? extends Serializable> statistics = chunkMetadata.getStatistics();
    this.statistics.mergeStatistics(statistics);
    this.ramSize = calculateRamSize();
  }

  @Override
  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  @Override
  public boolean isSeq() {
    return isSeq;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void setClosed(boolean closed) {
    isClosed = closed;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;

    Pair<String, long[]> tsFilePrefixPathAndTsFileVersionPair =
        FilePathUtils.getTsFilePrefixPathAndTsFileVersionPair(filePath);
    // set tsFilePrefixPath
    tsFilePrefixPath = tsFilePrefixPathAndTsFileVersionPair.left;
    this.version = tsFilePrefixPathAndTsFileVersionPair.right[0];
    this.compactionVersion = tsFilePrefixPathAndTsFileVersionPair.right[1];
  }

  @Override
  public byte getMask() {
    return mask;
  }

  public void setMask(byte mask) {
    this.mask = mask;
  }
}
