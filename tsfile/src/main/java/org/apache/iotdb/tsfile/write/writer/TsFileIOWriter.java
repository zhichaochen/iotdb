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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * TODO TsFileIOWriter用于构造元数据，并将存储在内存中的数据写入输出流。
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter implements AutoCloseable {

  protected static final byte[] MAGIC_STRING_BYTES; // 魔数的字节数组
  public static final byte VERSION_NUMBER_BYTE; // 版本号的字节
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig(); // 配置信息
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    VERSION_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
  }

  protected TsFileOutput out; // 代表一个输出类型的TsFile的文件引用
  protected boolean canWrite = true; // 是否能写入
  protected File file;

  // current flushed Chunk
  private ChunkMetadata currentChunkMetadata; // 当前已刷盘的chunk
  // current flushed ChunkGroup
  // 当前已经刷盘的ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();

  private long markedPosition;
  private String currentChunkGroupDeviceId;

  // for upgrade tool and split tool
  // 设备和时间序列元数据的映射
  Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap;

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  // 这两个long标志着当前MemTable中操作的索引范围，并在MetaMarker之后序列化。恢复文件级别范围的操作索引范围
  private long minPlanIndex;
  private long maxPlanIndex;

  /** empty construct function. */
  protected TsFileIOWriter() {}

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), false);
    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    startFile();
  }

  /** for test only */
  public TsFileIOWriter(TsFileOutput output, boolean test) {
    this.out = output;
  }

  /**
   * 将字节数组写入输出流
   * 为啥是输出流呢，因为相对内存而言就是输出
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile() throws IOException {
    out.write(MAGIC_STRING_BYTES);
    out.write(VERSION_NUMBER_BYTE);
  }

  /**
   * 开始一个chunk group任务，由内存表任务进行驱动
   * TODO 主要是序列化了chunk头
   * @param deviceId
   * @throws IOException
   */
  public int startChunkGroup(String deviceId) throws IOException {
    this.currentChunkGroupDeviceId = deviceId;
    if (logger.isDebugEnabled()) {
      logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    }
    // 创建chunk元数据集合
    chunkMetadataList = new ArrayList<>();
    // 创建ChunkGroupHeader
    ChunkGroupHeader chunkGroupHeader = new ChunkGroupHeader(currentChunkGroupDeviceId);
    return chunkGroupHeader.serializeTo(out.wrapAsStream());
  }

  /**
   * 结束chunk group
   * 一个TsFile包含多个chunk group，所以这里会被调用多次
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup() throws IOException {
    if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }
    // 记录当前chunk group 的 chunk元数据列表
    chunkGroupMetadataList.add(
        new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList));
    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    // chunk group的元数据刷盘
    out.flush();
  }

  /**
   * For TsFileReWriteTool / UpgradeTool. Use this method to determine if needs to start a
   * ChunkGroup.
   *
   * @return isWritingChunkGroup
   */
  public boolean isWritingChunkGroup() {
    return currentChunkGroupDeviceId != null;
  }

  /**
   * 开始刷写chunk，记录chunk的元数据
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementId - measurementId of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @param mask - 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(
      String measurementId,
      CompressionType compressionCodecName,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics,
      int dataSize,
      int numOfPages,
      int mask)
      throws IOException {

    // 创建chunk元数据
    currentChunkMetadata =
        new ChunkMetadata(measurementId, tsDataType, out.getPosition(), statistics);
    // 设置掩码
    currentChunkMetadata.setMask((byte) mask);

    // chunk头
    ChunkHeader header =
        new ChunkHeader(
            measurementId,
            dataSize,
            tsDataType,
            compressionCodecName,
            encodingType,
            numOfPages,
            mask);
    // 序列化chunk头
    header.serializeTo(out.wrapAsStream());
  }

  /** Write a whole chunk in another file into this file. Providing fast merge for IoTDB. */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata =
        new ChunkMetadata(
            chunkHeader.getMeasurementID(),
            chunkHeader.getDataType(),
            out.getPosition(),
            chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "end flushing a chunk:{}, totalvalue:{}",
          chunkHeader.getMeasurementID(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /** end chunk and write some log. */
  public void endCurrentChunk() {
    chunkMetadataList.add(currentChunkMetadata);
    currentChunkMetadata = null;
  }

  /**
   * TODO 写TsFileMetadata到输出流并关闭它
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void endFile() throws IOException {
    // 记录元数据开始的位置
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    // 写入元数据的分隔符2
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    // group ChunkMetadata by series
    // 通过时间序列对ChunkMetadata进行分组
    // 一条时间序列，可能对应多个chunk中，这里构造 series 到 chunk元数据的映射
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = new TreeMap<>();

    // 遍历chunk group元数据列表
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      List<ChunkMetadata> chunkMetadatas = chunkGroupMetadata.getChunkMetadataList();
      // 遍历chunk group的所包含的chunk元数据列表
      for (IChunkMetadata chunkMetadata : chunkMetadatas) {
        // 创建时间序列路径
        Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());
        // 建立时间序列 和 chunkMetadata的映射
        chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
      }
    }

    // TODO 刷写元数据索引，这里会构造MetadataIndexNode树
    MetadataIndexNode metadataIndex = flushMetadataIndex(chunkMetadataListMap);
    // 构造TsFileMetadata
    TsFileMetadata tsFileMetaData = new TsFileMetadata();
    tsFileMetaData.setMetadataIndex(metadataIndex);
    tsFileMetaData.setMetaOffset(metaOffset);

    // 记录开始写元数据的位置
    long footerIndex = out.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write TsFileMetaData
    // TODO 写入FsFile元数据信息
    int size = tsFileMetaData.serializeTo(out.wrapAsStream());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());
    }

    // write bloom filter
    // 写bloom过滤器
    size += tsFileMetaData.serializeBloomFilter(out.wrapAsStream(), chunkMetadataListMap.keySet());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", out.getPosition());
    }

    // write TsFileMetaData size
    // 写TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream()); // write the size of the file metadata.

    // write magic string
    // 写魔数TsFile
    out.write(MAGIC_STRING_BYTES);

    // close file
    // 关闭文件
    out.close();
    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
    }
    canWrite = false;
  }

  /**
   * 刷写元数据索引
   * Flush TsFileMetadata, including ChunkMetadataList and TimeseriesMetaData
   *
   * @param chunkMetadataListMap chunkMetadata that Path.mask == 0
   * @return MetadataIndexEntry list in TsFileMetadata
   */
  private MetadataIndexNode flushMetadataIndex(Map<Path, List<IChunkMetadata>> chunkMetadataListMap)
      throws IOException {

    // convert ChunkMetadataList to this field
    deviceTimeseriesMetadataMap = new LinkedHashMap<>();
    // create device -> TimeseriesMetaDataList Map
    // 创建设备和TimeseriesMetaDataList 的映射
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      // for ordinary path
      // 序列化一个时间序列的chunk元数据列表
      flushOneChunkMetadata(entry.getKey(), entry.getValue());
    }

    // construct TsFileMetadata and return
    // TODO 构造 TsFileMetadata
    return MetadataIndexConstructor.constructMetadataIndex(deviceTimeseriesMetadataMap, out);
  }

  /**
   * 刷写一个时间序列的chunk元数据列表
   * Flush one chunkMetadata
   *
   * @param path Path of chunk
   * @param chunkMetadataList List of chunkMetadata about path(previous param)
   */
  private void flushOneChunkMetadata(Path path, List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    // create TimeseriesMetaData
    // 创建时间序列元数据
    PublicBAOS publicBAOS = new PublicBAOS();
    TSDataType dataType = chunkMetadataList.get(chunkMetadataList.size() - 1).getDataType();
    // 创建一个时间序列的统计信息
    Statistics seriesStatistics = Statistics.getStatsByType(dataType);

    int chunkMetadataListLength = 0;
    boolean serializeStatistic = (chunkMetadataList.size() > 1);
    // flush chunkMetadataList one by one
    // 遍历chunk列表，序列化多个chunk数据
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      chunkMetadataListLength += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);
      // 合并统计信息
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    // 创建一个时间序列元数据
    TimeseriesMetadata timeseriesMetadata =
        new TimeseriesMetadata(
            (byte)
                ((serializeStatistic ? (byte) 1 : (byte) 0) | chunkMetadataList.get(0).getMask()),
            chunkMetadataListLength,
            path.getMeasurement(),
            dataType,
            seriesStatistics,
            publicBAOS);
    // 将当前时间序列加入deviceTimeseriesMetadataMap
    deviceTimeseriesMetadataMap
        .computeIfAbsent(path.getDevice(), k -> new ArrayList<>())
        .add(timeseriesMetadata);
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return out.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      deviceChunkMetadataMap
          .computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[] {MetaMarker.SEPARATOR});
  }

  void writeChunkGroupMarkerForTest() throws IOException {
    out.write(new byte[] {MetaMarker.CHUNK_GROUP_HEADER});
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  /** Remove such ChunkMetadata that its startTime is not in chunkStartTimes */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetadata> chunkGroupMetaDataIterator = chunkGroupMetadataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetadata chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDevice();
      int chunkNum = chunkGroupMetaData.getChunkMetadataList().size();
      Iterator<ChunkMetadata> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetadataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        IChunkMetadata chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid =
            startTimeIdx < pathChunkStartTimes.size()
                && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }

  public void writePlanIndices() throws IOException {
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, out.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, out.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, out.wrapAsStream());
    out.flush();
  }

  public void truncate(long offset) throws IOException {
    out.truncate(offset);
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return out;
  }

  /**
   * this function is for Upgrade Tool and Split Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<String, List<TimeseriesMetadata>> getDeviceTimeseriesMetadataMap() {
    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }
}
