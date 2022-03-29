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

package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.v2.file.header.PageHeaderV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * chunk读取器
 */
public class ChunkReader implements IChunkReader {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkDataBuffer;
  private IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  protected Filter filter;
  private long currentTimestamp;

  private List<IPageReader> pageReaderList = new LinkedList<>();

  /**
   * 已经删除的时间区间
   * A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  /**
   * constructor of ChunkReader.
   *
   * @param chunk input Chunk object
   * @param filter filter
   */
  public ChunkReader(Chunk chunk, Filter filter) throws IOException {
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    this.currentTimestamp = Long.MIN_VALUE;
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    // TODO 初始化当前chunk的所有page
    if (chunk.isFromOldFile()) {
      initAllPageReadersV2();
    } else {
      initAllPageReaders(chunk.getChunkStatistic());
    }
  }

  /**
   * Constructor of ChunkReader by timestamp. This constructor is used to accelerate queries by
   * filtering out pages whose endTime is less than current timestamp.
   */
  public ChunkReader(Chunk chunk, Filter filter, long currentTimestamp) throws IOException {
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    this.currentTimestamp = currentTimestamp;
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    if (chunk.isFromOldFile()) {
      initAllPageReadersV2();
    } else {
      initAllPageReaders(chunk.getChunkStatistic());
    }
  }

  private void initAllPageReaders(Statistics chunkStatistic) throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      // if the current page satisfies
      // 当前page是否满足
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPage(pageHeader));
      } else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }

  /** judge if has next page whose page header satisfies the filter. */
  @Override
  public boolean hasNextSatisfiedPage() {
    return !pageReaderList.isEmpty();
  }

  /**
   * 下一个page
   * get next data batch.
   *
   * @return next data batch
   * @throws IOException IOException
   */
  @Override
  public BatchData nextPageData() throws IOException {
    if (pageReaderList.isEmpty()) {
      throw new IOException("No more page");
    }
    return pageReaderList.remove(0).getAllSatisfiedPageData();
  }

  private void skipBytesInStreamByLength(int length) {
    chunkDataBuffer.position(chunkDataBuffer.position() + length);
  }

  /**
   * 判断Page是否满足当前序列的过滤条件
   * 依赖与PageHeader信息
   * @param pageHeader
   * @return
   */
  protected boolean pageSatisfied(PageHeader pageHeader) {
    if (currentTimestamp > pageHeader.getEndTime()) {
      // used for chunk reader by timestamp
      return false;
    }
    if (deleteIntervalList != null) {
      // 遍历已删除的时间间隔列表
      for (TimeRange range : deleteIntervalList) {
        // 如果包含，说明已经删除了，则直接返回false
        if (range.contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {
          return false;
        }
        // 已删除的时间范围 和 当前page的时间范围是否重叠
        if (range.overlaps(new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()))) {
          // 表示当前page被修改过了
          pageHeader.setModified(true);
        }
      }
    }
    // 通过page的统计信息，判断下当前page是否满足
    return filter == null || filter.satisfy(pageHeader.getStatistics());
  }

  private PageReader constructPageReaderextPage(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.ForNgetEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }

    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    // 构建Page读取器
    PageReader reader =
        new PageReader(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }

  @Override
  public void close() {}

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }

  /**
   * 初始化chunk的所有page
   * @throws IOException
   */
  // For reading TsFile V2
  private void initAllPageReadersV2() throws IOException {
    // construct next satisfied page header
    // 轮询chunk数据缓存，只要有剩余，就继续
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      // TODO chunk是由多个page组成的
      // 反序列化一个page的page头
      PageHeader pageHeader =
          PageHeaderV2.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      // if the current page satisfies
      // 如果当前page满足，则加入page列表
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPageV2(pageHeader));
      }
      // 不满足，则跳过当前page数据，继续下一个page的解析
      else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }

  // For reading TsFile V2
  private PageReader constructPageReaderForNextPageV2(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    unCompressor.uncompress(
        compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    PageReader reader =
        new PageReaderV2(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }
}
