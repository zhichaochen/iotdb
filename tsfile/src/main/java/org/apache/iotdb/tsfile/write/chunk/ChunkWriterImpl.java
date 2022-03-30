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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * 数据块写入器
 * 数据块：包含一条时间序列上的多个page，是数据块被IO读取的最小单元
 * TODO 将数据写入磁盘的操作肯定在当前类中进行
 */
public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger logger = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private final IMeasurementSchema measurementSchema;

  private final ICompressor compressor;

  /** all pages of this chunk. */
  private final PublicBAOS pageBuffer; // page缓存

  private int numOfPages; // 当前chunk块中page的数量

  /** write data into current page */
  private PageWriter pageWriter;

  /** page size threshold. */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck; // 在一个page中值的总数

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private Statistics<? extends Serializable> statistics;

  /** SDT parameters */
  private boolean isSdtEncoding;
  // When the ChunkWriter WILL write the last data point in the chunk, set it to true to tell SDT
  // saves the point.
  private boolean isLastPoint;
  // do not re-execute SDT compression when merging chunks
  private boolean isMerging;
  private SDTEncoder sdtEncoder;

  private static final String LOSS = "loss";
  private static final String SDT = "sdt";
  private static final String SDT_COMP_DEV = "compdev";
  private static final String SDT_COMP_MIN_TIME = "compmintime";
  private static final String SDT_COMP_MAX_TIME = "compmaxtime";

  /** first page info */
  private int sizeWithoutStatistic; // 第一个page的大小，不包含统计信息

  private Statistics<?> firstPageStatistics; // 第一个page的统计信息

  /** @param schema schema of this measurement */
  public ChunkWriterImpl(IMeasurementSchema schema) {
    this.measurementSchema = schema; //
    // 获取压缩器
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    //
    this.pageBuffer = new PublicBAOS();

    // 每页数据的大小，默认64kb
    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    // 每页最大的数据点，默认1024 * 1024. 1M
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter(measurementSchema);

    // 当前物理量的时间编码器
    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    // 当前物理量的值编码器
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());

    // check if the measurement schema uses SDT
    checkSdtEncoding();
  }

  public ChunkWriterImpl(IMeasurementSchema schema, boolean isMerging) {
    this(schema);
    this.isMerging = isMerging;
  }

  private void checkSdtEncoding() {
    if (measurementSchema.getProps() != null && !isMerging) {
      if (measurementSchema.getProps().getOrDefault(LOSS, "").equals(SDT)) {
        isSdtEncoding = true;
        sdtEncoder = new SDTEncoder();
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_DEV)) {
        sdtEncoder.setCompDeviation(
            Double.parseDouble(measurementSchema.getProps().get(SDT_COMP_DEV)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MIN_TIME)) {
        sdtEncoder.setCompMinTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MIN_TIME)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MAX_TIME)) {
        sdtEncoder.setCompMaxTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MAX_TIME)));
      }
    }
  }

  /**
   * 写入时间戳和值
   * @param time
   * @param value
   */
  public void write(long time, long value) {
    // store last point for sdtEncoding, it still needs to go through encoding process
    // in case it exceeds compdev and needs to store second last point
    if (!isSdtEncoding || sdtEncoder.encodeLong(time, value)) {
      // 调用page写入器写入
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getLongValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    // 检查当前页的大小，可能需要打开一个新的页
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, int value) {
    if (!isSdtEncoding || sdtEncoder.encodeInt(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getIntValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * 写入一个boolean值
   * @param time
   * @param value
   */
  public void write(long time, boolean value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, float value) {
    if (!isSdtEncoding || sdtEncoder.encodeFloat(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getFloatValue() : value);
    }
    // store last point for sdt encoding
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, double value) {
    if (!isSdtEncoding || sdtEncoder.encodeDouble(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getDoubleValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, Binary value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, int[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, long[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, float[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, double[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * 检查当前页的大小，可能需要打开一个新的页
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private void checkPageSizeAndMayOpenANewPage() {
    // 如果达到最大数据点数量，则将字节数组中的Page写入
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
      logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      // 通过上面的日志提示，表示要写入磁盘
      writePageToPageBuffer();
    }
    //
    else if (pageWriter.getPointNumber()
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      // 当前page大小
      long currentPageSize = pageWriter.estimateMaxMemSize();
      // 如果当前page的大小超过page的阈值，则写入page到磁盘
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        logger.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(),
            pageSizeThreshold,
            currentPageSize,
            pageWriter.getPointNumber());
        writePageToPageBuffer();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck =
            (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
      }
    }
  }

  /**
   * 写page到page缓存
   * 将page写入到缓存中，待刷盘，存储引擎会通过调用writeToFileWriter()，将数据刷写到磁盘
   */
  private void writePageToPageBuffer() {
    try {
      // 如果page的数量为0，说明当前是第一个page
      if (numOfPages == 0) { // record the firstPageStatistics
        // 第一个page的统计信息
        this.firstPageStatistics = pageWriter.getStatistics();
        // 写入page头和数据到buffer中
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);
      }
      //
      else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        // page数组
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
        firstPageStatistics = null;
      } else {
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
      }

      // update statistics of this chunk
      // 更新chunk的统计信息
      numOfPages++;
      // 合并 chunk 的统计信息
      this.statistics.mergeStatistics(pageWriter.getStatistics());
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset(measurementSchema);
    }
  }

  /**
   * TODO 这里将TsFile刷写到磁盘
   * @param tsfileWriter
   * @throws IOException
   */
  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    // 密封当前page，将当前page写入缓存
    sealCurrentPage();
    // 将当前chunk的所有page写入TsFile，只是写入，并没有flush。
    writeAllPagesOfChunkToTsFile(tsfileWriter, statistics);

    // reinit this chunk writer
    // 重新初始化这个chunk write
    pageBuffer.reset();
    numOfPages = 0;
    firstPageStatistics = null;
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  @Override
  public long getSerializedChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId(), pageBuffer.size())
        + (long) pageBuffer.size();
  }

  /**
   * 密封当前页
   */
  @Override
  public void sealCurrentPage() {
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {
      writePageToPageBuffer();
    }
  }

  @Override
  public void clearPageWriter() {
    pageWriter = null;
  }

  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  /**
   * write the page header and data into the PageWriter's output stream. @NOTE: for upgrading
   * 0.11/v2 to 0.12/v3 TsFile
   */
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    // write the page header to pageBuffer
    try {
      logger.debug(
          "start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      // serialize pageHeader  see writePageToPageBuffer method
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = header.getStatistics();
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
        header.getStatistics().serialize(pageBuffer);
        firstPageStatistics = null;
      } else {
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
        header.getStatistics().serialize(pageBuffer);
      }
      logger.debug(
          "finish to flush a page header {} of {} into buffer, buffer position {} ",
          header,
          measurementSchema.getMeasurementId(),
          pageBuffer.size());

      statistics.mergeStatistics(header.getStatistics());

    } catch (IOException e) {
      throw new PageException("IO Exception in writeDataPageHeader,ignore this page", e);
    }
    numOfPages++;
    // write page content to temp PBAOS
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      channel.write(data);
    } catch (IOException e) {
      throw new PageException(e);
    }
  }

  /**
   * 将当前chunk的所有page写入TsFile
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @param statistics the chunk statistics
   * @throws IOException exception in IO
   */
  private void writeAllPagesOfChunkToTsFile(
      TsFileIOWriter writer, Statistics<? extends Serializable> statistics) throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    // 开始去写数据块，写入了chunk的一些元数据
    writer.startFlushChunk(
        measurementSchema.getMeasurementId(),
        compressor.getType(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        statistics,
        pageBuffer.size(),
        numOfPages,
        0);

    // 数据偏移量
    long dataOffset = writer.getPos();

    // write all pages of this column
    // 将这列所有的page写入输出流
    writer.writeBytesToStream(pageBuffer);

    // 数据的size
    int dataSize = (int) (writer.getPos() - dataOffset);
    // 校验
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size());
    }

    // 结束当前chunk
    writer.endCurrentChunk();
  }

  public void setIsMerging(boolean isMerging) {
    this.isMerging = isMerging;
  }

  public boolean isMerging() {
    return isMerging;
  }

  public void setLastPoint(boolean isLastPoint) {
    this.isLastPoint = isLastPoint;
  }
}
