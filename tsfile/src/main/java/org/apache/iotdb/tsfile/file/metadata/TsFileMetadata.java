/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * TsFile元数据
 * 该类现在叫做，IndexOfTimeseriesIndex，是TimeseriesIndex的索引，用来查找时间序列的。
 * TODO 是时间序列元数据的元数据，【时间序列元数据的索引】
 *
 * TSFileMetaData收集所有元数据信息并保存在其数据结构中。
 * TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadata {

  // bloom filter
  private BloomFilter bloomFilter; // 布隆过滤器，是用来过滤【物理量】的

  // List of <name, offset, childMetadataIndexType>
  private MetadataIndexNode metadataIndex; // 元数据索引节点

  // offset of MetaMarker.SEPARATOR
  private long metaOffset; // 元数据偏移量

  /**
   * 从缓存中反序列化TsFile的元数据
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    // 反序列化
    fileMetaData.metadataIndex = MetadataIndexNode.deserializeFrom(buffer);

    // metaOffset
    // 读取元数据的偏移量
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      // 读取
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      // 过滤器size，
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      // 使用几个hash函数
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      // 构建bloom过滤器
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
    }

    return fileMetaData;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   *
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    // metadataIndex
    if (metadataIndex != null) {
      byteLen += metadataIndex.serializeTo(outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeBloomFilter(OutputStream outputStream, Set<Path> paths) throws IOException {
    int byteLen = 0;
    BloomFilter filter = buildBloomFilter(paths);

    byte[] bytes = filter.serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    byteLen += bytes.length;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getSize(), outputStream);
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getHashFunctionSize(), outputStream);
    return byteLen;
  }

  /**
   * build bloom filter
   *
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(Set<Path> paths) {
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), paths.size());
    for (Path path : paths) {
      filter.add(path.toString());
    }
    return filter;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public MetadataIndexNode getMetadataIndex() {
    return metadataIndex;
  }

  public void setMetadataIndex(MetadataIndexNode metadataIndex) {
    this.metadataIndex = metadataIndex;
  }
}
