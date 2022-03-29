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
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件时间序列读取器
 * TODO 和TsFileSequenceReader有什么区别？？？
 * FileSeriesReader：专注于时间序列，专注于chunk的查询，一个时间序列的查询本质来说就是对Chunk的查询
 * TsFileSequenceReader： 专注于整个TsFile的读取，对整个TsFile的各个部分的读取都提供了对应的方法
 *
 * 时间序列读取器，被用于查询一个tsFile的一条时间序列，
 * Series reader is used to query one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */
public class FileSeriesReader extends AbstractFileSeriesReader {

  public FileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(chunkLoader, chunkMetadataList, filter);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {
    if (chunkMetaData instanceof ChunkMetadata) {
      // 加载chunk
      Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
      // 更新chunk读取器
      this.chunkReader = new ChunkReader(chunk, filter);
    }
    // 对齐的chunk数据
    else {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetaData;
      Chunk timeChunk =
          chunkLoader.loadChunk((ChunkMetadata) (alignedChunkMetadata.getTimeChunkMetadata()));
      List<Chunk> valueChunkList = new ArrayList<>();
      for (IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        valueChunkList.add(chunkLoader.loadChunk((ChunkMetadata) metadata));
      }
      this.chunkReader = new AlignedChunkReader(timeChunk, valueChunkList, filter);
    }
  }

  /**
   * 当前chunk是否满足过滤条件
   * @param chunkMetaData
   * @return
   */
  @Override
  protected boolean chunkSatisfied(IChunkMetadata chunkMetaData) {
    // 通过chunkMetaData.getStatistics使用过滤器的条件进行过滤
    return filter == null || filter.satisfy(chunkMetaData.getStatistics());
  }
}
