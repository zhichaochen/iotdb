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

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;

import java.io.IOException;
import java.util.List;

/**
 * 文件的时间序列读取器
 * 被用于查询一个tsfile中的一个时间序列
 * Series reader is used to query one series of one tsfile. */
public abstract class AbstractFileSeriesReader implements IBatchReader {

  protected IChunkLoader chunkLoader; // chunk加载器
  protected List<IChunkMetadata> chunkMetadataList; // chunk元数据列表
  protected IChunkReader chunkReader; // chunk读取器
  private int chunkToRead; // 从第几个chunk开始读

  protected Filter filter; // 当前序列的过滤条件

  /** constructor of FileSeriesReader. */
  public AbstractFileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    this.chunkLoader = chunkLoader;
    this.chunkMetadataList = chunkMetadataList;
    this.filter = filter;
    this.chunkToRead = 0;
  }

  /**
   * 是否有下一批次
   * 在这个过程中，会通过chunk元数据，加载chunk，读取page
   * @return
   * @throws IOException
   */
  @Override
  public boolean hasNextBatch() throws IOException {

    // current chunk has additional batch
    // 当前chunk有额外的page没有读取，直接返回true
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
      return true;
    }

    // current chunk does not have additional batch, init new chunk reader
    // 没有额外的批次，需要初始化chunk读取器
    while (chunkToRead < chunkMetadataList.size()) {

      // 下一个chunk的元数据
      IChunkMetadata chunkMetaData = nextChunkMeta();
      // chunk是否满足序列的过滤条件
      if (chunkSatisfied(chunkMetaData)) {
        // chunk metadata satisfy the condition
        // 初始化chunk读取器，其实就是通过chunk元数据，加载chunk数据，将其放入chunk reader中
        initChunkReader(chunkMetaData);

        // 是否有满足过滤条件的下一个Page
        if (chunkReader.hasNextSatisfiedPage()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * 下一批次，读取下一个page的数据
   * @return
   * @throws IOException
   */
  @Override
  public BatchData nextBatch() throws IOException {
    // 下一个page数据
    return chunkReader.nextPageData();
  }

  protected abstract void initChunkReader(IChunkMetadata chunkMetaData) throws IOException;

  /*当前chunk是否满足时间序列的过滤条件*/
  protected abstract boolean chunkSatisfied(IChunkMetadata chunkMetaData);

  @Override
  public void close() throws IOException {
    chunkLoader.close();
  }

  /**
   * 下一批次
   * @return
   */
  private IChunkMetadata nextChunkMeta() {
    return chunkMetadataList.get(chunkToRead++);
  }
}
