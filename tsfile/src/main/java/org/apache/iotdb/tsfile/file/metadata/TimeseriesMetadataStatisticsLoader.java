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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeseriesMetadataStatisticsLoader {
  private static boolean removeStat = TSFileDescriptor.getInstance().getConfig().isRemoveStat();

  private static final Logger logger =
      LoggerFactory.getLogger(TimeseriesMetadataStatisticsLoader.class);

  public static Statistics<? extends Serializable> getStatistics(
      TimeseriesMetadata metadata, ByteBuffer buffer, TsFileSequenceReader reader) {
    // stat is in buffer, we just deserialize it
    // currently aligned timeseries also hold statistics
    if (!removeStat || metadata.getTimeSeriesMetadataType() != 0) {
      return Statistics.deserialize(buffer, metadata.getTSDataType());
    }

    // for test
    if (reader == null) {
      return Statistics.getStatsByType(metadata.getTSDataType());
    }

    // load chunk meta data
    ByteBuffer byteBuffer = buffer.slice();
    byteBuffer.limit(metadata.getDataSizeOfChunkMetaDataList());
    ArrayList<ChunkMetadata> chunkMetadataArrayList = new ArrayList<>();
    while (byteBuffer.hasRemaining()) {
      chunkMetadataArrayList.add(ChunkMetadata.deserializeFrom(byteBuffer, metadata));
    }
    // minimize the storage of an ArrayList instance.
    chunkMetadataArrayList.trimToSize();
    metadata.setChunkMetadataList(chunkMetadataArrayList);

    // return null;
    // we need generate statistic from data
    IChunkReader chunkReader = null;
    if ((metadata.getTimeSeriesMetadataType() & 0xc0) > 0) {
      // aligned
    }

    Statistics<? extends Serializable> res = Statistics.getStatsByType(metadata.getTSDataType());
    try {

      // normal series
      for (IChunkMetadata iChunkMetadata : metadata.getChunkMetadataList()) {
        ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
        chunkMetadata.setFilePath(reader.getFileName());
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        chunkReader =
            new ChunkReader(
                new Chunk(
                    chunk.getHeader(),
                    chunk.getData().duplicate(),
                    chunkMetadata.getDeleteIntervalList(),
                    chunkMetadata.getStatistics()),
                null);
      }

      while (chunkReader.hasNextSatisfiedPage()) {
        BatchData data = chunkReader.nextPageData();
        while (data.hasCurrent()) {
          switch (metadata.getTSDataType()) {
            case INT32:
              res.update(data.currentTime(), (int) data.currentValue());
              break;
            case INT64:
              res.update(data.currentTime(), (long) data.currentValue());
              break;
            case BOOLEAN:
              res.update(data.currentTime(), (boolean) data.currentValue());
              break;
            case FLOAT:
              res.update(data.currentTime(), (float) data.currentValue());
              break;
            case TEXT:
              res.update(data.currentTime(), (Binary) data.currentValue());
              break;
            case DOUBLE:
              res.update(data.currentTime(), (Double) data.currentValue());
              break;
            case VECTOR:
              throw new IllegalStateException("shouldn't be vector");
            default:
              throw new IllegalArgumentException("unknown type");
          }
          data.next();
        }
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }

    for (IChunkMetadata iChunkMetadata : metadata.getChunkMetadataList()) {
      iChunkMetadata.setStatistics(res);
    }

    return res;
  }
}
