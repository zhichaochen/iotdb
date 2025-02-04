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

package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 内存表中的一个chunk group
 */
public class WritableMemChunkGroup implements IWritableMemChunkGroup {

  // 一个chunk group中恩多个chunk，也就是设备中的多个物理量
  // key：物理量，value：物理量对应的chunk
  private Map<String, IWritableMemChunk> memChunkMap;

  public WritableMemChunkGroup() {
    memChunkMap = new HashMap<>();
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    int emptyColumnCount = 0;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      memChunk.write(
          times,
          columns[i],
          bitMaps == null ? null : bitMaps[i],
          schemaList.get(i - emptyColumnCount).getType(),
          start,
          end);
    }
  }

  private IWritableMemChunk createMemChunkIfNotExistAndGet(IMeasurementSchema schema) {
    return memChunkMap.computeIfAbsent(
        schema.getMeasurementId(), k -> new WritableMemChunk(schema));
  }

  @Override
  public void release() {
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      memChunk.release();
    }
  }

  @Override
  public long count() {
    long count = 0;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      count += memChunk.count();
    }
    return count;
  }

  @Override
  public boolean contains(String measurement) {
    return memChunkMap.containsKey(measurement);
  }

  /**
   * 写入
   * @param insertTime
   * @param objectValue
   * @param schemaList
   */
  @Override
  public void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    int emptyColumnCount = 0;
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null) {
        emptyColumnCount++;
        continue;
      }
      // 获取一个可写入的内存块
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      // 写入数据
      memChunk.write(insertTime, objectValue[i]);
    }
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    int deletedPointsNumber = 0;
    Iterator<Entry<String, IWritableMemChunk>> iter = memChunkMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, IWritableMemChunk> entry = iter.next();
      IWritableMemChunk chunk = entry.getValue();
      // the key is measurement rather than component of multiMeasurement
      PartialPath fullPath = devicePath.concatNode(entry.getKey());
      if (originalPath.matchFullPath(fullPath)) {
        // matchFullPath ensures this branch could work on delete data of unary or multi measurement
        // and delete timeseries
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          iter.remove();
        }
        deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
      }
    }
    return deletedPointsNumber;
  }

  @Override
  public long getCurrentTVListSize(String measurement) {
    return memChunkMap.get(measurement).getTVList().rowCount();
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      size += ReadWriteIOUtils.sizeToWrite(entry.getKey());
      size += entry.getValue().serializedSize();
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putInt(memChunkMap.size());
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      WALWriteUtils.write(entry.getKey(), buffer);
      IWritableMemChunk memChunk = entry.getValue();
      memChunk.serializeToWAL(buffer);
    }
  }

  public static WritableMemChunkGroup deserialize(DataInputStream stream) throws IOException {
    WritableMemChunkGroup memChunkGroup = new WritableMemChunkGroup();
    int memChunkMapSize = stream.readInt();
    for (int i = 0; i < memChunkMapSize; ++i) {
      String measurement = ReadWriteIOUtils.readString(stream);
      IWritableMemChunk memChunk = WritableMemChunk.deserialize(stream);
      memChunkGroup.memChunkMap.put(measurement, memChunk);
    }
    return memChunkGroup;
  }
}
