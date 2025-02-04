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
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

/**
 * 内存中的chunk group
 * TODO 其实一个chunk group就是一个entity，一个设备，chunk表示一列数据，chunk group表示一个实体的多列数据
 * 表示一个驻扎在内存中的ChunkGroup，记录了chunk group的信息
 */
public interface IWritableMemChunkGroup extends WALEntryValue {

  void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end);

  void release();

  long count();

  boolean contains(String measurement);

  // 写入数据
  void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  Map<String, IWritableMemChunk> getMemChunkMap();

  int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp);

  long getCurrentTVListSize(String measurement);
}
