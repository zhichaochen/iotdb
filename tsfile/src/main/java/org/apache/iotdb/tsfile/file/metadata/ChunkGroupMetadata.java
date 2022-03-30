/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.file.metadata;

import java.util.List;

/**
 * chunk group的元数据
 *
 * 怎么理解ChunkGroup呢？
 * 每个chunk保存了某个字段的数据，也就是行式数据库的一列，chunk group保存了一个对象的多个字段，也就是多列。
 * chunk group相当于一张表，其中的每个chunk是表中的一列数据
 *
 * 一个chunk包含多个page，page是该字段在一定时间范围内数据的集合。
 *
 * Only maintained when writing, not serialized to TsFile */
public class ChunkGroupMetadata {

  private String device; // 设备

  private List<ChunkMetadata> chunkMetadataList; // 当前设备的多个chunk的元数据列表

  public ChunkGroupMetadata(String device, List<ChunkMetadata> chunkMetadataList) {
    this.device = device;
    this.chunkMetadataList = chunkMetadataList;
  }

  public String getDevice() {
    return device;
  }

  public List<ChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }
}
