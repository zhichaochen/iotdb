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

package org.apache.iotdb.tsfile.file;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.IOException;

/**
 * MetaMarker表示页眉和页脚的类型。枚举不用于节省空间。
 * MetaMarker denotes the type of headers and footers. Enum is not used for space saving. */
@SuppressWarnings({"squid:S1133"}) // Deprecated code should be removed
public class MetaMarker {

  public static final byte CHUNK_GROUP_HEADER = 0;
  /** Chunk header marker and this chunk has more than one page. */
  public static final byte CHUNK_HEADER = 1;

  public static final byte SEPARATOR = 2; // 分隔符
  /**
   * @deprecated (Since TsFile version 3, the marker VERSION is no longer used in TsFile. It should
   *     be removed when TsFile upgrade to version 4)
   */
  @Deprecated public static final byte VERSION = 3;

  // following this marker are two longs marking the minimum and maximum indices of operations
  // involved in the last flushed MemTable, which are generally used to support checkpoint,
  // snapshot, or backup.
  // 在这个标记后面是两个长标记，分别标记最后一次刷新的MemTable中涉及的操作的最小索引和最大索引，它们通常用于支持检查点、快照或备份。
  public static final byte OPERATION_INDEX_RANGE = 4; // 操作索引范围

  /** Chunk header marker and this chunk has only one page. */
  public static final byte ONLY_ONE_PAGE_CHUNK_HEADER = 5;

  /** Time Chunk header marker and this chunk has more than one page. */
  public static final byte TIME_CHUNK_HEADER =
      (byte) (CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK);

  /** Value Chunk header marker and this chunk has more than one page. */
  public static final byte VALUE_CHUNK_HEADER =
      (byte) (CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK);

  /** Time Chunk header marker and this chunk has only one page. */
  public static final byte ONLY_ONE_PAGE_TIME_CHUNK_HEADER =
      (byte) (ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK);

  /** Value Chunk header marker and this chunk has only one page. */
  public static final byte ONLY_ONE_PAGE_VALUE_CHUNK_HEADER =
      (byte) (ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK);

  private MetaMarker() {}

  public static void handleUnexpectedMarker(byte marker) throws IOException {
    throw new IOException("Unexpected marker " + marker);
  }
}
