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

package org.apache.iotdb.db.query.udf.datastructure;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.udf.service.TemporaryQueryDataFileService;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 可序列化列表
 */
public interface SerializableList {

  int INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL =
      IoTDBDescriptor.getInstance().getConfig().getUdfInitialByteArrayLengthForMemoryControl();

  void serialize(PublicBAOS outputStream) throws IOException;

  void deserialize(ByteBuffer byteBuffer);

  void release();

  void init();

  SerializationRecorder getSerializationRecorder();

  /**
   * 序列化记录器
   */
  class SerializationRecorder {

    protected static final int NOT_SERIALIZED = -1;

    protected final long queryId; // 查询ID

    protected boolean isSerialized; // 是否已经序列化
    protected int serializedByteLength; // 序列化的字节长度
    protected int serializedElementSize; // 序列化的元素size

    protected String fileName; // 文件名
    protected RandomAccessFile file; // 可随机访问的文件
    protected FileChannel fileChannel; // 文件通道

    public SerializationRecorder(long queryId) {
      this.queryId = queryId;
      isSerialized = false;
      serializedByteLength = NOT_SERIALIZED;
      serializedElementSize = NOT_SERIALIZED;
    }

    public void markAsSerialized() {
      isSerialized = true;
    }

    public void markAsNotSerialized() {
      isSerialized = false;
      serializedByteLength = NOT_SERIALIZED;
      serializedElementSize = NOT_SERIALIZED;
    }

    public boolean isSerialized() {
      return isSerialized;
    }

    public void setSerializedByteLength(int serializedByteLength) {
      this.serializedByteLength = serializedByteLength;
    }

    public int getSerializedByteLength() {
      return serializedByteLength;
    }

    public void setSerializedElementSize(int serializedElementSize) {
      this.serializedElementSize = serializedElementSize;
    }

    public int getSerializedElementSize() {
      return serializedElementSize;
    }

    /**
     * 获取RandomAccessFile
     * @return
     * @throws IOException
     */
    public RandomAccessFile getFile() throws IOException {
      if (file == null) {
        if (fileName == null) {
          // 注册一个临时文件
          fileName = TemporaryQueryDataFileService.getInstance().register(this);
        }
        file = new RandomAccessFile(SystemFileFactory.INSTANCE.getFile(fileName), "rw");
      }
      return file;
    }

    public void closeFile() throws IOException {
      if (file == null) {
        return;
      }
      closeFileChannel();
      file.close();
      file = null;
    }

    public FileChannel getFileChannel() throws IOException {
      if (fileChannel == null) {
        fileChannel = getFile().getChannel();
      }
      return fileChannel;
    }

    public void closeFileChannel() throws IOException {
      if (fileChannel == null) {
        return;
      }
      fileChannel.close();
      fileChannel = null;
    }

    public long getQueryId() {
      return queryId;
    }
  }

  /**
   * 序列化
   * @throws IOException
   */
  default void serialize() throws IOException {
    // 序列化记录器
    SerializationRecorder recorder = getSerializationRecorder();
    // 如果已经序列化则返回
    if (recorder.isSerialized()) {
      return;
    }
    // 创建一个ByteArrayOutputStream
    PublicBAOS outputStream = new PublicBAOS();
    // 序列化
    serialize(outputStream);
    // 分配bytebuffer
    ByteBuffer byteBuffer = ByteBuffer.allocate(outputStream.size());
    // 将数组缓存转化为byteBuffer
    byteBuffer.put(outputStream.getBuf(), 0, outputStream.size());
    // 回到第一个字节
    byteBuffer.flip();
    // TODO 序列化的内容写入磁盘
    recorder.getFileChannel().write(byteBuffer);
    // 关闭文件
    recorder.closeFile();
    // 释放资源
    release();
    // 标记为已序列化
    recorder.markAsSerialized();
  }

  default void deserialize() throws IOException {
    SerializationRecorder recorder = getSerializationRecorder();
    if (!recorder.isSerialized()) {
      return;
    }
    init();
    ByteBuffer byteBuffer = ByteBuffer.allocate(recorder.getSerializedByteLength());
    recorder.getFileChannel().read(byteBuffer);
    byteBuffer.flip();
    deserialize(byteBuffer);
    recorder.closeFile();
    recorder.markAsNotSerialized();
  }
}
