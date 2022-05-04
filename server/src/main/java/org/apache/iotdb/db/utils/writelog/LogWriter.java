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
package org.apache.iotdb.db.utils.writelog;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * 日志写入器，作用是将Bytebuffer真正写入磁盘
 * （在写入数据之前，先记录wal日志）
 *
 * CRC32，冗余码校验，是一种数据错误检查技术
 * 会产生一个32bit的校验值
 * 因为CRC32会对byte数组中的每一位都进行计算，所以，能判断写入磁盘的数据和再次拿出的数据是否一致，
 * 如果不一致可能磁盘文件损坏了，
 *
 *
 * LogWriter使用FileChannel将二进制日志写入文件，并使用CRC32计算每个日志的校验和。
 * LogWriter writes the binary logs into a file using FileChannel together with check sums of each
 * log calculated using CRC32.
 */
public class LogWriter implements ILogWriter {
  private static final Logger logger = LoggerFactory.getLogger(LogWriter.class);

  private File logFile; // 日志文件
  private FileOutputStream fileOutputStream; // 文件输出流
  private FileChannel channel; // 文件通道
  private final CRC32 checkSummer = new CRC32(); // 完整性校验
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4); // 字节缓存，记录日志长度
  private final ByteBuffer checkSumBuffer = ByteBuffer.allocate(8); // 记录检查数
  private final boolean forceEachWrite; // 每次都强制写入，刷盘

  /**
   * @param logFilePath
   * @param forceEachWrite
   * @throws FileNotFoundException
   */
  @TestOnly
  public LogWriter(String logFilePath, boolean forceEachWrite) throws FileNotFoundException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    this.forceEachWrite = forceEachWrite;

    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
  }

  public LogWriter(File logFile, boolean forceEachWrite) throws FileNotFoundException {
    this.logFile = logFile;
    this.forceEachWrite = forceEachWrite;

    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
  }

  /**
   * 写入wal日志
   * @param logBuffer WAL logs that have been converted to bytes
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer logBuffer) throws IOException {
    // 如果文件通道为空，则获取
    if (channel == null) {
      fileOutputStream = new FileOutputStream(logFile, true);
      channel = fileOutputStream.getChannel();
    }
    // 写模式切换成读模式
    logBuffer.flip();
    // logBuffer的长度，日志长度
    int logSize = logBuffer.limit();
    // 4 bytes size and 8 bytes check sum

    // 重置校验器
    checkSummer.reset();
    // 计算字节总和
    checkSummer.update(logBuffer);
    // 校验和
    long checkSum = checkSummer.getValue();

    // TODO checkSummer会计算buffer中的所有字节，计算完之后欧指针已经指向了最后以为，
    //  所以需要flip一下，重置到开始指针，以便channel在写入的时候，是从0字节开始写入的。
    logBuffer.flip();

    lengthBuffer.clear();
    checkSumBuffer.clear();
    // 写入日志长度到bytebuffer
    lengthBuffer.putInt(logSize);
    // 写入记录校验和到bytebuffer
    checkSumBuffer.putLong(checkSum);
    lengthBuffer.flip();
    checkSumBuffer.flip();

    // TODO 将bytebuffer写入channel，并刷盘
    try {
      // 写入日志长度
      channel.write(lengthBuffer);
      // 写入日志
      channel.write(logBuffer);
      // 写入check sum
      channel.write(checkSumBuffer);

      // 如果需要强制刷盘，则强制刷盘
      if (this.forceEachWrite) {
        // TODO 强制刷盘
        channel.force(true);
      }
    } catch (ClosedChannelException ignored) {
      logger.warn("someone interrupt current thread, so no need to do write for io safety");
    }
  }

  /**
   * 强制刷盘
   * @throws IOException
   */
  @Override
  public void force() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.force(true);
    }
  }

  /**
   * 关闭
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (channel != null) {
      if (channel.isOpen()) {
        channel.force(true);
      }
      fileOutputStream.close();
      fileOutputStream = null;
      channel.close();
      channel = null;
    }
  }

  @Override
  public String toString() {
    return "LogWriter{" + "logFile=" + logFile + '}';
  }
}
