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
package org.apache.iotdb.db.writelog.node;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.utils.ThreadUtils;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.concurrent.ThreadName.WAL_FLUSH;

/**
 * 专用的写日志节点
 * 当前类专用于写入tsFile的wal，与虚拟存储组形成1对多的关系
 * WriteLogNode用于管理TsFile的WAL（写前日志）
 * This WriteLogNode is used to manage insert ahead logs of a TsFile. */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);

  private final String identifier;

  private final String logDirectory;

  private ILogWriter currentFileWriter;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private volatile ByteBuffer logBufferWorking; // 工作状态下的日志缓存
  //
  private volatile ByteBuffer logBufferIdle;
  // 正在刷盘中的日志，如果不在刷盘中，该对象为空
  private volatile ByteBuffer logBufferFlushing;

  // used for the convenience of deletion
  private volatile ByteBuffer[] bufferArray;

  // 能否写入缓存的开关
  private final Object switchBufferCondition = new Object();

  // 读写锁
  private final ReentrantLock lock = new ReentrantLock();
  private final ExecutorService FLUSH_BUFFER_THREAD_POOL; // 刷盘的线程池

  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0; // 当前缓存的日志数量

  // 当前节点是否是已删除的状态
  private final AtomicBoolean deleted = new AtomicBoolean(false);

  private int bufferOverflowNum = 0;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public ExclusiveWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
        DirectoryManager.getInstance().getWALFolder() + File.separator + this.identifier;
    if (SystemFileFactory.INSTANCE.getFile(logDirectory).mkdirs()) {
      logger.info("create the WAL folder {}.", logDirectory);
    }
    // this.identifier contains the storage group name + tsfile name.
    FLUSH_BUFFER_THREAD_POOL =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(WAL_FLUSH.getName() + "-" + this.identifier);
  }

  @Override
  public void initBuffer(ByteBuffer[] byteBuffers) {
    this.logBufferWorking = byteBuffers[0];
    this.logBufferIdle = byteBuffers[1];
    this.bufferArray = byteBuffers;
  }

  /**
   * 写入WAL
   * @param plan - a PhysicalPlan
   * @throws IOException
   */
  @Override
  public void write(PhysicalPlan plan) throws IOException {
    // 如果当前节点已经删除，则抛出IO异常
    if (deleted.get()) {
      throw new IOException("WAL node deleted");
    }
    // 加锁
    lock.lock();
    try {
      // 写入写前日志
      putLog(plan);
      // 如果缓存的日志数量超过了配置的缓存的最大数量，则同步刷写到磁盘
      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
    } catch (BufferOverflowException e) {
      // if the size of a single plan bigger than logBufferWorking
      // we need to clear the buffer to drop something wrong that has written.
      logBufferWorking.clear();
      int neededSize = plan.getSerializedSize();
      throw new IOException(
          "Log cannot fit into the buffer, please increase wal_buffer_size to more than "
              + neededSize * 2,
          e);
    } finally {
      lock.unlock();
    }
  }

  private void putLog(PhysicalPlan plan) {
    try {
      // 序列化日志
      plan.serialize(logBufferWorking);
    } catch (BufferOverflowException e) {
      // 如果抓到缓存溢出异常，则尝试刷盘，然后再序列化进logBufferWorking缓存
      bufferOverflowNum++;
      if (bufferOverflowNum > 200) {
        logger.info(
            "WAL bytebuffer overflows too many times. If this occurs frequently, please increase wal_buffer_size.");
        bufferOverflowNum = 0;
      }
      // 同步刷盘
      sync();
      // 计划序列化
      plan.serialize(logBufferWorking);
    }
    // 日志数量加1
    bufferedLogNum++;
  }

  @Override
  public void close() {
    sync();
    forceWal();
    lock.lock();
    try {
      synchronized (switchBufferCondition) {
        while (logBufferFlushing != null && !deleted.get()) {
          switchBufferCondition.wait();
        }
        switchBufferCondition.notifyAll();
      }

      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        logger.debug("WAL file {} is closed", currentFileWriter);
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.warn("Cannot close log node {} because:", identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for current buffer being flushed interrupted");
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void release() {
    ThreadUtils.stopThreadPool(FLUSH_BUFFER_THREAD_POOL, WAL_FLUSH);
    lock.lock();
    try {
      if (this.logBufferWorking != null && this.logBufferWorking instanceof MappedByteBuffer) {
        MmapUtil.clean((MappedByteBuffer) this.logBufferFlushing);
      }
      if (this.logBufferIdle != null && this.logBufferIdle instanceof MappedByteBuffer) {
        MmapUtil.clean((MappedByteBuffer) this.logBufferIdle);
      }
      if (this.logBufferFlushing != null && this.logBufferFlushing instanceof MappedByteBuffer) {
        MmapUtil.clean((MappedByteBuffer) this.logBufferFlushing);
      }
      logger.debug("ByteBuffers are freed successfully");
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void forceSync() {
    if (deleted.get()) {
      return;
    }
    sync();
    forceWal();
  }

  @Override
  public void notifyStartFlush() throws FileNotFoundException {
    lock.lock();
    try {
      close();
      nextFileWriter();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    lock.lock();
    try {
      File logFile =
          SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + ++lastFlushedId);
      discard(logFile);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getLogDirectory() {
    return logDirectory;
  }

  @Override
  public ByteBuffer[] delete() throws IOException {
    lock.lock();
    try {
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(logDirectory));
      deleted.set(true);
      return this.bufferArray;
    } finally {
      FLUSH_BUFFER_THREAD_POOL.shutdown();
      lock.unlock();
    }
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(
        logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles);
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      logger.info("Log file does not exist");
    } else {
      try {
        FileUtils.forceDelete(logFile);
        logger.info("Log node {} cleaned old file", identifier);
      } catch (IOException e) {
        logger.warn("Old log file {} of {} cannot be deleted", logFile.getName(), identifier, e);
      }
    }
  }

  private void forceWal() {
    lock.lock();
    try {
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.warn("Log node {} force failed.", identifier, e);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 同步
   */
  private void sync() {
    lock.lock();
    try {
      // 如果缓存的日志数量为0，则返回
      if (bufferedLogNum == 0) {
        return;
      }
      // 获取当前日志写入器
      ILogWriter currWriter = getCurrentFileWriter();
      // 将working buffer 赋值给 flushing buffer
      switchBufferWorkingToFlushing();
      // 开启一个线程进行刷盘
      FLUSH_BUFFER_THREAD_POOL.submit(() -> flushBuffer(currWriter));
      // 将缓存的日志数量设置成0
      bufferedLogNum = 0;
      logger.debug("Log node {} ends sync.", identifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for available buffer interrupted");
    } catch (FileNotFoundException e) {
      logger.warn("can not found file {}", identifier, e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 刷写缓存
   * @param writer
   */
  private void flushBuffer(ILogWriter writer) {
    try {
      // 刷盘
      writer.write(logBufferFlushing);
    } catch (Throwable e) {
      logger.error("Log node {} sync failed, change system mode to read-only", identifier, e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    } finally {
      // switch buffer flushing to idle and notify the sync thread
      // 切换flushing缓存到idle缓存，并通知同步线程
      synchronized (switchBufferCondition) {
        logBufferIdle = logBufferFlushing;
        logBufferFlushing = null;
        switchBufferCondition.notifyAll();
      }
    }
  }

  /**
   *
   * @throws InterruptedException
   */
  private void switchBufferWorkingToFlushing() throws InterruptedException {
    synchronized (switchBufferCondition) {
      // 如果logBufferFlushing不为null，则表示正在刷盘中，等待100ms
      while (logBufferFlushing != null && !deleted.get()) {
        switchBufferCondition.wait(100);
      }
      // 将工作中的缓存付给
      logBufferFlushing = logBufferWorking;
      //
      logBufferWorking = logBufferIdle;
      logBufferWorking.clear();
      logBufferIdle = null;
    }
  }

  private ILogWriter getCurrentFileWriter() throws FileNotFoundException {
    if (currentFileWriter == null) {
      nextFileWriter();
    }
    return currentFileWriter;
  }

  private void nextFileWriter() throws FileNotFoundException {
    fileId++;
    File newFile = SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + fileId);
    if (newFile.getParentFile().mkdirs()) {
      logger.info("create WAL parent folder {}.", newFile.getParent());
    }
    logger.debug("WAL file {} is opened", newFile);
    currentFileWriter = new LogWriter(newFile, config.getForceWalPeriodInMs() == 0);
  }

  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((ExclusiveWriteLogNode) obj) == 0;
  }

  @Override
  public String toString() {
    return "Log node " + identifier;
  }

  @Override
  public int compareTo(ExclusiveWriteLogNode o) {
    return this.identifier.compareTo(o.identifier);
  }
}
