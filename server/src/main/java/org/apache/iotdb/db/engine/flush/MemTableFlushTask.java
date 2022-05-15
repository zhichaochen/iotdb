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
package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 内存表刷写任务
 * 用一个管道模型去刷写一个memtable，即排序的内存表 -> 编码 -> 写入磁盘
 * flush task to flush one memtable using a pipeline model to flush, which is sort memtable ->
 * encoding -> write to disk (io task)
 */
public class MemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
      FlushSubTaskPoolManager.getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  // 编码任务队列，存储了需要编码的数据
  private final LinkedBlockingQueue<Object> encodingTaskQueue = new LinkedBlockingQueue<>();
  // IO任务队列，存储了刷盘的数据
  private final LinkedBlockingQueue<Object> ioTaskQueue =
      (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private String storageGroup; // 存储组

  private IMemTable memTable; // 内存表

  private volatile long memSerializeTime = 0L;
  private volatile long ioTime = 0L;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   */
  public MemTableFlushTask(
      IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    // 开启一个编码任务
    this.encodingTaskFuture = SUB_TASK_POOL_MANAGER.submit(encodingTask);
    // 开启一个IO任务
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);
    LOGGER.debug(
        "flush task of Storage group {} memtable is created, flushing to file {}.",
        storageGroup,
        writer.getFile().getName());
  }

  /**
   * 同步刷写内存表
   * TODO 这里开始刷写内存表，这里的产生的任务的顺序也决定了刷盘时要做的事情的顺序
   * the function for flushing memtable. */
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    // 打印日志
    LOGGER.info(
        "The memTable size of SG {} is {}, the avg series points num in chunk is {}, total timeseries number is {}",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber(),
        memTable.getSeriesNumber());

    // 内存控制，预估刷盘需要的内存，并更新使用内存
    long estimatedTemporaryMemSize = 0L;
    if (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()) {
      estimatedTemporaryMemSize =
          memTable.memSize() / memTable.getSeriesNumber() * config.getIoTaskQueueSizeForFlushing();
      SystemInfo.getInstance().applyTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    // 记录开始时间戳
    long start = System.currentTimeMillis();
    // 记录排序时长
    long sortTime = 0;

    // for map do not use get(key) to iterate
    // 遍历mem table的所有 chunk group，或者说遍历mem table中的所有设备
    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> memTableEntry :
        memTable.getMemTableMap().entrySet()) {
      // TODO 开启一个刷写chunk group的任务，一个TsFile包含多个chunk group，所以会创建多个chunk group的任务
      encodingTaskQueue.put(new StartFlushGroupIOTask(memTableEntry.getKey().toStringID()));
      // 表示一个chunk group
      final Map<String, IWritableMemChunk> value = memTableEntry.getValue().getMemChunkMap();
      // 遍历chunk group中的多个chunk
      for (Map.Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
        // 开始时间
        long startTime = System.currentTimeMillis();
        // 内存chunk
        IWritableMemChunk series = iWritableMemChunkEntry.getValue();
        /*
         * sort task (first task of flush pipeline)
         */
        // 对数据进行排序
        series.sortTvListForFlush();
        // 记录排序时间
        sortTime += System.currentTimeMillis() - startTime;
        // chunk编码任务，对chunk进行编码
        encodingTaskQueue.put(series);
      }

      // TODO 这里处理一个结束chunk group任务
      encodingTaskQueue.put(new EndChunkGroupIoTask());
    }
    // 加入编码任务队列
    encodingTaskQueue.put(new TaskEnd());
    LOGGER.debug(
        "Storage group {} memtable flushing into file {}: data sort time cost {} ms.",
        storageGroup,
        writer.getFile().getName(),
        sortTime);

    try {
      encodingTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      ioTaskFuture.cancel(true);
      throw e;
    }

    ioTaskFuture.get();

    try {
      // 写入本次操作的索引范围，就是记录了当前数据的开始、结束位置
      writer.writePlanIndices();
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (config.isEnableMemControl()) {
      if (estimatedTemporaryMemSize != 0) {
        SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
      }
      SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);
    }

    // 统计flush指标
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .timer(
              System.currentTimeMillis() - start,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              "flush");
    }

    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup,
        memTable,
        System.currentTimeMillis() - start);
  }

  /**
   * 编码任务，将内存中的数据，进行编码
   * encoding task (second task of pipeline) */
  private Runnable encodingTask =
      new Runnable() {

        @SuppressWarnings("squid:S135")
        @Override
        public void run() {
          LOGGER.debug(
              "Storage group {} memtable flushing to file {} starts to encoding data.",
              storageGroup,
              writer.getFile().getName());
          while (true) {
            // 从队列中拿出一个任务
            Object task;
            try {
              task = encodingTaskQueue.take();
            } catch (InterruptedException e1) {
              LOGGER.error("Take task into ioTaskQueue Interrupted");
              Thread.currentThread().interrupt();
              break;
            }
            // 如果是开始/结束chunk group的任务
            if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
              try {
                // TODO 将其放入IO队列中，IO线程会消费队列中的任务，执行刷盘操作
                ioTaskQueue.put(task);
              } catch (
                  @SuppressWarnings("squid:S2142")
                  InterruptedException e) {
                LOGGER.error(
                    "Storage group {} memtable flushing to file {}, encoding task is interrupted.",
                    storageGroup,
                    writer.getFile().getName(),
                    e);
                // generally it is because the thread pool is shutdown so the task should be aborted
                break;
              }
            }
            // 如果任务结束，则跳出
            else if (task instanceof TaskEnd) {
              break;
            }
            // 执行编码任务
            else {
              // 开始时间
              long starTime = System.currentTimeMillis();
              // 内存chunk，在内存中的数据
              IWritableMemChunk writableMemChunk = (IWritableMemChunk) task;
              // 创建chunk写入器
              IChunkWriter seriesWriter = writableMemChunk.createIChunkWriter();
              // 对chunk数据进行编码
              writableMemChunk.encode(seriesWriter);
              // 密封当前页
              seriesWriter.sealCurrentPage();
              // 清空页写入器
              seriesWriter.clearPageWriter();
              try {
                // 将内存数据加入队列
                ioTaskQueue.put(seriesWriter);
              } catch (InterruptedException e) {
                LOGGER.error("Put task into ioTaskQueue Interrupted");
                Thread.currentThread().interrupt();
              }
              memSerializeTime += System.currentTimeMillis() - starTime;
            }
          }
          try {
            // 将TaskEnd加入队列
            ioTaskQueue.put(new TaskEnd());
          } catch (InterruptedException e) {
            LOGGER.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
          }

          LOGGER.debug(
              "Storage group {}, flushing memtable {} into disk: Encoding data cost " + "{} ms.",
              storageGroup,
              writer.getFile().getName(),
              memSerializeTime);
        }
      };

  /**
   * IO任务
   * io task (third task of pipeline) */
  @SuppressWarnings("squid:S135")
  private Runnable ioTask =
      () -> {
        LOGGER.debug(
            "Storage group {} memtable flushing to file {} start io.",
            storageGroup,
            writer.getFile().getName());
        while (true) {
          Object ioMessage = null;
          try {
            // 从IO队列中拿出一条消息
            ioMessage = ioTaskQueue.take();
          } catch (InterruptedException e1) {
            LOGGER.error("take task from ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
            break;
          }
          // 开始时间
          long starTime = System.currentTimeMillis();
          try {
            // 开始flush chunk group 任务
            if (ioMessage instanceof StartFlushGroupIOTask) {
              this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
            } else if (ioMessage instanceof TaskEnd) {
              // 如果是任务结束消息，则跳出
              break;
            }
            // 结束flush chunk group 任务
            else if (ioMessage instanceof EndChunkGroupIoTask) {
              // 如果是结束chunk group任务，则记录最大、最小值
              this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
              this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
              // TODO 这里是结束chunk group的任务
              this.writer.endChunkGroup();
            }
            // 其他的说明是写chunk数据
            else {
              // TODO 写数据到TsFile, 在这里将进入TSFile模块
              ((IChunkWriter) ioMessage).writeToFileWriter(this.writer);
            }
          } catch (IOException e) {
            LOGGER.error(
                "Storage group {} memtable {}, io task meets error.", storageGroup, memTable, e);
            throw new FlushRunTimeException(e);
          }
          ioTime += System.currentTimeMillis() - starTime;
        }
        LOGGER.debug(
            "flushing a memtable to file {} in storage group {}, io cost {}ms",
            writer.getFile().getName(),
            storageGroup,
            ioTime);
      };

  static class TaskEnd {

    TaskEnd() {}
  }

  static class EndChunkGroupIoTask {

    EndChunkGroupIoTask() {}
  }

  static class StartFlushGroupIOTask {

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }
}
