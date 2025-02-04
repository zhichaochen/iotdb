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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.SystemInfo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 存储组信息
 * The storageGroupInfo records the total memory cost of the Storage Group. */
public class StorageGroupInfo {

  private DataRegion dataRegion;

  /**
   * The total Storage group memory cost, including unsealed TsFileResource, ChunkMetadata, WAL,
   * primitive arrays and TEXT values
   */
  private AtomicLong memoryCost; // 当前存储组的内存消耗

  /** The threshold of reporting it's size to SystemInfo */
  private long storageGroupSizeReportThreshold =
      IoTDBDescriptor.getInstance().getConfig().getStorageGroupSizeReportThreshold();

  //
  private AtomicLong lastReportedSize = new AtomicLong();

  /** A set of all unclosed TsFileProcessors in this SG */
  private List<TsFileProcessor> reportedTsps = new CopyOnWriteArrayList<>();

  public StorageGroupInfo(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
    memoryCost = new AtomicLong();
  }

  public DataRegion getDataRegion() {
    return dataRegion;
  }

  /** When create a new TsFileProcessor, call this method */
  public void initTsFileProcessorInfo(TsFileProcessor tsFileProcessor) {
    reportedTsps.add(tsFileProcessor);
  }

  public void addStorageGroupMemCost(long cost) {
    memoryCost.getAndAdd(cost);
  }

  public void releaseStorageGroupMemCost(long cost) {
    memoryCost.getAndAdd(-cost);
  }

  public long getMemCost() {
    return memoryCost.get();
  }

  public List<TsFileProcessor> getAllReportedTsp() {
    return reportedTsps;
  }

  public boolean needToReportToSystem() {
    return memoryCost.get() - lastReportedSize.get() > storageGroupSizeReportThreshold;
  }

  public void setLastReportedSize(long size) {
    lastReportedSize.set(size);
  }

  /**
   * When a TsFileProcessor is closing, remove it from reportedTsps, and report to systemInfo to
   * update SG cost.
   *
   * @param tsFileProcessor
   */
  public void closeTsFileProcessorAndReportToSystem(TsFileProcessor tsFileProcessor) {
    reportedTsps.remove(tsFileProcessor);
    SystemInfo.getInstance().resetStorageGroupStatus(this);
  }
}
