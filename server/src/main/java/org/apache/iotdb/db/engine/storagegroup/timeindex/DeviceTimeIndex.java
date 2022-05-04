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

package org.apache.iotdb.db.engine.storagegroup.timeindex;

import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备的时间索引
 *
 * 默认配置下：将此 TsFile 中所有时间序列的路径按照 （root - 倒数第二层）分组，每组为一个索引条目，包含：路径、起始时间、终止时间
 * root.sg.d1.s1, 1, 10
 * root.sg.d1.s2, 1, 15
 * root.sg.d2.s1, 1, 10
 * root.sg.d3.s1, 5, 10
 *
 * 按倒数第二层分组后，则 时间索引 为
 * root.sg.d1, 1, 15
 * root.sg.d2, 1, 10
 * root.sg.d3, 5, 10
 *
 * 不按照最后一层分组的原因是，时间序列可能过多，导致时间索引过大，因此选择了倒数第二层作为索引的路径，然而，
 * 当倒数第二层基数过多时（几十万、百万量级），时间索引仍然会较大。
 *
 * 时间索引粒度的影响：
 * 在各个组的序列时间范围相同时（即写入是同步的），索引粒度越粗，越节省内存，且无任何副作用。
 * 在各个组的序列时间范围不同时（如一个序列写入1-10，另一个写入20-30），索引粒度越粗，越容易多读不需要的文件元数据。
 */
public class DeviceTimeIndex implements ITimeIndex {

  private static final Logger logger = LoggerFactory.getLogger(DeviceTimeIndex.class);

  public static final int INIT_ARRAY_SIZE = 64;

  /**
   * 因为一个tsfile文件包含多个设备，所以有多个开始结束时间
   * start times array. */
  protected long[] startTimes; // 开始时间数组

  /**
   * end times array. The values in this array are Long.MIN_VALUE if it's an unsealed sequence
   * tsfile
   */
  protected long[] endTimes; // 结束时间数组

  /**
   * 这个应该针对所有设备
   * min start time */
  private long minStartTime = Long.MAX_VALUE;

  /**
   * 未封口则为Long.MAX_VALUE
   * max end time */
  private long maxEndTime = Long.MIN_VALUE;

  /**
   * device设备名 => 开始 / 结束时间列表index
   * 也就是记录了，某个设备，它的开始结束在数组中的下标，上面设备的下标
   * device -> index of start times array and end times array */
  protected Map<String, Integer> deviceToIndex;

  public DeviceTimeIndex() {
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  public DeviceTimeIndex(Map<String, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.startTimes = startTimes;
    this.endTimes = endTimes;
    this.deviceToIndex = deviceToIndex;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    int deviceNum = deviceToIndex.size();

    ReadWriteIOUtils.write(deviceNum, outputStream);
    for (int i = 0; i < deviceNum; i++) {
      ReadWriteIOUtils.write(startTimes[i], outputStream);
      ReadWriteIOUtils.write(endTimes[i], outputStream);
    }

    for (Entry<String, Integer> stringIntegerEntry : deviceToIndex.entrySet()) {
      String deviceName = stringIntegerEntry.getKey();
      int index = stringIntegerEntry.getValue();
      ReadWriteIOUtils.write(deviceName, outputStream);
      ReadWriteIOUtils.write(index, outputStream);
    }
  }

  @Override
  public DeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);

    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      endTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = ReadWriteIOUtils.readString(inputStream).intern();
      int index = ReadWriteIOUtils.readInt(inputStream);
      deviceToIndex.put(path, index);
    }
    return this;
  }

  @Override
  public DeviceTimeIndex deserialize(ByteBuffer buffer) {
    int deviceNum = buffer.getInt();
    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = buffer.getLong();
      endTimes[i] = buffer.getLong();
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = SerializeUtils.deserializeString(buffer).intern();
      int index = buffer.getInt();
      deviceToIndex.put(path, index);
    }
    return this;
  }

  @Override
  public void close() {
    startTimes = Arrays.copyOfRange(startTimes, 0, deviceToIndex.size());
    endTimes = Arrays.copyOfRange(endTimes, 0, deviceToIndex.size());
  }

  @Override
  public Set<String> getDevices(String tsFilePath, TsFileResource tsFileResource) {
    return deviceToIndex.keySet();
  }

  @Override
  public boolean endTimeEmpty() {
    for (long endTime : endTimes) {
      if (endTime != Long.MIN_VALUE) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean stillLives(long ttlLowerBound) {
    if (ttlLowerBound == Long.MAX_VALUE) {
      return true;
    }
    for (long endTime : endTimes) {
      // the file cannot be deleted if any device still lives
      if (endTime >= ttlLowerBound) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(deviceToIndex)
        + RamUsageEstimator.sizeOf(startTimes)
        + RamUsageEstimator.sizeOf(endTimes);
  }

  private int getDeviceIndex(String deviceId) {
    int index;
    if (deviceToIndex.containsKey(deviceId)) {
      index = deviceToIndex.get(deviceId);
    } else {
      index = deviceToIndex.size();
      deviceToIndex.put(deviceId.intern(), index);
      if (startTimes.length <= index) {
        startTimes = enLargeArray(startTimes, Long.MAX_VALUE);
        endTimes = enLargeArray(endTimes, Long.MIN_VALUE);
      }
    }
    return index;
  }

  private void initTimes(long[] times, long defaultTime) {
    Arrays.fill(times, defaultTime);
  }

  private long[] enLargeArray(long[] array, long defaultValue) {
    long[] tmp = new long[(int) (array.length * 2)];
    initTimes(tmp, defaultValue);
    System.arraycopy(array, 0, tmp, 0, array.length);
    return tmp;
  }

  @Override
  public long getTimePartition(String tsFilePath) {
    try {
      if (deviceToIndex != null && !deviceToIndex.isEmpty()) {
        return StorageEngine.getTimePartition(startTimes[deviceToIndex.values().iterator().next()]);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(tsFilePath);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /** @return the time partition id, if spans multi time partitions, return -1. */
  private long getTimePartitionWithCheck() {
    long partitionId = SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
    for (int index : deviceToIndex.values()) {
      long p = StorageEngine.getTimePartition(startTimes[index]);
      if (partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID) {
        partitionId = p;
      } else {
        if (partitionId != p) {
          return SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
        }
      }

      p = StorageEngine.getTimePartition(endTimes[index]);
      if (partitionId != p) {
        return SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
      }
    }
    return partitionId;
  }

  @Override
  public long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException {
    long partitionId = getTimePartitionWithCheck();
    if (partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID) {
      throw new PartitionViolationException(tsFilePath);
    }
    return partitionId;
  }

  @Override
  public boolean isSpanMultiTimePartitions() {
    long partitionId = getTimePartitionWithCheck();
    return partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
  }

  @Override
  public void updateStartTime(String deviceId, long time) {
    long startTime = getStartTime(deviceId);
    if (time < startTime) {
      int index = getDeviceIndex(deviceId);
      startTimes[index] = time;
    }
    minStartTime = Math.min(minStartTime, time);
  }

  @Override
  public void updateEndTime(String deviceId, long time) {
    long endTime = getEndTime(deviceId);
    if (time > endTime) {
      int index = getDeviceIndex(deviceId);
      endTimes[index] = time;
    }
    maxEndTime = Math.max(maxEndTime, time);
  }

  @Override
  public void putStartTime(String deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    startTimes[index] = time;
    minStartTime = Math.min(minStartTime, time);
  }

  @Override
  public void putEndTime(String deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    endTimes[index] = time;
    maxEndTime = Math.max(maxEndTime, time);
  }

  @Override
  public long getStartTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MAX_VALUE;
    }
    return startTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public long getEndTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MIN_VALUE;
    }
    return endTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public boolean checkDeviceIdExist(String deviceId) {
    return deviceToIndex.containsKey(deviceId);
  }

  @Override
  public long getMinStartTime() {
    return minStartTime;
  }

  @Override
  public long getMaxEndTime() {
    return maxEndTime;
  }

  @Override
  public int compareDegradePriority(ITimeIndex timeIndex) {
    if (timeIndex instanceof DeviceTimeIndex) {
      return Long.compare(getMinStartTime(), timeIndex.getMinStartTime());
    } else if (timeIndex instanceof FileTimeIndex) {
      return -1;
    } else {
      logger.error("Wrong timeIndex type {}", timeIndex.getClass().getName());
      throw new RuntimeException("Wrong timeIndex type " + timeIndex.getClass().getName());
    }
  }

  @Override
  public boolean mayContainsDevice(String device) {
    return deviceToIndex.containsKey(device);
  }
}
