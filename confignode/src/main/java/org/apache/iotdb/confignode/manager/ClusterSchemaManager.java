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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.AsyncClientPool;
import org.apache.iotdb.confignode.client.handlers.InitRegionHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.DataNodeInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 集群元数据管理器
 */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
  private static final int schemaReplicationFactor = conf.getSchemaReplicationFactor();
  private static final int dataReplicationFactor = conf.getDataReplicationFactor();
  private static final int initialSchemaRegionCount = conf.getInitialSchemaRegionCount();
  // 数据分区数
  private static final int initialDataRegionCount = conf.getInitialDataRegionCount();

  // 集群元数据信息
  private static final ClusterSchemaInfo clusterSchemaInfo = ClusterSchemaInfo.getInstance();
  private static final PartitionInfo partitionInfo = PartitionInfo.getInstance();

  private final Manager configManager;

  public ClusterSchemaManager(Manager configManager) {
    this.configManager = configManager;
  }

  /**
   * 设置存储组并分配默认数量的分区
   * Set StorageGroup and allocate the default amount Regions
   *
   * @return SUCCESS_STATUS if the StorageGroup is set and region allocation successful.
   *     NOT_ENOUGH_DATA_NODE if there are not enough DataNode for Region allocation.
   *     STORAGE_GROUP_ALREADY_EXISTS if the StorageGroup is already set.
   */
  public TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq) {
    TSStatus result;
    // data node 数量不足
    if (configManager.getDataNodeManager().getOnlineDataNodeCount()
        < Math.max(initialSchemaRegionCount, initialDataRegionCount)) {
      result = new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode());
      result.setMessage("DataNode is not enough, please register more.");
    }
    else {
      // 存储组已经存在
      if (clusterSchemaInfo.containsStorageGroup(setStorageGroupReq.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode());
        result.setMessage(
            String.format(
                "StorageGroup %s is already set.", setStorageGroupReq.getSchema().getName()));
      }
      else {
        // 创建分区请求
        CreateRegionsReq createRegionsReq = new CreateRegionsReq();

        // Allocate default Regions
        // 分配分区
        allocateRegions(TConsensusGroupType.SchemaRegion, createRegionsReq, setStorageGroupReq);
        allocateRegions(TConsensusGroupType.DataRegion, createRegionsReq, setStorageGroupReq);

        // Create Regions in DataNode
        createRegions(
            setStorageGroupReq.getSchema().getName(),
            createRegionsReq,
            setStorageGroupReq.getSchema().getTTL());

        // Persist StorageGroup and Regions
        // 持久化存储组和分区
        getConsensusManager().write(setStorageGroupReq);
        result = getConsensusManager().write(createRegionsReq).getStatus();
      }
    }
    return result;
  }

  /**
   * 分配region
   * TODO: Allocate by LoadManager */
  private void allocateRegions(
      TConsensusGroupType type, CreateRegionsReq createRegionsReq, SetStorageGroupReq setSGReq) {

    // TODO: Use CopySet algorithm to optimize region allocation policy
    // 副本数
    int replicaCount =
        type.equals(TConsensusGroupType.SchemaRegion)
            ? schemaReplicationFactor
            : dataReplicationFactor;
    // region数，region其实表示一部分数据，什么的一部分数据呢？ 应该是存储组，因为设置存储组的时候需要处理分区
    int regionCount =
        type.equals(TConsensusGroupType.SchemaRegion)
            ? initialSchemaRegionCount
            : initialDataRegionCount;
    // 在线的数据节点
    List<TDataNodeLocation> onlineDataNodes = getDataNodeInfoManager().getOnlineDataNodes();
    // 遍历分区，每个分区会有多个副本，将分区副本随机的分到几个数据节点
    for (int i = 0; i < regionCount; i++) {
      // 随机一下
      Collections.shuffle(onlineDataNodes);

      // 分区副本集合
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      TConsensusGroupId consensusGroupId =
          new TConsensusGroupId(type, partitionInfo.generateNextRegionGroupId());
      regionReplicaSet.setRegionId(consensusGroupId);
      // TODO 随机地分配个副本数量的节点
      regionReplicaSet.setDataNodeLocations(
          new ArrayList<>(onlineDataNodes.subList(0, replicaCount)));
      // 添加分区信息
      createRegionsReq.addRegion(regionReplicaSet);

      switch (type) {
        case SchemaRegion:
          setSGReq.getSchema().addToSchemaRegionGroupIds(consensusGroupId);
          break;
        case DataRegion:
          setSGReq.getSchema().addToDataRegionGroupIds(consensusGroupId);
      }
    }
  }

  /** Create Regions on DataNode TODO: Async create Regions by LoadManager */
  private void createRegions(String storageGroup, CreateRegionsReq createRegionsReq, long TTL) {
    int regionNum =
        initialSchemaRegionCount * schemaReplicationFactor
            + initialDataRegionCount * dataReplicationFactor;
    BitSet bitSet = new BitSet(regionNum);
    List<TEndPoint> schemaRegionEndPoints = new ArrayList<>();
    List<TEndPoint> dataRegionEndPoints = new ArrayList<>();

    for (int retry = 0; retry < 3; retry++) {
      int index = 0;
      CountDownLatch latch = new CountDownLatch(regionNum - bitSet.cardinality());
      for (TRegionReplicaSet regionReplicaSet : createRegionsReq.getRegionReplicaSets()) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          TEndPoint endPoint =
              DataNodeInfo.getInstance()
                  .getOnlineDataNode(dataNodeLocation.getDataNodeId())
                  .getInternalEndPoint();
          InitRegionHandler handler = new InitRegionHandler(index, bitSet, latch);
          switch (regionReplicaSet.getRegionId().getType()) {
            case SchemaRegion:
              if (retry == 0) {
                schemaRegionEndPoints.add(endPoint);
              }
              AsyncClientPool.getInstance()
                  .initSchemaRegion(
                      endPoint, genCreateSchemaRegionReq(storageGroup, regionReplicaSet), handler);
              break;
            case DataRegion:
              if (retry == 0) {
                dataRegionEndPoints.add(endPoint);
              }
              AsyncClientPool.getInstance()
                  .initDataRegion(
                      endPoint,
                      genCreateDataRegionReq(storageGroup, regionReplicaSet, TTL),
                      handler);
          }
          index += 1;
        }
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        LOGGER.error("ClusterSchemaManager was interrupted during create Regions on DataNodes", e);
      }
      if (bitSet.cardinality() == regionNum) {
        break;
      }
    }

    if (bitSet.cardinality() < regionNum) {
      LOGGER.error("Can't create SchemaRegions and DataRegions on DataNodes.");
    } else {
      LOGGER.info("Successfully create SchemaRegions on DataNodes: {}", schemaRegionEndPoints);
      LOGGER.info("Successfully create DataRegions on DataNodes: {}", dataRegionEndPoints);
    }
  }

  /**
   * Get the SchemaRegionGroupIds or DataRegionGroupIds from the specific StorageGroup
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return All SchemaRegionGroupIds when type is SchemaRegion, and all DataRegionGroupIds when
   *     type is DataRegion
   */
  public List<TConsensusGroupId> getRegionGroupIds(String storageGroup, TConsensusGroupType type) {
    return clusterSchemaInfo.getRegionGroupIds(storageGroup, type);
  }

  public TSStatus setTTL(SetTTLReq setTTLReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTTLReq).getStatus();
  }

  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorReq setSchemaReplicationFactorReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setSchemaReplicationFactorReq).getStatus();
  }

  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorReq setDataReplicationFactorReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setDataReplicationFactorReq).getStatus();
  }

  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalReq setTimePartitionIntervalReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTimePartitionIntervalReq).getStatus();
  }

  /**
   * Count StorageGroups by specific path pattern
   *
   * @return CountStorageGroupResp
   */
  public CountStorageGroupResp countMatchedStorageGroups(
      CountStorageGroupReq countStorageGroupReq) {
    ConsensusReadResponse readResponse = getConsensusManager().read(countStorageGroupReq);
    return (CountStorageGroupResp) readResponse.getDataset();
  }

  /**
   * Get StorageGroupSchemas by specific path pattern
   *
   * @return StorageGroupSchemaDataSet
   */
  public StorageGroupSchemaResp getMatchedStorageGroupSchema(
      GetStorageGroupReq getStorageGroupReq) {
    ConsensusReadResponse readResponse = getConsensusManager().read(getStorageGroupReq);
    return (StorageGroupSchemaResp) readResponse.getDataset();
  }

  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  private TCreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet, long TTL) {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    req.setTtl(TTL);
    return req;
  }

  public List<String> getStorageGroupNames() {
    return clusterSchemaInfo.getStorageGroupNames();
  }

  private DataNodeManager getDataNodeInfoManager() {
    return configManager.getDataNodeManager();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
