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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

/**
 * MetadataIndexNode构建器
 */
public class MetadataIndexConstructor {

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  private MetadataIndexConstructor() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * 构造元数据索引树
   * 构建【时间序列元数据】的索引，是索引的索引
   * TODO 这里是构造 【时间序列元数据的索引树】，包括设备和物理量，每256个建立一个 设备/物理量节点
   * Construct metadata index tree
   *
   * @param deviceTimeseriesMetadataMap
   * @param out tsfile output
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static MetadataIndexNode constructMetadataIndex(
          // TODO device => TimeseriesMetadata list 的映射
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap, TsFileOutput out)
      throws IOException {
    // tree map , 设备的元数据索引map
    Map<String, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();

    // for timeseriesMetadata of each device
    // 遍历所有设备及其元数据
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      // 如果为空，继续
      if (entry.getValue().isEmpty()) {
        continue;
      }
      // 物理量元数据索引队列
      Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
      TimeseriesMetadata timeseriesMetadata;
      // 创建元数据索引，标记节点类型为物理量
      MetadataIndexNode currentIndexNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
      // TODO 这个循环里会为【时间序列】建立 MetadataIndexNode，其子全是LEAF_MEASUREMENT类型，最多256
      // 遍历设备的时间序列元数据List<TimeseriesMetadata>
      for (int i = 0; i < entry.getValue().size(); i++) {
        // 时间序列元数据
        timeseriesMetadata = entry.getValue().get(i);
        // TODO 只有子节点是256的倍数，才会进入其中
        if (i % config.getMaxDegreeOfIndexNode() == 0) {
          // 如果子节点达到256了，则创建一个新的节点
          if (currentIndexNode.isFull()) {
            // 如果currentIndexNode的子节点达到了256，则添加currentIndexNode到队列中
            addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            // 并创建新的索引节点。
            currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
          }
          // 如果child【达到256】，则向其中添加MetadataIndexEntry，记录时间序列的偏移量
          // TODO 这里特别重要，如果没有达到256，时间序列元数据一直在序列化，pos一直在增加，
          //  只有达到256，创建一个子条目，记录256条时间序列元数据的结束位置。
          currentIndexNode.addEntry(
              new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(), out.getPosition()));
        }
        // 时间序列元数据序列化
        timeseriesMetadata.serializeTo(out.wrapAsStream());
      }
      // 添加当前索引节点到队列
      addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
      // 加入设备元数据索引，设备 和 MetadataIndexNode的映射
      deviceMetadataIndexMap.put(entry.getKey(),
          // 生成根节点
          generateRootNode(
              measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }

    // TODO 下面构建设备元数据的索引树
    // TODO 这里不在时间序列循环里面，而是在设备循环里面, 这里会为【设备建立MetadataIndexNode】
    //  其子MetadataIndexEntry全部为LEAF_DEVICE类型。最多256
    // if not exceed the max child nodes num, ignore the device index and directly point to the
    // measurement
    // 如果未超过最大子节点数，请忽略设备索引并直接指向物理量
    if (deviceMetadataIndexMap.size() <= config.getMaxDegreeOfIndexNode()) {
      // TODO LEAF_DEVICE类型
      MetadataIndexNode metadataIndexNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      // 遍历设备索引Map
      for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        //
        metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        entry.getValue().serializeTo(out.wrapAsStream());
      }
      // TODO 记录当前设备的结束位置
      metadataIndexNode.setEndOffset(out.getPosition());
      return metadataIndexNode;
    }

    // else, build level index for devices
    // TODO 如果超过256了，则构建多层级
    // 设备元数据索引队列
    Queue<MetadataIndexNode> deviceMetadataIndexQueue = new ArrayDeque<>();
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
    // 遍历设备索引Map
    for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
      // when constructing from internal node, each node is related to an entry
      // 从内部节点构造时，每个节点都与一个条目相关
      if (currentIndexNode.isFull()) {
        addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
        currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      }
      currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
      entry.getValue().serializeTo(out.wrapAsStream());
    }
    addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
    // 设备元数据节点，TODO 内部设备类型 INTERNAL_DEVICE
    MetadataIndexNode deviceMetadataIndexNode =
        generateRootNode(deviceMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_DEVICE);
    deviceMetadataIndexNode.setEndOffset(out.getPosition());
    return deviceMetadataIndexNode;
  }

  /**
   * 生成根节点
   * 先生成最底下一层，如果child超过256，则再创建一个MetadataIndexNode，包住256个child，直到只存在一个根节点
   *
   * Generate root node, using the nodes in the queue as leaf nodes. The final metadata tree has two
   * levels: measurement leaf nodes will generate to measurement root node; device leaf nodes will
   * generate to device root node
   *
   * @param metadataIndexNodeQueue queue of metadataIndexNode
   * @param out tsfile output
   * @param type MetadataIndexNode type
   */
  private static MetadataIndexNode generateRootNode(
      Queue<MetadataIndexNode> metadataIndexNodeQueue, TsFileOutput out, MetadataIndexNodeType type)
      throws IOException {
    // 队列长度
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNode metadataIndexNode;
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(type);
    // TODO 只要不是一个，没有得到根节点，就一直执行下面的步骤
    while (queueSize != 1) {
      for (int i = 0; i < queueSize; i++) {
        // 拿出一个
        metadataIndexNode = metadataIndexNodeQueue.poll();
        // when constructing from internal node, each node is related to an entry
        // 如果当前索引节点已经满了，则加入队列并创建一个新的
        // TODO 这里控制了一个MetadataIndexNode不能超过256
        if (currentIndexNode.isFull()) {
          // TODO 如果已经达到256，则会加入队列，并创建一个新的MetadataIndexNode
          addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
          currentIndexNode = new MetadataIndexNode(type);
        }
        // 加入一个child，TODO 这里的out.getPosition()
        currentIndexNode.addEntry(
            new MetadataIndexEntry(metadataIndexNode.peek().getName(), out.getPosition()));
        // 序列化MetadataIndexNode，out.getPosition() 指针会向前推进的
        // TODO 每【添加一个】MetadataIndexEntry则会序列化一下，指针向前推进，所以
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
      currentIndexNode = new MetadataIndexNode(type);
      queueSize = metadataIndexNodeQueue.size();
    }
    return metadataIndexNodeQueue.poll();
  }

  /**
   * 添加MetadataIndexNode，当前索引节点到队列
   * @param currentIndexNode
   * @param metadataIndexNodeQueue
   * @param out
   * @throws IOException
   */
  private static void addCurrentIndexNodeToQueue(
      MetadataIndexNode currentIndexNode,
      Queue<MetadataIndexNode> metadataIndexNodeQueue,
      TsFileOutput out)
      throws IOException {
    // 记录【当前索引】结束偏移量
    currentIndexNode.setEndOffset(out.getPosition());
    // 将当前索引节点添加到队列
    metadataIndexNodeQueue.add(currentIndexNode);
  }
}
