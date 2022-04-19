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
package org.apache.iotdb.db.metadata.mnode;

/**
 * 元数据节点工具类
 */
public class MNodeUtils {

  /**
   * 当模板或物理量表示的度量被添加到节点时，该节点应该被设置为EntityMNode，在调用方法之前，先锁定相关的MTree部分
   * When a measurement, represented by template or MeasurementMNode, is going to be added to a
   * node, the node should be set to entity. Before invoking this method, lock the related MTree
   * part first.
   *
   * @param node node to be transformed
   * @return generated entityMNode
   */
  public static IEntityMNode setToEntity(IMNode node) {
    IEntityMNode entityMNode;
    if (node.isEntity()) {
      entityMNode = node.getAsEntityMNode();
    } else {
      if (node.isStorageGroup()) {
        entityMNode =
            new StorageGroupEntityMNode(
                node.getParent(), node.getName(), node.getAsStorageGroupMNode().getDataTTL());
        node.moveDataToNewMNode(entityMNode);
      } else {
        entityMNode = new EntityMNode(node.getParent(), node.getName());
        if (node.getParent() != null) {
          node.getParent().replaceChild(node.getName(), entityMNode);
        } else {
          node.moveDataToNewMNode(entityMNode);
        }
      }
    }
    return entityMNode;
  }

  /**
   * 当此EntityNode下没有物理量时（由template或MeasurementMNode表示）时，它不应再充当实体。
   *
   * When there's no measurement, represented by template or MeasurementMNode, is under this
   * entityMNode, it should not act as entity anymore. Before invoking this method, lock related
   * MTree structure first.
   *
   * @param entityMNode node to be transformed
   * @return generated NoEntity node
   */
  public static IMNode setToInternal(IEntityMNode entityMNode) {
    IMNode node;
    IMNode parent = entityMNode.getParent();
    if (entityMNode.isStorageGroup()) {
      node =
          new StorageGroupMNode(
              parent, entityMNode.getName(), entityMNode.getAsStorageGroupMNode().getDataTTL());
    } else {
      node = new InternalMNode(parent, entityMNode.getName());
    }

    if (parent != null) {
      parent.replaceChild(entityMNode.getName(), node);
    }
    return node;
  }
}
