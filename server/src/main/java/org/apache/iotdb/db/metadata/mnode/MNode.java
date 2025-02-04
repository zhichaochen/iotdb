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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 元数据节点
 * 表示在元数据结构中的一个节点，元数据以树形结构进行保存，每个元数据结构都是
 */
public abstract class MNode implements IMNode {

  /** Name of the MNode */
  protected String name; // 当前元数据节点名称

  protected IMNode parent; // 当前节点的父节点

  /**
   * 从根节点到该节点，仅当对InternalMNode使用一次时才能设置
   * from root to this node, only be set when used once for InternalMNode */
  protected String fullPath; // 全路径

  /** registered trigger */
  protected TriggerExecutor triggerExecutor;

  protected CacheEntry cacheEntry;

  /** Constructor of MNode. */
  public MNode(IMNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public IMNode getParent() {
    return parent;
  }

  @Override
  public void setParent(IMNode parent) {
    this.parent = parent;
  }

  /**
   * get partial path of this node
   *
   * @return partial path
   */
  @Override
  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    IMNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  /** get full path */
  @Override
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath();
    }
    return fullPath;
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    IMNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.getName());
    }
    return builder.toString();
  }

  @Override
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    newMNode.setParent(parent);
    newMNode.setCacheEntry(cacheEntry);
  }

  @Override
  public boolean isUseTemplate() {
    return false;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isEntity() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public IStorageGroupMNode getAsStorageGroupMNode() {
    if (isStorageGroup()) {
      return (IStorageGroupMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IEntityMNode getAsEntityMNode() {
    if (isEntity()) {
      return (IEntityMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IMeasurementMNode getAsMeasurementMNode() {
    if (isMeasurement()) {
      return (IMeasurementMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public List<TriggerExecutor> getUpperTriggerExecutorList() {
    IMNode currentNode = this;
    List<TriggerExecutor> results = new ArrayList<>();
    while (currentNode != null && !IoTDBConstant.PATH_ROOT.equals(currentNode.getName())) {
      TriggerExecutor executor = currentNode.getTriggerExecutor();
      currentNode = currentNode.getParent();
      if (executor == null) {
        continue;
      }
      results.add(executor);
    }
    return results;
  }

  @Override
  public TriggerExecutor getTriggerExecutor() {
    return triggerExecutor;
  }

  @Override
  public void setTriggerExecutor(TriggerExecutor triggerExecutor) {
    this.triggerExecutor = triggerExecutor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MNode mNode = (MNode) o;
    if (fullPath == null) {
      return Objects.equals(getFullPath(), mNode.getFullPath());
    } else {
      return Objects.equals(fullPath, mNode.getFullPath());
    }
  }

  @Override
  public int hashCode() {
    if (fullPath == null) {
      return Objects.hash(getFullPath());
    } else {
      return Objects.hash(fullPath);
    }
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public CacheEntry getCacheEntry() {
    return cacheEntry;
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {
    this.cacheEntry = cacheEntry;
  }
}
