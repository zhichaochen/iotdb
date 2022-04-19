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

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainers;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;

import java.io.IOException;

/**
 * Mtree的内部节点，树的内部的节点，注意不是叶子节点，叶子节点上是物理量
 *
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class InternalMNode extends MNode {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * 当前节点的子节点
   * use in Measurement Node so it's protected suppress warnings reason: volatile for double
   * synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile IMNodeContainer children = null;

  // schema template
  protected Template schemaTemplate = null; // 元数据模板

  private volatile boolean useTemplate = false; // 是否使用模板

  /** Constructor of MNode. */
  public InternalMNode(IMNode parent, String name) {
    super(parent, name);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name));
  }

  /**
   * 通过名称获取子节点
   * get the child with the name */
  @Override
  public IMNode getChild(String name) {
    IMNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    return child;
  }

  /**
   * 添加子节点
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   * @return the child of this node after addChild
   */
  @Override
  public IMNode addChild(String name, IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    // 设置子节点，如果子节点集合不存在，则同步创建
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = MNodeContainers.getNewMNodeContainer();
        }
      }
    }
    // 设置当前节点，作为其父节点
    child.setParent(this);
    IMNode existingChild = children.putIfAbsent(name, child);
    return existingChild == null ? child : existingChild;
  }

  /**
   * Add a child to the current mnode.
   *
   * <p>This method will not take the child's name as one of the inputs and will also make this
   * Mnode be child node's parent. All is to reduce the probability of mistaken by users and be more
   * convenient for users to use. And the return of this method is used to conveniently construct a
   * chain of time series for users.
   *
   * @param child child's node
   * @return return the MNode already added
   */
  @Override
  public IMNode addChild(IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = MNodeContainers.getNewMNodeContainer();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public IMNode deleteChild(String name) {
    if (children != null) {
      return children.remove(name);
    }
    return null;
  }

  /**
   * Replace a child of this mnode. New child's name must be the same as old child's name.
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public synchronized void replaceChild(String oldChildName, IMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    IMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    oldChildNode.moveDataToNewMNode(newChildNode);

    children.replace(newChildNode.getName(), newChildNode);
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    super.moveDataToNewMNode(newMNode);

    newMNode.setSchemaTemplate(schemaTemplate);
    newMNode.setUseTemplate(useTemplate);

    if (children != null) {
      newMNode.setChildren(children);
      children.forEach((childName, childNode) -> childNode.setParent(newMNode));
    }
  }

  @Override
  public IMNodeContainer getChildren() {
    if (children == null) {
      return MNodeContainers.emptyMNodeContainer();
    }
    return children;
  }

  @Override
  public void setChildren(IMNodeContainer children) {
    this.children = children;
  }

  /**
   * 得到这个节点的上模板，记住我们得到了这个节点到根最近的模板
   * get upper template of this node, remember we get nearest template alone this node to root
   *
   * @return upper template
   */
  @Override
  public Template getUpperTemplate() {
    IMNode cur = this;
    while (cur != null) {
      if (cur.getSchemaTemplate() != null) {
        return cur.getSchemaTemplate();
      }
      cur = cur.getParent();
    }

    return null;
  }

  @Override
  public Template getSchemaTemplate() {
    return schemaTemplate;
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {
    this.schemaTemplate = schemaTemplate;
  }

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }

  /**
   * 序列化到logWriter，这里做的是WAL
   * 会将当前节点的所有子节点（包括子节点的子节点）都序列化到磁盘
   * @param logWriter
   * @throws IOException
   */
  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    // 首先序列化当前节点的子节点
    serializeChildren(logWriter);
    // 序列化当前节点
    logWriter.serializeMNode(this);
  }

  void serializeChildren(MLogWriter logWriter) throws IOException {
    if (children == null) {
      return;
    }
    // 这里会递归调用到serializeTo
    for (IMNode child : children.values()) {
      child.serializeTo(logWriter);
    }
  }

  public static InternalMNode deserializeFrom(MNodePlan plan) {
    return new InternalMNode(null, plan.getName());
  }
}
