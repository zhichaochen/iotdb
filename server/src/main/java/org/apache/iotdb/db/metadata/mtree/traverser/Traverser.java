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
package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

/**
 * 元数据节点树的遍历器
 * 这个类定义了主遍历框架，并声明了一些用于结果过程扩展的方法。这个类可以扩展以实现具体的任务。
 * 目前，任务分为两类：
 * 1。计数器：计算与路径模式
 * 2 匹配的节点数或测量数。收集器：收集匹配节点或测量的自定义结果
 * This class defines the main traversal framework and declares some methods for result process
 * extension. This class could be extended to implement concrete tasks. <br>
 * Currently, the tasks are classified into two type:
 *
 * <ol>
 *   <li>counter: to count the node num or measurement num that matches the path pattern
 *   <li>collector: to collect customized results of the matched node or measurement
 * </ol>
 */
public abstract class Traverser {

  protected IMTreeStore store;

  protected IMNode startNode;
  protected String[] nodes;
  protected int startIndex;
  protected int startLevel;
  protected boolean isPrefixStart = false;

  // to construct full path or find mounted node on MTree when traverse into template
  // 当遍历到模板中时，在MTree上构造完整路径或查找mount的节点
  protected Deque<IMNode> traverseContext; //

  protected boolean isInTemplate = false;

  // if isMeasurementTraverser, measurement in template should be processed
  protected boolean isMeasurementTraverser = false; //

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false; // 是否是前缀匹配

  /**
   * 创建一个遍历器
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  public Traverser(IMNode startNode, PartialPath path, IMTreeStore store) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(PATH_ROOT)) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
    this.store = store;
    this.traverseContext = new ArrayDeque<>();
    initStartIndexAndLevel(path);
  }

  /**
   * The traverser may start traversing from a storageGroupMNode, which is an InternalMNode of the
   * whole MTree.
   */
  private void initStartIndexAndLevel(PartialPath path) throws MetadataException {
    IMNode parent = startNode.getParent();
    Deque<IMNode> ancestors = new ArrayDeque<>();
    ancestors.push(startNode);

    startLevel = 0;
    while (parent != null) {
      startLevel++;
      traverseContext.addLast(parent);

      ancestors.push(parent);
      parent = parent.getParent();
    }

    IMNode cur;
    // given root.a.sg, accept path starting with prefix like root.a.sg, root.*.*, root.**,
    // root.a.**, which means the prefix matches the startNode's fullPath
    for (startIndex = 0; startIndex <= startLevel && startIndex < nodes.length; startIndex++) {
      cur = ancestors.pop();
      if (nodes[startIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return;
      } else if (!nodes[startIndex].equals(cur.getName())
          && !nodes[startIndex].contains(ONE_LEVEL_PATH_WILDCARD)) {
        throw new IllegalPathException(
            path.getFullPath(), path.getFullPath() + " doesn't start with " + cur.getFullPath());
      }
    }

    if (startIndex <= startLevel) {
      if (!nodes[startIndex - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        isPrefixStart = true;
      }
    } else {
      startIndex--;
    }
  }

  /**
   * 开始去遍历
   * The interface to start the traversal. The node process should be defined before traversal by
   * overriding or implement concerned methods.
   */
  public void traverse() throws MetadataException {
    if (isPrefixStart && !isPrefixMatch) {
      return;
    }
    traverse(startNode, startIndex, startLevel);
  }

  /**
   * MTree遍历的递归方法
   * The recursive method for MTree traversal. If the node matches nodes[idx], then do some
   * operation and traverse the children with nodes[idx+1].
   *
   * 比如：s1.g1.** ,那么
   * @param node current node that match the targetName in given path 在路径中当前要匹配的节点的名称
   * @param idx the index of targetName in given path 在给定路径中，当前节点所在的下标
   * @param level the level of current node in MTree 当前节点在Mtree中的层级
   * @throws MetadataException some result process may throw MetadataException
   */
  protected void traverse(IMNode node, int idx, int level) throws MetadataException {

    // 处理已匹配节点
    if (processMatchedMNode(node, idx, level)) {
      return;
    }

    // 说明
    if (idx >= nodes.length - 1) {
      // 如果路径的最后是**，或者前缀匹配，比如: s1.g1.**
      if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD) || isPrefixMatch) {
        // 处理多级通配符**
        processMultiLevelWildcard(node, idx, level);
      }
      return;
    }

    // 如果当前节点是物理量，则返回
    if (node.isMeasurement()) {
      return;
    }

    // 路径节点名称
    String targetName = nodes[idx + 1];
    // 如果是多级通配符
    if (MULTI_LEVEL_PATH_WILDCARD.equals(targetName)) {
      // 处理多级路径通配符
      processMultiLevelWildcard(node, idx, level);
    } else if (targetName.contains(ONE_LEVEL_PATH_WILDCARD)) {
      // 处理单级路径通配符
      processOneLevelWildcard(node, idx, level);
    } else {
      // 处理名称匹配
      processNameMatch(node, idx, level);
    }
  }

  /**
   * process curNode that matches the targetName during traversal. there are two cases: 1. internal
   * match: root.sg internal match root.sg.**(pattern) 2. full match: root.sg.d full match
   * root.sg.**(pattern) Both of them are default abstract and should be implemented according
   * concrete tasks.
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  private boolean processMatchedMNode(IMNode node, int idx, int level) throws MetadataException {
    if (idx < nodes.length - 1) {
      return processInternalMatchedMNode(node, idx, level);
    } else {
      return processFullMatchedMNode(node, idx, level);
    }
  }

  /**
   * internal match: root.sg internal match root.sg.**(pattern)
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  protected abstract boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

  /**
   * full match: root.sg.d full match root.sg.**(pattern)
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  protected abstract boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

  /**
   * 处理多级通配符**
   * @param node
   * @param idx
   * @param level
   * @throws MetadataException
   */
  protected void processMultiLevelWildcard(IMNode node, int idx, int level)
      throws MetadataException {
    if (isInTemplate) {
      traverseContext.push(node);
      for (IMNode child : node.getChildren().values()) {
        // 继续递归
        traverse(child, idx + 1, level + 1);
      }
      traverseContext.pop();
      return;
    }

    traverseContext.push(node);
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
          traverse(child, idx + 1, level + 1);
        } finally {
          store.unPin(child);
        }
      }
    } finally {
      iterator.close();
    }

    traverseContext.pop();

    // 如果当前节点没有使用模板，则返回
    if (!node.isUseTemplate()) {
      return;
    }

    // ?
    Template upperTemplate = node.getUpperTemplate();
    isInTemplate = true;
    traverseContext.push(node);
    for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
      traverse(childInTemplate, idx + 1, level + 1);
    }
    traverseContext.pop();
    isInTemplate = false;
  }

  /**
   * 处理一级通配符
   */
  protected void processOneLevelWildcard(IMNode node, int idx, int level) throws MetadataException {
    // 是否多级通配符
    boolean multiLevelWildcard = nodes[idx].equals(MULTI_LEVEL_PATH_WILDCARD);
    String targetNameRegex = nodes[idx + 1].replace("*", ".*");

    if (isInTemplate) {
      traverseContext.push(node);
      for (IMNode child : node.getChildren().values()) {
        if (!Pattern.matches(targetNameRegex, child.getName())) {
          continue;
        }
        traverse(child, idx + 1, level + 1);
      }
      traverseContext.pop();

      if (multiLevelWildcard) {
        traverseContext.push(node);
        for (IMNode child : node.getChildren().values()) {
          traverse(child, idx, level + 1);
        }
        traverseContext.pop();
      }
      return;
    }

    traverseContext.push(node);
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
          if (child.isMeasurement()) {
            String alias = child.getAsMeasurementMNode().getAlias();
            if (!Pattern.matches(targetNameRegex, child.getName())
                && !(alias != null && Pattern.matches(targetNameRegex, alias))) {
              continue;
            }
          } else {
            if (!Pattern.matches(targetNameRegex, child.getName())) {
              continue;
            }
          }
          traverse(child, idx + 1, level + 1);
        } finally {
          store.unPin(child);
        }
      }
    } finally {
      iterator.close();
    }

    traverseContext.pop();

    if (multiLevelWildcard) {
      traverseContext.push(node);
      iterator = store.getChildrenIterator(node);
      try {
        while (iterator.hasNext()) {
          child = iterator.next();
          try {
            traverse(child, idx, level + 1);
          } finally {
            store.unPin(child);
          }
        }
      } finally {
        iterator.close();
      }
      traverseContext.pop();
    }

    // 以下是处理模板
    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();

    isInTemplate = true;
    traverseContext.push(node);
    for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
      if (!Pattern.matches(targetNameRegex, childInTemplate.getName())) {
        continue;
      }
      traverse(childInTemplate, idx + 1, level + 1);
    }
    traverseContext.pop();

    if (multiLevelWildcard) {
      traverseContext.push(node);
      for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
        traverse(childInTemplate, idx, level + 1);
      }
      traverseContext.pop();
    }
    isInTemplate = false;
  }

  /**
   * 处理名称匹配
   */
  @SuppressWarnings("Duplicates")
  protected void processNameMatch(IMNode node, int idx, int level) throws MetadataException {
    // 是否是多级匹配
    boolean multiLevelWildcard = nodes[idx].equals(MULTI_LEVEL_PATH_WILDCARD);
    // 下一个节点的名称
    String targetName = nodes[idx + 1];

    if (isInTemplate) {
      IMNode targetNode = node.getChild(targetName);
      if (targetNode != null) {
        traverseContext.push(node);
        traverse(targetNode, idx + 1, level + 1);
        traverseContext.pop();
      }

      if (multiLevelWildcard) {
        traverseContext.push(node);
        for (IMNode child : node.getChildren().values()) {
          traverse(child, idx, level + 1);
        }
        traverseContext.pop();
      }
      return;
    }

    IMNode next = store.getChild(node, targetName);
    if (next != null) {
      try {
        traverseContext.push(node);
        traverse(next, idx + 1, level + 1);
        traverseContext.pop();
      } finally {
        store.unPin(next);
      }
    }

    if (multiLevelWildcard) {
      traverseContext.push(node);
      IMNode child;
      IMNodeIterator iterator = store.getChildrenIterator(node);
      try {
        while (iterator.hasNext()) {
          child = iterator.next();
          try {
            traverse(child, idx, level + 1);
          } finally {
            store.unPin(child);
          }
        }
      } finally {
        iterator.close();
      }
      traverseContext.pop();
    }

    // 如果没有使用模板则停止本地调用
    if (!node.isUseTemplate()) {
      return;
    }

    // 否则查找模板节点
    Template upperTemplate = node.getUpperTemplate();
    isInTemplate = true;
    IMNode targetNode = upperTemplate.getDirectNode(targetName);
    if (targetNode != null) {
      traverseContext.push(node);
      traverse(targetNode, idx + 1, level + 1);
      traverseContext.pop();
    }

    if (multiLevelWildcard) {
      traverseContext.push(node);
      for (IMNode child : upperTemplate.getDirectNodes()) {
        traverse(child, idx, level + 1);
      }
      traverseContext.pop();
    }
    isInTemplate = false;
  }

  public void setPrefixMatch(boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
  }

  /**
   * @param currentNode the node need to get the full path of
   * @return full path from traverse start node to the current node
   */
  protected PartialPath getCurrentPartialPath(IMNode currentNode) {
    return new PartialPath(getCurrentPathNodes(currentNode));
  }

  protected String[] getCurrentPathNodes(IMNode currentNode) {
    Iterator<IMNode> nodes = traverseContext.descendingIterator();
    List<String> nodeNames = new LinkedList<>();
    if (nodes.hasNext()) {
      nodeNames.addAll(Arrays.asList(nodes.next().getPartialPath().getNodes()));
    }

    while (nodes.hasNext()) {
      nodeNames.add(nodes.next().getName());
    }

    nodeNames.add(currentNode.getName());

    return nodeNames.toArray(new String[0]);
  }

  /** @return the storage group node in the traverse path */
  protected IMNode getStorageGroupNodeInTraversePath(IMNode currentNode) {
    if (currentNode.isStorageGroup()) {
      return currentNode;
    }
    Iterator<IMNode> nodes = traverseContext.iterator();
    while (nodes.hasNext()) {
      IMNode node = nodes.next();
      if (node.isStorageGroup()) {
        return node;
      }
    }
    return null;
  }
}
