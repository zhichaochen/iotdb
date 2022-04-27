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
package org.apache.iotdb.cluster;

import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * IoTDB服务命令行
 */
public class ClusterIoTDBServerCommandLine extends ServerCommandLine {
  private static final Logger logger = LoggerFactory.getLogger(ClusterIoTDBServerCommandLine.class);

  // establish the cluster as a seed 将集群建立为种子
  private static final String MODE_START = "-s";
  // join an established cluster 加入一个已建立的集群
  private static final String MODE_ADD = "-a";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node // 移除一个节点
  private static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-a|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node as a seed\n"
          + "-a: start the node as a new node\n"
          + "-r: remove the node out of the cluster\n";

  @Override
  protected String getUsage() {
    return USAGE;
  }

  /**
   * main方法运行集群
   * @param args system args
   * @return
   */
  @Override
  protected int run(String[] args) {
    if (args.length < 1) {
      usage(null);
      return -1;
    }

    ClusterIoTDB cluster = ClusterIoTDB.getInstance();
    // check config of iotdb,and set some configs in cluster mode
    // 检查iotdb的配置，并在集群模式下设置一些配置
    try {
      // 检查配置并初始化
      if (!cluster.serverCheckAndInit()) {
        return -1;
      }
    } catch (ConfigurationException | IOException e) {
      logger.error("meet error when doing start checking", e);
      return -1;
    }
    // 模式
    String mode = args[0];
    logger.info("Running mode {}", mode);

    // initialize the current node and its services
    // TODO 初始化当前节点和他的服务
    if (!cluster.initLocalEngines()) {
      logger.error("initLocalEngines error, stop process!");
      return -1;
    }

    // we start IoTDB kernel first. then we start the cluster module.
    // 我们首先启动IoTDB内核。然后我们启动集群模块。
    if (MODE_START.equals(mode)) {
      // 启动集群，也就是启动当前节点，相比添加
      cluster.activeStartNodeMode();
    } else if (MODE_ADD.equals(mode)) {
      // 向集群添加一个节点，也就是让当前节点知道有一个节点加入集群了
      cluster.activeAddNodeMode();
    } else if (MODE_REMOVE.equals(mode)) {
      try {
        // 移除一个节点
        cluster.doRemoveNode(args);
      } catch (IOException e) {
        logger.error("Fail to remove node in cluster", e);
      }
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
    return 0;
  }
}
