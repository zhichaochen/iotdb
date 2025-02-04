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

package org.apache.iotdb.cluster.log;

/**
 * 日志申请器
 * 日志申请器申请log到本地节点，使本地节点是有效的。
 * LogApplier applies the log to the local node to make it take effect. */
public interface LogApplier {

  /**
   * Apply the given log, if any exception is thrown during the execution, the exception will be
   * recorded in the log. Either an exception is thrown or the log is executed successfully, log
   * .setApplied(true) must be called.
   *
   * @param log
   */
  void apply(Log log);

  default void close() {}
}
