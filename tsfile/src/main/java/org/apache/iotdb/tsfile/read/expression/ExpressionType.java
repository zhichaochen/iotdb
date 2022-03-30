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
package org.apache.iotdb.tsfile.read.expression;

/**
 * 表达式类型
 */
public enum ExpressionType {

  /**
   * 表示左表达式和右表达式之间的关系为和
   * Represent the relationship between the left expression and the right expression is AND */
  AND,

  /** Represent the relationship between the left expression and the right expression is OR */
  OR,

  /**
   * 表示表达式是表达式树中的叶节点，类型为value过滤
   * Represents that the expression is a leaf node in the expression tree and the type is value
   * filtering
   */
  SERIES,

  /**
   * 表示表达式是表达式树中的叶节点，类型为时间筛选
   * Represents that the expression is a leaf node in the expression tree and the type is time
   * filtering
   */
  GLOBAL_TIME,

  /**
   * 这种类型用于分布式读取过程中表达式树的修剪过程。在为数据组修剪表达式树时，属于其他数据组的叶节点将被设置为该类型。
   * This type is used in the pruning process of expression tree in the distributed reading process.
   * When pruning a expression tree for a data group, leaf nodes belonging to other data groups will
   * be set to that type.
   */
  TRUE
}
