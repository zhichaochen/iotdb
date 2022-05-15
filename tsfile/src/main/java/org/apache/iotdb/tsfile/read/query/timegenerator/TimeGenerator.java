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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 时间生成器，用来生成满足过滤条件的下一个时间戳
 *
 * TODO 为啥需要时间生成器？？？
 *  如果有过滤条件，那么我们肯定要查询符合条件的数据，那么过滤之后取那个时间呢，由时间生成器提供。
 *
 * TODO 查询步骤是怎样的呢？
 *  1、通过表达式构建节点树，叶子节点都是数据读取器，比如：FileSeriesReader
 *  1、获取我们需要的时间戳
 *  2、通过FileSeriesReaderByTimestamp,通过时间戳查询目标数据。
 *
 * IExpression中涉及的所有SingleSeriesExpression将被传输到一个TimeGenerator树，该树的叶节点都是SeriesReader，
 * TimeGenerator树可以生成满足过滤条件的下一个时间戳。然后，我们使用这个时间戳来获取IExpression中未包含的其他系列中的值
 *
 * All SingleSeriesExpression involved in a IExpression will be transferred to a TimeGenerator tree
 * whose leaf nodes are all SeriesReaders, The TimeGenerator tree can generate the next timestamp
 * that satisfies the filter condition. Then we use this timestamp to get values in other series
 * that are not included in IExpression
 */
public abstract class TimeGenerator {

  // 路径和叶结点的映射，一个时间序列可能有多个过滤条件，那么会有多个叶结点
  private HashMap<Path, List<LeafNode>> leafNodeCache = new HashMap<>();

  private HashMap<Path, List<Object>> leafValuesCache;
  // 这里主要通过该对象获取下一个时间
  protected Node operatorNode; // 表示通过表达式构建的过滤器节点
  // 是否有or节点
  private boolean hasOrNode;

  public boolean hasNext() throws IOException {
    return operatorNode.hasNext();
  }

  /**
   * 下一个时间
   * @return
   * @throws IOException
   */
  public long next() throws IOException {
    // 是否有OR节点
    if (!hasOrNode) {
      // 叶子值缓存
      if (leafValuesCache == null) {
        leafValuesCache = new HashMap<>();
      }
      //
      leafNodeCache.forEach(
          (path, nodes) ->
              leafValuesCache
                  .computeIfAbsent(path, k -> new ArrayList<>())
                  .add(nodes.get(0).currentValue()));
    }
    // 下一个时间戳
    return operatorNode.next();
  }

  /** ATTENTION: this method should only be used when there is no `OR` node */
  public Object[] getValues(Path path) throws IOException {
    if (hasOrNode) {
      throw new IOException(
          "getValues() method should not be invoked when there is OR operator in where clause");
    }
    if (leafValuesCache.get(path) == null) {
      throw new IOException(
          "getValues() method should not be invoked by non-existent path in where clause");
    }
    return leafValuesCache.remove(path).toArray();
  }

  /** ATTENTION: this method should only be used when there is no `OR` node */
  public Object getValue(Path path) throws IOException {
    if (hasOrNode) {
      throw new IOException(
          "getValue() method should not be invoked when there is OR operator in where clause");
    }
    if (leafValuesCache.get(path) == null) {
      throw new IOException(
          "getValue() method should not be invoked by non-existent path in where clause");
    }
    return leafValuesCache.get(path).remove(0);
  }

  /**
   * 构造节点
   * @param expression
   * @throws IOException
   */
  public void constructNode(IExpression expression) throws IOException {
    operatorNode = construct(expression);
  }

  /**
   * 构造节点树，叶子节点都是
   * 构造生成时间戳的树
   * construct the tree that generate timestamp. */
  protected Node construct(IExpression expression) throws IOException {
    // 单序列表达式，一元表达式
    if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
      // 生成批量数据读取器，比如:TsFileTimeGenerator会生成FileSeriesReader
      IBatchReader seriesReader = generateNewBatchReader(singleSeriesExp);
      Path path = singleSeriesExp.getSeriesPath();

      // put the current reader to valueCache
      // 创建叶子节点，叶子节点是数据读取器
      LeafNode leafNode = new LeafNode(seriesReader);
      // 将当前读取器加入叶子缓存
      leafNodeCache.computeIfAbsent(path, p -> new ArrayList<>()).add(leafNode);

      return leafNode;
    }
    // 二元表达式
    else {
      // 构造当前表达式左边的节点
      Node leftChild = construct(((IBinaryExpression) expression).getLeft());
      // 构造当前表达式右边的节点
      Node rightChild = construct(((IBinaryExpression) expression).getRight());

      // Or节点
      if (expression.getType() == ExpressionType.OR) {
        hasOrNode = true;
        return new OrNode(leftChild, rightChild, isAscending());
      }
      // And节点
      else if (expression.getType() == ExpressionType.AND) {
        return new AndNode(leftChild, rightChild, isAscending());
      }
      throw new UnSupportedDataTypeException(
          "Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
    }
  }

  protected abstract IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException;

  public boolean hasOrNode() {
    return hasOrNode;
  }

  protected abstract boolean isAscending();

  public abstract Filter getTimeFilter();
}
