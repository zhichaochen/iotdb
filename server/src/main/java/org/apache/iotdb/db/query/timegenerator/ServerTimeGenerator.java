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
package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 服务端时间生成器（适用于从存储引擎过来的服务）
 *
 * 带有过滤器的查询时间戳生成器, 例如查询字句select s1, s2 from root where s3 < 0 and time > 100"，该类可以迭代回查询的每个时间戳。
 * A timestamp generator for query with filter. e.g. For query clause "select s1, s2 from root where
 * s3 < 0 and time > 100", this class can iterate back to every timestamp of the query.
 */
public class ServerTimeGenerator extends TimeGenerator {

  private static final Logger logger = LoggerFactory.getLogger(ServerTimeGenerator.class);

  protected QueryContext context; // 查询上下文
  protected RawDataQueryPlan queryPlan; // 查询计划

  private Filter timeFilter; // 时间过滤器

  public ServerTimeGenerator(QueryContext context) {
    this.context = context;
  }

  /**
   * 创建时间生成器引擎
   * Constructor of EngineTimeGenerator. */
  public ServerTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    this.context = context;
    this.queryPlan = queryPlan;
    try {
      // 构造时间戳树节点
      serverConstructNode(queryPlan.getExpression());
    } catch (IOException | QueryProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * 通过表达式构造及诶按
   * @param expression
   * @throws IOException
   * @throws StorageEngineException
   * @throws QueryProcessException
   */
  public void serverConstructNode(IExpression expression)
      throws IOException, StorageEngineException, QueryProcessException {
    // 路径列表
    List<PartialPath> pathList = new ArrayList<>();
    //
    timeFilter = getPathListAndConstructTimeFilterFromExpression(expression, pathList);

    Pair<List<DataRegion>, Map<DataRegion, List<PartialPath>>> lockListAndProcessorToSeriesMapPair =
        StorageEngine.getInstance().mergeLock(pathList);
    List<DataRegion> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<DataRegion, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // 初始化QueryDataSource缓存
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }
    // 构建生成时间戳的树
    operatorNode = construct(expression);
  }

  /**
   * 从表达式中收集PartialPath，并将isUnderAlignedEntity为true的MeasurementPath转换为AlignedPath
   * collect PartialPath from Expression and transform MeasurementPath whose isUnderAlignedEntity is
   * true to AlignedPath
   */
  private Filter getPathListAndConstructTimeFilterFromExpression(
      IExpression expression, List<PartialPath> pathList) {
    if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression seriesExpression = (SingleSeriesExpression) expression;
      MeasurementPath measurementPath = (MeasurementPath) seriesExpression.getSeriesPath();
      // change the MeasurementPath to AlignedPath if the MeasurementPath's isUnderAlignedEntity ==
      // true
      seriesExpression.setSeriesPath(measurementPath.transformToExactPath());
      pathList.add((PartialPath) seriesExpression.getSeriesPath());
      return getTimeFilter(((SingleSeriesExpression) expression).getFilter());
    } else {
      Filter leftTimeFilter =
          getTimeFilter(
              getPathListAndConstructTimeFilterFromExpression(
                  ((IBinaryExpression) expression).getLeft(), pathList));
      Filter rightTimeFilter =
          getTimeFilter(
              getPathListAndConstructTimeFilterFromExpression(
                  ((IBinaryExpression) expression).getRight(), pathList));

      if (expression instanceof AndFilter) {
        if (leftTimeFilter != null && rightTimeFilter != null) {
          return FilterFactory.and(leftTimeFilter, rightTimeFilter);
        } else if (leftTimeFilter != null) {
          return leftTimeFilter;
        } else return rightTimeFilter;
      } else {
        if (leftTimeFilter != null && rightTimeFilter != null) {
          return FilterFactory.or(leftTimeFilter, rightTimeFilter);
        } else {
          return null;
        }
      }
    }
  }

  /**
   * 生成新的批量读取器
   * @param expression
   * @return
   * @throws IOException
   */
  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter valueFilter = expression.getFilter();
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType = path.getSeriesType();
    // 查询数据源
    QueryDataSource queryDataSource;
    try {
      queryDataSource =
          QueryResourceManager.getInstance()
              .getQueryDataSource(path, context, valueFilter, queryPlan.isAscending());
      // update valueFilter by TTL
      // 用存活时间去更新过滤器
      valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
    } catch (Exception e) {
      throw new IOException(e);
    }

    // get the TimeFilter part in SingleSeriesExpression
    // 从SingleSeriesExpression中获取时间过滤器
    Filter timeFilter = getTimeFilter(valueFilter);

    // 创建原生数据批量读取器
    return new SeriesRawDataBatchReader(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        null,
        queryPlan.isAscending());
  }

  /**
   * 从一个值过滤器中提取时间过滤器
   * extract time filter from a value filter */
  protected Filter getTimeFilter(Filter filter) {
    // 如果是一元过滤器，且是时间过滤器
    if (filter instanceof UnaryFilter
        && ((UnaryFilter) filter).getFilterType() == FilterType.TIME_FILTER) {
      return filter;
    }
    // and 过滤器
    if (filter instanceof AndFilter) {
      // 左边的时间过滤器
      Filter leftTimeFilter = getTimeFilter(((AndFilter) filter).getLeft());
      // 右边的时间过滤器
      Filter rightTimeFilter = getTimeFilter(((AndFilter) filter).getRight());
      //
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return filter;
      } else if (leftTimeFilter != null) {
        return leftTimeFilter;
      } else {
        return rightTimeFilter;
      }
    }
    return null;
  }

  @Override
  protected boolean isAscending() {
    return queryPlan.isAscending();
  }

  @Override
  public Filter getTimeFilter() {
    return timeFilter;
  }
}
