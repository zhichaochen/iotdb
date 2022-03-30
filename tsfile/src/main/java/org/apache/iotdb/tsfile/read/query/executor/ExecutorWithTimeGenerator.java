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
package org.apache.iotdb.tsfile.read.query.executor;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * 带有时间生成器的执行器
 */
public class ExecutorWithTimeGenerator implements QueryExecutor {

  private IMetadataQuerier metadataQuerier; // 元数据查询器
  private IChunkLoader chunkLoader; // chunk加载器

  public ExecutorWithTimeGenerator(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  /**
   * queryExpression中queryFilter的所有叶节点都是序列过滤器，我们使用时间生成器来控制查询处理。有关更多信息，请参阅DataSetWithTimeGenerator
   * All leaf nodes of queryFilter in queryExpression are SeriesFilters, We use a TimeGenerator to
   * control query processing. for more information, see DataSetWithTimeGenerator
   *
   * @return DataSet with TimeGenerator
   */
  @Override
  public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException {

    // 表达式
    IExpression expression = queryExpression.getExpression();
    // 选择的时间序列
    List<Path> selectedPathList = queryExpression.getSelectedSeries();

    // get TimeGenerator by IExpression
    // 时间生成器
    TimeGenerator timeGenerator = new TsFileTimeGenerator(expression, chunkLoader, metadataQuerier);

    // the size of hasFilter is equal to selectedPathList, if a series has a filter, it is true,
    // otherwise false
    // 和selectedPathList的size是相同的，如果一个序列有过滤器则为true，否则为false
    // TODO 其实表示对应的序列是否有过滤器
    List<Boolean> cached =
        markFilterdPaths(expression, selectedPathList, timeGenerator.hasOrNode());
    // 选择的序列读取器
    List<FileSeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    Iterator<Boolean> cachedIterator = cached.iterator();
    Iterator<Path> selectedPathIterator = selectedPathList.iterator();
    // 遍历
    while (cachedIterator.hasNext()) {
      boolean cachedValue = cachedIterator.next();
      Path selectedPath = selectedPathIterator.next();

      // 通过路径查询所有的chunk元数据
      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(selectedPath);
      if (chunkMetadataList.size() != 0) {
        dataTypes.add(chunkMetadataList.get(0).getDataType());
        if (cachedValue) {
          readersOfSelectedSeries.add(null);
          continue;
        }
        FileSeriesReaderByTimestamp seriesReader =
            new FileSeriesReaderByTimestamp(chunkLoader, chunkMetadataList);
        readersOfSelectedSeries.add(seriesReader);
      } else {
        selectedPathIterator.remove();
        cachedIterator.remove();
      }
    }

    //
    return new DataSetWithTimeGenerator(
        selectedPathList, cached, dataTypes, timeGenerator, readersOfSelectedSeries);
  }

  public static List<Boolean> markFilterdPaths(
      IExpression expression, List<Path> selectedPaths, boolean hasOrNode) {
    List<Boolean> cached = new ArrayList<>();
    // 是否有or节点
    if (hasOrNode) {
      //
      for (Path ignored : selectedPaths) {
        cached.add(false);
      }
      return cached;
    }

    // 包含过滤条件的路径
    HashSet<Path> filteredPaths = new HashSet<>();
    // 获取所有有过滤条件的时间序列
    getAllFilteredPaths(expression, filteredPaths);
    // 将过滤路径加入缓存
    for (Path selectedPath : selectedPaths) {
      cached.add(filteredPaths.contains(selectedPath));
    }

    return cached;
  }

  private static void getAllFilteredPaths(IExpression expression, HashSet<Path> paths) {
    if (expression instanceof BinaryExpression) {
      getAllFilteredPaths(((BinaryExpression) expression).getLeft(), paths);
      getAllFilteredPaths(((BinaryExpression) expression).getRight(), paths);
    } else if (expression instanceof SingleSeriesExpression) {
      paths.add(((SingleSeriesExpression) expression).getSeriesPath());
    }
  }
}
