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
package org.apache.iotdb.tsfile.read.query.dataset;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

/**
 * 查询数据集，依赖TimeGenerator生成时间戳
 * TODO 适用于什么什么情况
 *  包含值过滤条件的查询（注意：也可能会包含时间）
 *
 *  * TODO 为啥需要时间生成器？？？
 *  *  如果有过滤条件，那么我们肯定要查询符合条件的数据，那么过滤之后取那个时间呢，由时间生成器提供。
 *  *
 *  * TODO 查询步骤是怎样的呢？
 *  *  1、通过表达式构建节点树，叶子节点都是数据读取器，比如：FileSeriesReader
 *  *  1、获取我们需要的时间戳
 *  *  2、通过FileSeriesReaderByTimestamp,通过时间戳查询目标数据。
 *
 * DataSetWithTimeGenerator是一个QueryDataSet数据类型
 * 查询处理：（1）按具有筛选器的序列生成时间（2）获取不具有筛选器的序列的值（3）构造行记录。
 * query processing: (1) generate time by series that has filter (2) get value of series that does
 * not have filter (3) construct RowRecord.
 */
public class DataSetWithTimeGenerator extends QueryDataSet {
  // TODO 注意：这些列表都是与时间序列的列表一一对应的。
  private TimeGenerator timeGenerator; // 时间生成器（TsFileTimeGenerator）
  private List<FileSeriesReaderByTimestamp> readers; // 时间序列读取器列表;
  private List<Boolean> cached; // 时间序列是否有过滤器

  /**
   * constructor of DataSetWithTimeGenerator.
   *
   * @param paths paths in List structure
   * @param cached cached boolean in List(boolean) structure
   * @param dataTypes TSDataTypes in List structure
   * @param timeGenerator TimeGenerator object
   * @param readers readers in List(FileSeriesReaderByTimestamp) structure
   */
  public DataSetWithTimeGenerator(
      List<Path> paths,
      List<Boolean> cached,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<FileSeriesReaderByTimestamp> readers) {
    super(paths, dataTypes);
    this.cached = cached;
    this.timeGenerator = timeGenerator;
    this.readers = readers;
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    return timeGenerator.hasNext();
  }

  /**
   * 没有约束地获取一行记录
   * @return
   * @throws IOException
   */
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    // TODO 通过时间生成器生成一个时间戳
    long timestamp = timeGenerator.next();
    // 创建一行数据
    RowRecord rowRecord = new RowRecord(timestamp);

    // 遍历所有时间序列
    for (int i = 0; i < paths.size(); i++) {
      // get value from readers in time generator
      // 如果有过滤条件，则通过时间生成器获取值
      if (cached.get(i)) {
        Object value = timeGenerator.getValue(paths.get(i));
        if (dataTypes.get(i) == TSDataType.VECTOR) {
          TsPrimitiveType v = ((TsPrimitiveType[]) value)[0];
          rowRecord.addField(v.getValue(), v.getDataType());
        } else {
          rowRecord.addField(value, dataTypes.get(i));
        }
        continue;
      }

      // get value from series reader without filter
      //  如果没有过滤条件，则使用FileSeriesReaderByTimestamp查找时间戳对应的值
      FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp = readers.get(i);
      // 通过时间戳获取值
      Object value = fileSeriesReaderByTimestamp.getValueInTimestamp(timestamp);
      // 如果返回多个值，则取第一个
      if (dataTypes.get(i) == TSDataType.VECTOR) {
        TsPrimitiveType v = ((TsPrimitiveType[]) value)[0];
        rowRecord.addField(v.getValue(), v.getDataType());
      } else {
        rowRecord.addField(value, dataTypes.get(i));
      }
    }

    return rowRecord;
  }
}
