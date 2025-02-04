/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AccumulatorTest {

  private TsBlock rawData;
  private Statistics statistics;
  private TimeRange defaultTimeRange = new TimeRange(0, Long.MAX_VALUE);

  @Before
  public void setUp() {
    initInputTsBlock();
  }

  private void initInputTsBlock() {
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE);
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypes);
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = 0; i < 100; i++) {
      timeColumnBuilder.writeLong(i);
      columnBuilders[0].writeDouble(i * 1.0);
      tsBlockBuilder.declarePosition();
    }
    rawData = tsBlockBuilder.build();

    statistics = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics.update(100L, 100d);
  }

  @Test
  public void avgAccumulatorTest() {
    Accumulator avgAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.AVG, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, avgAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getFinalType());

    avgAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(avgAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    intermediateResult[1] = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(100, intermediateResult[0].build().getLong(0));
    Assert.assertEquals(4950d, intermediateResult[1].build().getDouble(0), 0.001);

    // add intermediate result as input
    avgAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(49.5d, finalResult.build().getDouble(0), 0.001);

    avgAccumulator.reset();
    avgAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void countAccumulatorTest() {
    Accumulator countAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.COUNT, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getFinalType());

    countAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(countAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(100, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    countAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(200, finalResult.build().getLong(0));

    countAccumulator.reset();
    countAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1, finalResult.build().getLong(0));
  }

  @Test
  public void extremeAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.EXTREME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());

    extremeAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void firstValueAccumulatorTest() {
    Accumulator firstValueAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.FIRST_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, firstValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getFinalType());

    firstValueAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertTrue(firstValueAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    firstValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(0L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    firstValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0L, finalResult.build().getDouble(0), 0.001);

    firstValueAccumulator.reset();
    firstValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void lastValueAccumulatorTest() {
    Accumulator lastValueAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.LAST_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, lastValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getFinalType());

    lastValueAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    lastValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(99L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    lastValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99L, finalResult.build().getDouble(0), 0.001);

    lastValueAccumulator.reset();
    lastValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void maxTimeAccumulatorTest() {
    Accumulator maxTimeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MAX_TIME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getFinalType());

    maxTimeAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(maxTimeAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    maxTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99, finalResult.build().getLong(0));

    maxTimeAccumulator.reset();
    maxTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100, finalResult.build().getLong(0));
  }

  @Test
  public void minTimeAccumulatorTest() {
    Accumulator minTimeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MIN_TIME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getFinalType());

    minTimeAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertTrue(minTimeAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    minTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    minTimeAccumulator.reset();
    minTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100, finalResult.build().getLong(0));
  }

  @Test
  public void maxValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MAX_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());

    extremeAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void minValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MIN_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());

    extremeAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void sumAccumulatorTest() {
    Accumulator sumAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.SUM, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getFinalType());

    sumAccumulator.addInput(rawData.getTimeAndValueColumn(0), defaultTimeRange);
    Assert.assertFalse(sumAccumulator.hasFinalResult());
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(4950d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    sumAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(9900d, finalResult.build().getDouble(0), 0.001);

    sumAccumulator.reset();
    sumAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }
}
