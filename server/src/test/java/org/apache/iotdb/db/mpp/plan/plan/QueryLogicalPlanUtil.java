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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** This class generates logical plans for test cases of the query statements. */
public class QueryLogicalPlanUtil {

  // test cases of query statement
  public static final List<String> querySQLs = new ArrayList<>();

  // key: query statement; value: expected logical plan
  public static final Map<String, PlanNode> sqlToPlanMap = new HashMap<>();

  public static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d1.s3", new MeasurementPath("root.sg.d1.s3", TSDataType.BOOLEAN));
      schemaMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d2.s4", new MeasurementPath("root.sg.d2.s4", TSDataType.TEXT));

      MeasurementPath aS1 = new MeasurementPath("root.sg.d2.a.s1", TSDataType.INT32);
      MeasurementPath aS2 = new MeasurementPath("root.sg.d2.a.s2", TSDataType.DOUBLE);
      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s1", "s2"),
              Arrays.asList(aS1.getMeasurementSchema(), aS2.getMeasurementSchema()));
      aS1.setUnderAlignedEntity(true);
      aS2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s1", aS1);
      schemaMap.put("root.sg.d2.a.s2", aS2);
      schemaMap.put("root.sg.d2.a", alignedPath);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  /* Simple Query */
  static {
    String sql = "SELECT ** FROM root.sg.d2 LIMIT 10 OFFSET 10";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s4"),
            OrderBy.TIMESTAMP_ASC));
    sourceNodeList.add(
        new AlignedSeriesScanNode(
            new PlanNodeId("3"),
            (AlignedPath) schemaMap.get("root.sg.d2.a"),
            OrderBy.TIMESTAMP_ASC));
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("4"), OrderBy.TIMESTAMP_ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("5"), timeJoinNode, 10);
    LimitNode limitNode = new LimitNode(new PlanNodeId("6"), offsetNode, 10);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Raw Data Query */
  static {
    String sql =
        "SELECT s1 FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 SLIMIT 1 SOFFSET 1";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("3"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression predicate =
        new LogicAndExpression(timeFilter, new LogicAndExpression(valueFilter1, valueFilter2));

    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("4"),
            timeJoinNode,
            new Expression[] {new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))},
            predicate,
            false,
            ZonedDateTime.now().getOffset());

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("5"),
            filterNode,
            FilterNullPolicy.CONTAINS_NULL,
            Collections.singletonList(new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))));

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("6"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("7"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Raw Data Query (align by device) */
  static {
    String sql =
        "SELECT * FROM root.sg.* WHERE time > 100 and s1 > 10 "
            + "ORDER BY TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s3"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("3"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
            new ConstantOperand(TSDataType.INT64, "10"));
    LogicAndExpression predicate1 = new LogicAndExpression(timeFilter, valueFilter1);

    FilterNode filterNode1 =
        new FilterNode(
            new PlanNodeId("4"),
            timeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s3")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))
            },
            predicate1,
            false,
            ZonedDateTime.now().getOffset());

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("5"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s4"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("6"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("7"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("8"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression predicate2 = new LogicAndExpression(timeFilter, valueFilter2);

    FilterNode filterNode2 =
        new FilterNode(
            new PlanNodeId("9"),
            timeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s4")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))
            },
            predicate2,
            false,
            ZonedDateTime.now().getOffset());

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 3, 4));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(2, 3, 4));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("10"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList(HeaderConstant.COLUMN_DEVICE, "s3", "s4", "s1", "s2"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", filterNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", filterNode2);

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("11"), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("12"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Simple Aggregation Query */
  static {
    String sql =
        "SELECT last_value(s1), first_value(s1), sum(s2) FROM root.sg.** WHERE time > 100 LIMIT 10 OFFSET 10";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.FIRST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.SUM,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("3"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.FIRST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("4"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.SUM,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("5"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new AlignedSeriesAggregationScanNode(
            new PlanNodeId("6"),
            (AlignedPath) schemaMap.get("root.sg.d2.a"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.FIRST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    AggregationType.SUM,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.add(
        new AlignedSeriesAggregationScanNode(
            new PlanNodeId("7"),
            new AlignedPath((MeasurementPath) schemaMap.get("root.sg.d2.a.s1")),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))))),
            OrderBy.TIMESTAMP_ASC,
            null));
    sourceNodeList.forEach(
        node -> {
          if (node instanceof SeriesAggregationScanNode) {
            ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          } else {
            ((AlignedSeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          }
        });

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("8"), OrderBy.TIMESTAMP_ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("9"), timeJoinNode, 10);
    LimitNode limitNode = new LimitNode(new PlanNodeId("10"), offsetNode, 10);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (without value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.** WHERE time > 100 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("3"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList.add(
        new AlignedSeriesAggregationScanNode(
            new PlanNodeId("4"),
            (AlignedPath) schemaMap.get("root.sg.d2.a"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList.forEach(
        node -> {
          if (node instanceof SeriesAggregationScanNode) {
            ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          } else {
            ((AlignedSeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          }
        });

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("5"),
            sourceNodeList,
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.COUNT,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                        new FunctionExpression(
                            SQLConstant.COUNT,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))))),
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new FunctionExpression(
                            SQLConstant.COUNT,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.MAX_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                        new FunctionExpression(
                            SQLConstant.MAX_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new FunctionExpression(
                            SQLConstant.MAX_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2")))))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.LAST_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                        new FunctionExpression(
                            SQLConstant.LAST_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new FunctionExpression(
                            SQLConstant.LAST_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))))))),
            Arrays.asList(
                "count(root.sg.*.s1)",
                "count(root.sg.*.*.s1)",
                "max_value(root.sg.*.s2)",
                "max_value(root.sg.*.*.s2)",
                "last_value(root.sg.*.s1)",
                "last_value(root.sg.*.*.s1)"));

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("6"), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("7"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (without value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "ORDER BY TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    Filter timeFilter = TimeFilter.gt(100);
    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList1.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("2"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("3"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("4"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            OrderBy.TIMESTAMP_DESC,
            null));
    sourceNodeList2.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(1, 2, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("6"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList(
                HeaderConstant.COLUMN_DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", timeJoinNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", timeJoinNode2);

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("7"), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("8"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (with value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("2"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("3"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("4"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression predicate =
        new LogicAndExpression(timeFilter, new LogicAndExpression(valueFilter1, valueFilter2));
    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("5"),
            timeJoinNode,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate,
            false,
            ZonedDateTime.now().getOffset());

    AggregationNode aggregationNode =
        new AggregationNode(
            new PlanNodeId("6"),
            Collections.singletonList(filterNode),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))));

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("7"),
            Collections.singletonList(aggregationNode),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.COUNT,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                        new FunctionExpression(
                            SQLConstant.COUNT,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.MAX_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                        new FunctionExpression(
                            SQLConstant.MAX_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new FunctionExpression(
                            SQLConstant.LAST_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                        new FunctionExpression(
                            SQLConstant.LAST_VALUE,
                            new LinkedHashMap<>(),
                            Collections.singletonList(
                                new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))))),
            Arrays.asList(
                "count(root.sg.*.s1)", "max_value(root.sg.*.s2)", "last_value(root.sg.*.s1)"));

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("8"), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("9"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (with value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.forEach(
        planNode -> {
          ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100));
        });

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("2"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT64, "10"));
    LogicAndExpression predicate1 = new LogicAndExpression(timeFilter, valueFilter1);

    FilterNode filterNode1 =
        new FilterNode(
            new PlanNodeId("3"),
            timeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate1,
            false,
            ZonedDateTime.now().getOffset());

    AggregationNode aggregationNode1 =
        new AggregationNode(
            new PlanNodeId("4"),
            Collections.singletonList(filterNode1),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))));

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("5"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("6"),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.forEach(
        planNode -> {
          ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100));
        });

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("7"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression predicate2 = new LogicAndExpression(timeFilter, valueFilter2);

    FilterNode filterNode2 =
        new FilterNode(
            new PlanNodeId("8"),
            timeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            },
            predicate2,
            false,
            ZonedDateTime.now().getOffset());

    AggregationNode aggregationNode2 =
        new AggregationNode(
            new PlanNodeId("9"),
            Collections.singletonList(filterNode2),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))));

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(1, 2, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("10"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList(
                HeaderConstant.COLUMN_DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", aggregationNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", aggregationNode2);

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("11"), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("12"), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }
}
