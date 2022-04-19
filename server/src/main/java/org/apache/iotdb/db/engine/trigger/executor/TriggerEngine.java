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

package org.apache.iotdb.db.engine.trigger.executor;

import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationInformation;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 触发器引擎
 * 触发器提供了一种侦听序列数据变动的机制。配合用户自定义逻辑，可完成告警、数据清洗、数据转发等功能。
 */
public class TriggerEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerEngine.class);

  /**
   * 启动触发器
   * @param event
   * @param insertRowPlan
   * @throws TriggerExecutionException
   */
  public static void fire(TriggerEvent event, InsertRowPlan insertRowPlan)
      throws TriggerExecutionException {
    if (TriggerRegistrationService.getInstance().executorSize() == 0) {
      return;
    }

    IMeasurementMNode[] mNodes = insertRowPlan.getMeasurementMNodes();
    int size = mNodes.length;

    // 插入数据的时间戳
    long timestamp = insertRowPlan.getTime();
    // 要插入的值
    Object[] values = insertRowPlan.getValues();

    // 遍历所欲物理量
    for (int i = 0; i < size; ++i) {
      IMeasurementMNode mNode = mNodes[i];
      if (mNode == null) {
        continue;
      }
      for (TriggerExecutor executor : mNode.getUpperTriggerExecutorList()) {
        executor.fireIfActivated(event, timestamp, values[i], mNode.getSchema().getType());
      }
    }
  }

  public static void fire(TriggerEvent event, InsertTabletPlan insertTabletPlan, int firePosition)
      throws TriggerExecutionException {
    if (TriggerRegistrationService.getInstance().executorSize() == 0) {
      return;
    }

    IMeasurementMNode[] mNodes = insertTabletPlan.getMeasurementMNodes();
    int size = mNodes.length;

    long[] timestamps = insertTabletPlan.getTimes();
    Object[] columns = insertTabletPlan.getColumns();
    if (firePosition != 0) {
      timestamps = Arrays.copyOfRange(timestamps, firePosition, timestamps.length);
      columns = Arrays.copyOfRange(columns, firePosition, columns.length);
    }

    for (int i = 0; i < size; ++i) {
      IMeasurementMNode mNode = mNodes[i];
      if (mNode == null) {
        continue;
      }
      for (TriggerExecutor executor : mNode.getUpperTriggerExecutorList()) {
        executor.fireIfActivated(event, timestamps, columns[i], mNode.getSchema().getType());
      }
    }
  }

  /**
   * 关闭触发器
   * @param measurementMNode
   */
  public static void drop(IMeasurementMNode measurementMNode) {
    TriggerExecutor executor = measurementMNode.getTriggerExecutor();
    if (executor == null) {
      return;
    }

    TriggerRegistrationInformation information = executor.getRegistrationInformation();
    try {
      TriggerRegistrationService.getInstance()
          .deregister(new DropTriggerPlan(information.getTriggerName()));
    } catch (TriggerManagementException e) {
      LOGGER.warn(
          "Failed to deregister trigger {}({}) when deleting timeseries ({}).",
          information.getTriggerName(),
          information.getClassName(),
          measurementMNode.getPartialPath().getFullPath(),
          e);
    }
  }

  public static void drop(List<IMeasurementMNode> measurementMNodes) {
    for (IMeasurementMNode measurementMNode : measurementMNodes) {
      drop(measurementMNode);
    }
  }

  private TriggerEngine() {}
}
