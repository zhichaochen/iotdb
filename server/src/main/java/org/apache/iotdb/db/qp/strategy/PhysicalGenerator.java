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
package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;

import java.util.List;

/**
 * 物理计划生成器
 * 用户将逻辑计划转换成物理计划
 * Used to convert logical operator to physical plan */
public class PhysicalGenerator {

  /**
   * 将逻辑算子转化成物理计划
   * @param operator
   * @return
   * @throws QueryProcessException
   */
  public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessException {
    // 生成物理计划
    PhysicalPlan physicalPlan = operator.generatePhysicalPlan(this);
    // 设置debug
    physicalPlan.setDebug(operator.isDebug());
    // 设置前缀匹配
    physicalPlan.setPrefixMatch(operator.isPrefixMatchPath());
    return physicalPlan;
  }

  public PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    switch (type) {
      case GLOBAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL);
      case LOCAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
      default:
        throw new QueryProcessException(
            String.format("Unrecognized load configuration operator type, %s", type.name()));
    }
  }

  public List<PartialPath> groupVectorPaths(List<PartialPath> paths) throws MetadataException {
    return MetaUtils.groupAlignedPaths(paths);
  }
}
