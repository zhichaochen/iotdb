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

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectIntoOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateContinuousQueryOperator;

/**
 * 逻辑计划检查器
 */
public class LogicalChecker {

  private LogicalChecker() {}

  /** To check whether illegal component exists in specific operator. */
  public static void check(Operator operator) throws LogicalOperatorException {
    if (operator instanceof QueryOperator) {
      ((QueryOperator) operator).check();
    }

    if (operator instanceof SelectIntoOperator) {
      ((SelectIntoOperator) operator).check();
    }

    if (operator instanceof CreateContinuousQueryOperator) {
      ((CreateContinuousQueryOperator) operator).getQueryOperator().check();
    }
  }
}
