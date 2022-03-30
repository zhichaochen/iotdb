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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.query.expression.ResultColumn;

import java.util.List;

/**
 * UDF ：user-defined function 用户定义函数，或自定义函数
 *
 * UDF execution plan.
 *
 * <p>The life cycle of an executor:
 *
 * <p>constructUdfExecutors -> initializeUdfExecutor -> finalizeUDFExecutors
 */
public interface UDFPlan {

  /**
   * 构建执行器的执行计划，这个方法不创建任何UDF实例，也不执行用户定义逻辑
   * Build the execution plan of the executors. This method will not create any UDF instances, nor
   * will it execute user-defined logic.
   */
  void constructUdfExecutors(List<ResultColumn> resultColumns);

  /**
   * 调用UDF最终方法去释放计算资源
   * Call UDF finalization methods and release computing resources. */
  void finalizeUDFExecutors(long queryId);
}
