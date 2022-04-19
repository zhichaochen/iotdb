/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.rest.RestApiService;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.handler.PhysicalPlanConstructionHandler;
import org.apache.iotdb.db.protocol.rest.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.model.SQL;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;

/**
 * 查询入口
 */
public class RestApiServiceImpl extends RestApiService {

  public static ServiceProvider serviceProvider = IoTDB.serviceProvider;

  private final Planner planner;
  private final AuthorizationHandler authorizationHandler;

  private final Integer defaultQueryRowLimit;

  public RestApiServiceImpl() throws QueryProcessException {
    planner = serviceProvider.getPlanner();
    authorizationHandler = new AuthorizationHandler();

    defaultQueryRowLimit =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestQueryDefaultRowSizeLimit();
  }

  /**
   * 执行非query语句
   * @param sql
   * @param securityContext
   * @return
   */
  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      // 校验SQL
      RequestValidationHandler.validateSQL(sql);

      // 解析Sql将其转换为物理计划
      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql.getSql());
      // 校验权限
      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      return Response.ok()
          .entity(
              serviceProvider.executeNonQuery(physicalPlan)
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                      .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
          .build();
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  /**
   * 执行查询语句
   * @param sql
   * @param securityContext
   * @return
   */
  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      // 生成物理计划
      PhysicalPlan physicalPlan =
          planner.parseSQLToRestQueryPlan(sql.getSql(), ZoneId.systemDefault());
      // 设置登陆人名称
      physicalPlan.setLoginUserName(securityContext.getUserPrincipal().getName());
      if (!(physicalPlan instanceof QueryPlan)
          && !(physicalPlan instanceof ShowPlan)
          && !(physicalPlan instanceof AuthorPlan)) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }

      // 检查权限
      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      // 查询ID
      final long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
      try {
        // 查询上下文
        QueryContext queryContext =
            serviceProvider.genQueryContext(
                queryId,
                physicalPlan.isDebug(),
                System.currentTimeMillis(),
                sql.getSql(),
                IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
        // 查询数据集，TODO 这是核心逻辑
        QueryDataSet queryDataSet =
            serviceProvider.createQueryDataSet(
                queryContext, physicalPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
        // set max row limit to avoid OOM
        // 设置最大行限制，避免OOM
        return QueryDataSetHandler.fillQueryDataSet(
            queryDataSet,
            physicalPlan,
            sql.getRowLimit() == null ? defaultQueryRowLimit : sql.getRowLimit());
      } finally {
        ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response insertTablet(
      InsertTabletRequest insertTabletRequest, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateInsertTabletRequest(insertTabletRequest);

      InsertTabletPlan insertTabletPlan =
          PhysicalPlanConstructionHandler.constructInsertTabletPlan(insertTabletRequest);

      Response response = authorizationHandler.checkAuthority(securityContext, insertTabletPlan);
      if (response != null) {
        return response;
      }

      return Response.ok()
          .entity(
              serviceProvider.executeNonQuery(insertTabletPlan)
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode())
                      .message(TSStatusCode.WRITE_PROCESS_ERROR.name()))
          .build();
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }
}
