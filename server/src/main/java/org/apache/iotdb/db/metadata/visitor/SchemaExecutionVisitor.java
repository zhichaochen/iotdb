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

package org.apache.iotdb.db.metadata.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteRegionNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/** Schema write PlanNode visitor */
public class SchemaExecutionVisitor extends PlanVisitor<TSStatus, ISchemaRegion> {
  private static final Logger logger = LoggerFactory.getLogger(SchemaExecutionVisitor.class);

  @Override
  public TSStatus visitCreateTimeSeries(CreateTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      PhysicalPlan plan = node.accept(new PhysicalPlanTransformer(), new TransformerContext());
      schemaRegion.createTimeseries((CreateTimeSeriesPlan) plan, -1);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitCreateAlignedTimeSeries(
      CreateAlignedTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      PhysicalPlan plan = node.accept(new PhysicalPlanTransformer(), new TransformerContext());
      schemaRegion.createAlignedTimeSeries((CreateAlignedTimeSeriesPlan) plan);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesNode node, ISchemaRegion schemaRegion) {
    CreateMultiTimeSeriesPlan multiPlan =
        (CreateMultiTimeSeriesPlan)
            node.accept(new PhysicalPlanTransformer(), new TransformerContext());
    for (int i = 0; i < multiPlan.getPaths().size(); i++) {
      if (multiPlan.getResults().containsKey(i) || multiPlan.isExecuted(i)) {
        continue;
      }
      CreateTimeSeriesPlan plan =
          new CreateTimeSeriesPlan(
              multiPlan.getPaths().get(i),
              multiPlan.getDataTypes().get(i),
              multiPlan.getEncodings().get(i),
              multiPlan.getCompressors().get(i),
              multiPlan.getProps() == null ? null : multiPlan.getProps().get(i),
              multiPlan.getTags() == null ? null : multiPlan.getTags().get(i),
              multiPlan.getAttributes() == null ? null : multiPlan.getAttributes().get(i),
              multiPlan.getAlias() == null ? null : multiPlan.getAlias().get(i));
      try {
        schemaRegion.createTimeseries(plan, -1);
      } catch (MetadataException e) {
        logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        multiPlan.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
    if (!multiPlan.getResults().isEmpty()) {
      return RpcUtils.getStatus(Arrays.asList(multiPlan.getFailingStatus()));
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitAlterTimeSeries(AlterTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      switch (node.getAlterType()) {
        case RENAME:
          String beforeName = node.getAlterMap().keySet().iterator().next();
          String currentName = node.getAlterMap().get(beforeName);
          schemaRegion.renameTagOrAttributeKey(beforeName, currentName, node.getPath());
          break;
        case SET:
          schemaRegion.setTagsOrAttributesValue(node.getAlterMap(), node.getPath());
          break;
        case DROP:
          schemaRegion.dropTagsOrAttributes(node.getAlterMap().keySet(), node.getPath());
          break;
        case ADD_TAGS:
          schemaRegion.addTags(node.getAlterMap(), node.getPath());
          break;
        case ADD_ATTRIBUTES:
          schemaRegion.addAttributes(node.getAlterMap(), node.getPath());
          break;
        case UPSERT:
          schemaRegion.upsertTagsAndAttributes(
              node.getAlias(), node.getTagsMap(), node.getAttributesMap(), node.getPath());
          break;
      }
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    } catch (IOException e) {
      logger.error("{}: IO error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitPlan(PlanNode node, ISchemaRegion context) {
    return null;
  }

  // TODO need remove
  private static class PhysicalPlanTransformer
      extends PlanVisitor<PhysicalPlan, TransformerContext> {
    @Override
    public PhysicalPlan visitPlan(PlanNode node, TransformerContext context) {
      throw new NotImplementedException();
    }

    public PhysicalPlan visitCreateTimeSeries(
        CreateTimeSeriesNode node, TransformerContext context) {
      return new CreateTimeSeriesPlan(
          node.getPath(),
          node.getDataType(),
          node.getEncoding(),
          node.getCompressor(),
          node.getProps(),
          node.getTags(),
          node.getAttributes(),
          node.getAlias());
    }

    public PhysicalPlan visitCreateAlignedTimeSeries(
        CreateAlignedTimeSeriesNode node, TransformerContext context) {
      return new CreateAlignedTimeSeriesPlan(
          node.getDevicePath(),
          node.getMeasurements(),
          node.getDataTypes(),
          node.getEncodings(),
          node.getCompressors(),
          node.getAliasList(),
          node.getTagsList(),
          node.getAttributesList());
    }

    public PhysicalPlan visitCreateMultiTimeSeries(
        CreateMultiTimeSeriesNode node, TransformerContext context) {
      CreateMultiTimeSeriesPlan multiPlan = new CreateMultiTimeSeriesPlan();
      multiPlan.setPaths(node.getPaths());
      multiPlan.setDataTypes(node.getDataTypes());
      multiPlan.setEncodings(node.getEncodings());
      multiPlan.setCompressors(node.getCompressors());
      multiPlan.setProps(node.getPropsList());
      multiPlan.setAlias(node.getAliasList());
      multiPlan.setTags(node.getTagsList());
      multiPlan.setAttributes(node.getAttributesList());
      return multiPlan;
    }
  }

  @Override
  public TSStatus visitDeleteRegion(DeleteRegionNode node, ISchemaRegion schemaRegion) {
    try {
      SchemaEngine.getInstance().deleteSchemaRegion((SchemaRegionId) node.getConsensusGroupId());
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  private static class TransformerContext {}
}
