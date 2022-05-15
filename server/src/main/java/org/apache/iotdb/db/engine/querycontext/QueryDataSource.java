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

package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import java.util.List;

/**
 * 查询数据源
 * TODO 表示一次查询中所有需要查询的TsFile文件，在存储引擎中便是：TsFileResource
 * QueryDataSource在一个查询中包含一个时间序列的所有有序和无序的TsFileResources
 * The QueryDataSource contains all the seq and unseq TsFileResources for one timeseries in one
 * query
 */
public class QueryDataSource {

  /**
   * TsFileResources used by query job.
   *
   * <p>Note: Sequences under the same virtual storage group share two lists of TsFileResources (seq
   * and unseq).
   */
  private List<TsFileResource> seqResources; // 有序的资源

  private List<TsFileResource> unseqResources; // 无序的资源

  /* The traversal order of unseqResources (different for each device) */
  private int[] unSeqFileOrderIndex;

  /**
   * 数据
   * data older than currentTime - dataTTL should be ignored. */
  private long dataTTL = Long.MAX_VALUE;

  public QueryDataSource(List<TsFileResource> seqResources, List<TsFileResource> unseqResources) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
  }

  public List<TsFileResource> getSeqResources() {
    return seqResources;
  }

  public List<TsFileResource> getUnseqResources() {
    return unseqResources;
  }

  public void setUnSeqFileOrderIndex(int[] index) {
    this.unSeqFileOrderIndex = index;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  /**
   * 用存活时间去更新过滤器
   * @return an updated filter concerning TTL */
  public Filter updateFilterUsingTTL(Filter filter) {
    if (dataTTL != Long.MAX_VALUE) {
      if (filter != null) {
        filter = new AndFilter(filter, TimeFilter.gtEq(System.currentTimeMillis() - dataTTL));
      } else {
        filter = TimeFilter.gtEq(System.currentTimeMillis() - dataTTL);
      }
    }
    return filter;
  }

  public TsFileResource getSeqResourceByIndex(int curIndex) {
    if (curIndex < seqResources.size()) {
      return seqResources.get(curIndex);
    }
    return null;
  }

  public TsFileResource getUnseqResourceByIndex(int curIndex) {
    int actualIndex = unSeqFileOrderIndex[curIndex];
    if (actualIndex < unseqResources.size()) {
      return unseqResources.get(actualIndex);
    }
    return null;
  }

  public boolean hasNextSeqResource(int curIndex, boolean ascending) {
    return ascending ? curIndex < seqResources.size() : curIndex >= 0;
  }

  public boolean hasNextUnseqResource(int curIndex) {
    return curIndex < unseqResources.size();
  }

  public int getSeqResourcesSize() {
    return seqResources.size();
  }

  public int getUnseqResourcesSize() {
    return unseqResources.size();
  }
}
