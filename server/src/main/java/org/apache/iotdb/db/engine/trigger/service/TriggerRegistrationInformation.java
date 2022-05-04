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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;

import java.util.Map;

/**
 * 触发器的注册信息
 */
public class TriggerRegistrationInformation {

  private final String triggerName; // 触发器名称
  private final TriggerEvent event; // 事件
  private final PartialPath fullPath; // 全路径
  private final String className; // 类名，比如：org.apache.iotdb.db.engine.trigger.example.AlertListener
  private final Map<String, String> attributes; // 自定义的属性集合，在通过Sql注册Trigger的时候添加

  private volatile boolean isStopped; // 当前触发器是否停止中，

  public TriggerRegistrationInformation(CreateTriggerPlan plan) {
    this.triggerName = plan.getTriggerName();
    this.event = plan.getEvent();
    this.fullPath = plan.getFullPath();
    this.className = plan.getClassName();
    this.attributes = plan.getAttributes();
    this.isStopped = plan.isStopped();
  }

  public CreateTriggerPlan convertToCreateTriggerPlan() {
    return new CreateTriggerPlan(triggerName, event, fullPath, className, attributes);
  }

  public void markAsStarted() {
    isStopped = false;
  }

  public void markAsStopped() {
    isStopped = true;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public boolean isStopped() {
    return isStopped;
  }
}
