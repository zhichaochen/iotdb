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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.service.TriggerClassLoader;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationInformation;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.lang.reflect.InvocationTargetException;

/**
 * 触发器执行器，每个触发器都会创建一个触发器执行器
 * 作用应该是触发->触发器的动作
 */
public class TriggerExecutor {

  private final TriggerRegistrationInformation registrationInformation;
  private final TriggerAttributes attributes;

  private final TriggerClassLoader classLoader;

  private final IMeasurementMNode measurementMNode;
  private final TSDataType seriesDataType;

  private final Trigger trigger;

  public TriggerExecutor(
      TriggerRegistrationInformation registrationInformation,
      TriggerClassLoader classLoader,
      IMeasurementMNode measurementMNode)
      throws TriggerManagementException {
    this.registrationInformation = registrationInformation;
    attributes = new TriggerAttributes(registrationInformation.getAttributes());

    this.classLoader = classLoader;

    this.measurementMNode = measurementMNode;
    seriesDataType = measurementMNode.getSchema().getType();

    // TODO 实例化触发器类
    trigger = constructTriggerInstance();
  }

  /**
   * 通过反射实例化类触发器
   * @return
   * @throws TriggerManagementException
   */
  private Trigger constructTriggerInstance() throws TriggerManagementException {
    try {
      Class<?> triggerClass =
          Class.forName(registrationInformation.getClassName(), true, classLoader);
      return (Trigger) triggerClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to reflect Trigger %s(%s) instance, because %s",
              registrationInformation.getTriggerName(), registrationInformation.getClassName(), e));
    }
  }

  public void onCreate() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onCreate(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("onConfig(TriggerAttributes)", e);
    }

    // The field isStopped in the registrationInformation is volatile, so the method
    // registrationInformation.markAsStarted() is always invoked after the method
    // trigger.onCreate(attributes) is invoked. It guarantees that the trigger will not be triggered
    // before trigger.onCreate(attributes) is called.
    registrationInformation.markAsStarted();
  }

  public synchronized void onDrop() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    registrationInformation.markAsStopped();

    try {
      trigger.onDrop();
    } catch (Exception e) {
      onTriggerExecutionError("onConfig(TriggerAttributes)", e);
    }
  }

  /**
   * 启动触发器
   * @throws TriggerExecutionException
   */
  public synchronized void onStart() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    // 此处语句的执行顺序不能互换！
    // 调用Trigger的onstart方法
    invokeOnStart();
    // 设置标志位为启动
    registrationInformation.markAsStarted();
  }

  /*调用启动方法*/
  private void invokeOnStart() throws TriggerExecutionException {
    // 设置线程上下文为当前类加载器
    // Trigger是通过classLoader加载的，想调用到Trigger必须使用加载它的类加载器
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      // 通知触发器
      trigger.onStart();
    } catch (Exception e) {
      onTriggerExecutionError("onStart()", e);
    }
  }

  public synchronized void onStop() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    registrationInformation.markAsStopped();
    invokeOnStop();
  }

  private void invokeOnStop() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onStop();
    } catch (Exception e) {
      onTriggerExecutionError("onStop()", e);
    }
  }

  public void fireIfActivated(TriggerEvent event, long timestamp, Object value)
      throws TriggerExecutionException {
    if (!registrationInformation.isStopped() && event.equals(registrationInformation.getEvent())) {
      fire(timestamp, value);
    }
  }

  private synchronized void fire(long timestamp, Object value) throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          trigger.fire(timestamp, (Integer) value);
          break;
        case INT64:
          trigger.fire(timestamp, (Long) value);
          break;
        case FLOAT:
          trigger.fire(timestamp, (Float) value);
          break;
        case DOUBLE:
          trigger.fire(timestamp, (Double) value);
          break;
        case BOOLEAN:
          trigger.fire(timestamp, (Boolean) value);
          break;
        case TEXT:
          trigger.fire(timestamp, (Binary) value);
          break;
        default:
          throw new TriggerExecutionException("Unsupported series data type.");
      }
    } catch (TriggerExecutionException e) {
      throw e;
    } catch (Exception e) {
      onTriggerExecutionError("fire(long, Object)", e);
    }
  }

  /**
   * 执行fire
   * @param event
   * @param timestamps
   * @param values
   * @throws TriggerExecutionException
   */
  public void fireIfActivated(TriggerEvent event, long[] timestamps, Object values)
      throws TriggerExecutionException {
    if (!registrationInformation.isStopped() && event.equals(registrationInformation.getEvent())) {
      fire(timestamps, values);
    }
  }

  private synchronized void fire(long[] timestamps, Object values)
      throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          trigger.fire(timestamps, (int[]) values);
          break;
        case INT64:
          trigger.fire(timestamps, (long[]) values);
          break;
        case FLOAT:
          trigger.fire(timestamps, (float[]) values);
          break;
        case DOUBLE:
          trigger.fire(timestamps, (double[]) values);
          break;
        case BOOLEAN:
          trigger.fire(timestamps, (boolean[]) values);
          break;
        case TEXT:
          trigger.fire(timestamps, (Binary[]) values);
          break;
        default:
          throw new TriggerExecutionException("Unsupported series data type.");
      }
    } catch (TriggerExecutionException e) {
      throw e;
    } catch (Exception e) {
      onTriggerExecutionError("fire(long[], Object)", e);
    }
  }

  private void onTriggerExecutionError(String methodName, Exception e)
      throws TriggerExecutionException {
    throw new TriggerExecutionException(
        String.format(
                "Error occurred during executing Trigger#%s: %s",
                methodName, System.lineSeparator())
            + e);
  }

  public TriggerRegistrationInformation getRegistrationInformation() {
    return registrationInformation;
  }

  public IMeasurementMNode getMeasurementMNode() {
    return measurementMNode;
  }

  @TestOnly
  public Trigger getTrigger() {
    return trigger;
  }
}
