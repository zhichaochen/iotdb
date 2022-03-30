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
package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceCheck;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.CacheHitRatioMonitor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.cq.ContinuousQueryService;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.LocalConfigManager;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManager;
import org.apache.iotdb.db.protocol.rest.RestService;
import org.apache.iotdb.db.query.udf.service.TemporaryQueryDataFileService;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.service.basic.StandaloneServiceProvider;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.sync.receiver.SyncServerManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Iot db
 * TODO IotDB启动类
 */
public class IoTDB implements IoTDBMBean {

  private static final Logger logger = LoggerFactory.getLogger(IoTDB.class);
  private final String mbeanName =
      String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, "IoTDB");
  // 服务注册管理器
  private static final RegisterManager registerManager = new RegisterManager();
  // 元数据管理器
  public static MManager metaManager = MManager.getInstance();
  // 服务提供器， TODO ServiceProvider和RegisterManager有啥区别
  public static LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
  public static LocalConfigManager configManager = LocalConfigManager.getInstance();
  public static ServiceProvider serviceProvider;
  // 是否是集群模式
  private static boolean clusterMode = false;

  public static IoTDB getInstance() {
    return IoTDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    try {
      // 检查iot配置
      IoTDBConfigCheck.getInstance().checkConfig();
      // 检查 iot rest 服务，检查服务通信
      IoTDBRestServiceCheck.getInstance().checkConfig();
    } catch (ConfigurationException | IOException e) {
      logger.error("meet error when doing start checking", e);
      // 如果有错误则退出
      System.exit(1);
    }
    // 获取当前服务，并启动守护进程
    IoTDB daemon = IoTDB.getInstance();
    daemon.active();
  }

  public static void setSchemaProcessor(LocalSchemaProcessor schemaProcessor) {
    IoTDB.schemaProcessor = schemaProcessor;
  }

  public static void setServiceProvider(ServiceProvider serviceProvider) {
    IoTDB.serviceProvider = serviceProvider;
  }

  public static void setClusterMode() {
    IoTDB.clusterMode = true;
  }

  public static boolean isClusterMode() {
    return IoTDB.clusterMode;
  }

  public void active() {
    // 启动检查
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      // TODO: what are some checks
      logger.error(
          "{}: failed to start because some checks failed. ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return;
    }
    try {
      // 设置所需要初始化的服务
      setUp();
    } catch (StartupException | QueryProcessException e) {
      logger.error("meet error while starting up.", e);
      // 卸载服务
      deactivate();
      logger.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
      return;
    }
    logger.info("{} has started.", IoTDBConstant.GLOBAL_DB_NAME);
  }

  /**
   * 设置
   * @throws StartupException
   * @throws QueryProcessException
   */
  private void setUp() throws StartupException, QueryProcessException {
    logger.info("Setting up IoTDB...");

    // 优雅关闭的钩子
    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();
    initServiceProvider();
    registerManager.register(MetricsService.getInstance());
    logger.info("recover the schema...");
    initConfigManager();
    registerManager.register(JMXService.getInstance());
    registerManager.register(FlushManager.getInstance());
    registerManager.register(MultiFileLogNodeManager.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());
    registerManager.register(CompactionTaskManager.getInstance());
    JMXService.registerMBean(getInstance(), mbeanName);
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(UDFClassLoaderManager.getInstance());
    registerManager.register(UDFRegistrationService.getInstance());

    // in cluster mode, RPC service is not enabled.
    //
    if (IoTDBDescriptor.getInstance().getConfig().isEnableRpcService()) {
      registerManager.register(RPCService.getInstance());
    }

    // 初始化协议
    initProtocols();
    // in cluster mode, InfluxDBMManager has been initialized, so there is no need to init again to
    // avoid wasting time.
    if (!isClusterMode()
        && IoTDBDescriptor.getInstance().getConfig().isEnableInfluxDBRpcService()) {
      initInfluxDBMManager();
    }

    logger.info("IoTDB is set up, now may some sgs are not ready, please wait several seconds...");

    while (!StorageEngine.getInstance().isAllSgReady()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("IoTDB failed to set up.", e);
        Thread.currentThread().interrupt();
        return;
      }
    }

    registerManager.register(SyncServerManager.getInstance());
    registerManager.register(UpgradeSevice.getINSTANCE());
    registerManager.register(SettleService.getINSTANCE());
    registerManager.register(TriggerRegistrationService.getInstance());
    registerManager.register(ContinuousQueryService.getInstance());

    // start reporter
    MetricsService.getInstance().startAllReporter();

    logger.info("Congratulation, IoTDB is set up successfully. Now, enjoy yourself!");
  }

  public static void initInfluxDBMManager() {
    InfluxDBMetaManager.getInstance().recover();
  }

  private void initServiceProvider() throws QueryProcessException {
    if (!clusterMode) {
      serviceProvider = new StandaloneServiceProvider();
    }
  }

  public static void initProtocols() throws StartupException {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableInfluxDBRpcService()) {
      registerManager.register(InfluxDBRPCService.getInstance());
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableMQTTService()) {
      registerManager.register(MQTTService.getInstance());
    }
    if (IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableRestService()) {
      registerManager.register(RestService.getInstance());
    }
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB...");
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  private void initConfigManager() {
    long time = System.currentTimeMillis();
    IoTDB.configManager.init();
    long end = System.currentTimeMillis() - time;
    logger.info("spend {}ms to recover schema.", end);
    logger.info(
        "After initializing, sequence tsFile threshold is {}, unsequence tsFile threshold is {}, memtableSize is {}",
        IoTDBDescriptor.getInstance().getConfig().getSeqTsFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getUnSeqTsFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold());
  }

  @Override
  public void stop() {
    deactivate();
  }

  public void shutdown() throws Exception {
    // TODO shutdown is not equal to stop()
    logger.info("Deactivating IoTDB...");
    registerManager.shutdownAll();
    PrimitiveArrayManager.close();
    SystemInfo.getInstance().close();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  // 设置未捕获异常处理器
  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class IoTDBHolder {

    private static final IoTDB INSTANCE = new IoTDB();

    private IoTDBHolder() {}
  }
}
