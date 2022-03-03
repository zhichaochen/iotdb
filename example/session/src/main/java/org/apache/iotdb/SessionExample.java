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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SuppressWarnings("squid:S106")
public class SessionExample {

  private static Session session;
  private static Session sessionEnableRedirect;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1_S5 = "root.sg1.d1.s5";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    if ("query".equals(args[0])) {
      String sql = args[1];
      query(sql);
    } else {
      long totalRowNum = Long.parseLong(args[1]);
      int sensorNum = Integer.parseInt(args[2]);
      float nullRatio = Float.parseFloat(args[3]);
      long startTime = System.currentTimeMillis();
      if ("aligned".equals(args[0])) {
        insertTablet(totalRowNum, sensorNum, true, nullRatio);
        System.out.println(
            "Insert aligned "
                + totalRowNum
                + " rows cost: "
                + (System.currentTimeMillis() - startTime)
                + "ms.");
      } else if ("nonAligned".equals(args[0])) {
        insertTablet(totalRowNum, sensorNum, false, nullRatio);
        System.out.println(
            "Insert nonAligned "
                + totalRowNum
                + " rows cost: "
                + (System.currentTimeMillis() - startTime)
                + "ms.");
      } else {
        throw new IllegalArgumentException("unknown command: " + args[0]);
      }
    }
    session.close();
  }

  private static void insertTablet(
      long totalRowNum, int sensorNum, boolean isAligned, float nullRatio)
      throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < sensorNum; i++) {
      schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
    }

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 1000);

    long timestamp = 1646134492000L;
    Random random = new Random(123456);
    for (long row = 0; row < totalRowNum; row += 10_000) {
      for (int i = 9_999; i >= 0; i--) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + i);
        for (int s = 0; s < sensorNum; s++) {
          float value = -100.0f + 200.0f * random.nextFloat();
          if (random.nextFloat() < nullRatio) {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, null);
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, timestamp + i);
          }
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          if (isAligned) {
            session.insertAlignedTablet(tablet, false);
          } else {
            session.insertTablet(tablet, false);
          }
          tablet.reset();
        }
      }
      System.out.println("already insert: " + row + " rows.");
      timestamp += 10_000;
    }

    if (tablet.rowSize != 0) {
      if (isAligned) {
        session.insertAlignedTablet(tablet, false);
      } else {
        session.insertTablet(tablet, false);
      }
      tablet.reset();
    }
  }

  private static void query(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    long startTime = System.currentTimeMillis();
    try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      dataSet.setFetchSize(10000); // default is 10000
      while (dataSet.hasNext()) {
        dataSet.next();
      }
    }
    System.out.println("cost: " + (System.currentTimeMillis() - startTime) + "ms");
  }
}
