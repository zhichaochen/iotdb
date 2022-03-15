package org.apache.iotdb.db.separation.statistics;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import static org.apache.iotdb.db.service.IoTDB.delayQueue;

public class Collector {

  public static void collect(Long generationTime, Long arrivalTime) {
    if (delayQueue.size() <= IoTDBDescriptor.getInstance().getConfig().getDelayNum()) {
      delayQueue.add((double) (arrivalTime - generationTime));
    }
  }
}
