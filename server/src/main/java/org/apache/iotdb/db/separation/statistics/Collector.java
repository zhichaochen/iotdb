package org.apache.iotdb.db.separation.statistics;

import org.python.core.Py;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.util.Properties;

import static org.apache.iotdb.db.service.IoTDB.delayQueue;

public class Collector {

  public static void collect(Long generationTime, Long arrivalTime) {
    delayQueue.add((double)(arrivalTime - generationTime));
  }
}
