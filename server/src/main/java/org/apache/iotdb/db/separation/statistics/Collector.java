package org.apache.iotdb.db.separation.statistics;

import java.util.Properties;
import org.python.core.Py;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class Collector {

  static String res = getString();

  private static String getString() {
    Properties props = new Properties();
    System.out.println("xxxxx");
    return "???";
  }

  static PythonInterpreter pi;
  static PyObject pyObject;

  public Collector(){

  }

  public static void collect(Long generationTime, Long arrivalTime){
    System.out.println("hello");
    pi = new PythonInterpreter();
    String hybridFile = "separation-py/hlsm.py";
    String pythonObjName = "hybrid";
    String pythonClassName = "Hybrid";
    System.out.println("world");
    pi.execfile(hybridFile);
    pi.exec(pythonObjName+"="+pythonClassName+"()");
    pyObject = pi.get(pythonObjName);
    System.out.println("hello world");
    PyObject result = pyObject.invoke("write_data", new PyObject[]{Py.newLong(arrivalTime - generationTime)});
    System.out.println(result);
  }


}
