package org.apache.iotdb.db.separation.statistics;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import static org.apache.iotdb.db.service.IoTDB.delayQueue;

public class SeparationTuningClient implements Runnable {

  SeparationTunningService.Client client;
  TTransport transport;
  TProtocol protocol;

  private double rs = -1;
  private double rc = -1;
  private int seqSize = -1;
  private int totalSize = IoTDBDescriptor.getInstance().getConfig().getTotalCapacity();

  private void tune(double estimatedRc, double estimatedRs, int recSeqSize) {
    System.out.println("print total size:" + totalSize);
    if (estimatedRc > estimatedRs) {
      if ((rs == -1 && rc == -1 && seqSize == -1) || rc < rs) {
        IoTDBDescriptor.getInstance()
            .getConfig()
            .setSeparate(
                true); // only set param here, real migrate logic will be complete at the start of
        // compaction
        IoTDBDescriptor.getInstance().getConfig().setAvgSeqSeriesPointNumberThreshold(recSeqSize);
        IoTDBDescriptor.getInstance()
            .getConfig()
            .setAvgUnseqSeriesPointNumberThreshold(totalSize - recSeqSize);
        rc = estimatedRc;
        rs = estimatedRs;
        seqSize = recSeqSize;
      } else if (seqSize != recSeqSize) {
        IoTDBDescriptor.getInstance().getConfig().setAvgSeqSeriesPointNumberThreshold(recSeqSize);
        IoTDBDescriptor.getInstance()
            .getConfig()
            .setAvgUnseqSeriesPointNumberThreshold(totalSize - recSeqSize);
      }

    } else {
      if ((rs == -1 && rc == -1 && seqSize == -1) || rc <= rs) {
        // do nothing
      } else {
        IoTDBDescriptor.getInstance().getConfig().setSeparate(false);
        IoTDBDescriptor.getInstance().getConfig().setAvgSeqSeriesPointNumberThreshold(0);
        IoTDBDescriptor.getInstance().getConfig().setAvgUnseqSeriesPointNumberThreshold(totalSize);
        rc = estimatedRc;
        rs = estimatedRs;
        seqSize = recSeqSize;
      }
    }
  }

  @Override
  public void run() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableSeparationTuning()) {
      return;
    }
    try {
      transport = new TSocket("127.0.0.1", 8989);
      protocol = new TBinaryProtocol(transport);
      client = new SeparationTunningService.Client(protocol);
      transport.open();
      while (true) {
        if (delayQueue.size() != 0) {
          Double d = delayQueue.poll();
          String result = client.writeDelay(d);
          String[] vals = result.split(",");
          double estimatedRc = Double.parseDouble(vals[0]);
          double estimatedRs = Double.parseDouble(vals[1]);
          int recSeqSize = Integer.parseInt(vals[2]);
          tune(estimatedRc, estimatedRs, recSeqSize);
        }
      }
    } catch (TException e) {
      e.printStackTrace();
    }
  }
}
