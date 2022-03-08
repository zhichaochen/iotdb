package org.apache.iotdb.db.separation.statistics;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import static java.lang.Thread.sleep;
import static org.apache.iotdb.db.service.IoTDB.delayQueue;

/**
 * @version V0.1.0
 * @Description: java thrift 客户端
 * @see
 * @since 2016-06-01
 */
public class SeparationTuningClient implements Runnable {
    SeparationTunningService.Client client;
    TTransport transport;
    TProtocol protocol;

    private double rs = -1;
    private double rc = -1;
    private int secSize = -1;

    private void tune(double estimatedRc, double estimatedRs, int recSeqSize) {
        if (estimatedRc > estimatedRs) {
            if ((rs == -1 && rc == -1 && secSize == -1) || rc < rs) {
                rc = estimatedRc;
                rs = estimatedRs;
                secSize = recSeqSize;
            } else if (secSize != recSeqSize) {
                // do nothing
            }

        } else {
            if ((rs == -1 && rc == -1 && secSize == -1) || rc <= rs) {
                // do nothing
            } else {
                rc = estimatedRc;
                rs = estimatedRs;
                secSize = recSeqSize;
            }
        }
    }

    @Override
    public void run() {
        if (!IoTDBDescriptor.getInstance().getConfig().isEnableSeparationTuning()) {
            return;
        }
        try {
            System.out.println("thrift client connext server at 8989 port ");
            transport = new TSocket("127.0.0.1", 8989);
            protocol = new TBinaryProtocol(transport);
            client = new SeparationTunningService.Client(protocol);
            transport.open();
            while (true) {
                sleep(1000);
                if (delayQueue.size() != 0) {
                    String result = client.writeDelay(delayQueue.poll());
                    String[] vals = result.split(",");
                    double estimatedRc = Double.parseDouble(vals[0]);
                    double estimatedRs = Double.parseDouble(vals[1]);
                    int recSeqSize = Integer.parseInt(vals[2]);
                    tune(estimatedRc, estimatedRs, recSeqSize);
                }
            }

        } catch (TException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}