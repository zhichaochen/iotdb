from org.apache.iotdb.separation_py import SeparationTunningService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from org.apache.iotdb.separation_py.hlsm import Hybrid


class SeparationHandler:
    def __init__(self):
        arg_time_interval = 50
        arg_buffer_size = 512
        delay_buffer_size = 100000
        self.hybrid = Hybrid(lsm_buffer_size=arg_buffer_size, generate_time_interval=arg_time_interval,
                             delay_distance_threshold=100, delay_buffer_size=delay_buffer_size,
                             cdf_split=1)

    def writeDelay(self, delay):
        rc_est, rs_est, seq_size_rec = self.hybrid.write_data(delay)
        return str(rc_est) + "," + str(rs_est) + "," + str(seq_size_rec)


handler = SeparationHandler()
processor = SeparationTunningService.Processor(handler)
transport = TSocket.TServerSocket("127.0.0.1", 8989)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
print("Starting tuning server in python...")
server.serve()
