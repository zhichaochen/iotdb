from org.apache.iotdb.separation_py import SeparationTunningService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from org.apache.iotdb.separation_py.hlsm import Hybrid


class SeparationHandler:
    def __init__(self):
        print('hello world')
        arg_time_interval = 50
        arg_buffer_size = 512
        # arg_statistics_num = 200
        delay_buffer_size = 100000
        # dataset_path = 'mixed-mu-[5, 5, 7, 5, 7]-sigma-[2, 0.5, 1.75, 1, 1.5]-t-50-10000000.npy'
        # dataset = np.load(dataset_path)

        # print
        # dataset.shape

        self.hybrid = Hybrid(lsm_buffer_size=arg_buffer_size, generate_time_interval=arg_time_interval,
                        delay_distance_threshold=100, delay_buffer_size=delay_buffer_size,
                        cdf_split=1)
        # for tuple in dataset:
        #     hybrid.write_data(tuple[2])

        # pass


    def writeDelay(self, delay):
        rc_est, rs_est, seq_size_rec = self.hybrid.write_data(delay)
        return str(rc_est) + "," + str(rs_est) + "," + str(seq_size_rec)

# handler processer类
handler = SeparationHandler()
processor = SeparationTunningService.Processor(handler)
transport = TSocket.TServerSocket("127.0.0.1", 8989)
# 传输方式，使用buffer
tfactory = TTransport.TBufferedTransportFactory()
# 传输的数据类型：二进制
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
# 创建一个thrift 服务
server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

print("Starting thrift server in python...")

server.serve()
print("done!")
