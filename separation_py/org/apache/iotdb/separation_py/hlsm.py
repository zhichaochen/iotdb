import copy

#
# import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import ks_2samp

from org.apache.iotdb.separation_py.tools import distance, get_cdf_function, to_pdf, get_g, recommend_n_1, get_rc, \
    get_rs


class Hybrid:

    def __init__(self, lsm_buffer_size, generate_time_interval, delay_distance_threshold,
                 delay_buffer_size=100000, min_sequential_buffer_size=1, cdf_split=16):

        # self.rc_ = None
        # self.rs_ = None
        self.is_separate = False
        self.last_delay_set = []
        self.current_delay_set = []
        self.last_delay_analysis = None
        self.current_delay_analysis = None

        self.lsm_buffer_size = lsm_buffer_size
        self.sequential_buffer_size = None
        self.generate_time_interval = generate_time_interval
        self.delay_buffer_size = delay_buffer_size
        self.min_sequential_buffer_size = min_sequential_buffer_size
        self.delay_distance_threshold = delay_distance_threshold
        self.cdf_split = cdf_split
        self.bin_step = self.generate_time_interval / self.cdf_split

        self.rs = 0
        self.rc = 0
        self.seq_rec = 0

    # def description(self):
    #     if self.is_separate:
    #         return 'separate,' + str(self.sequential_buffer_size) + ",rs," + str(self.rs_) + ",rc," + str(self.rc_)
    #     else:
    #         return 'conventional,' + ",rs," + str(self.rs_) + ",rc," + str(self.rc_)

    def __is_delay_changes(self, method='ks'):
        if len(self.last_delay_set) == 0:
            return True
        if method == 'distance':
            return distance(self.last_delay_set, self.current_delay_set, self.bin_step) > self.delay_distance_threshold
        elif method == 'ks':
            _, p = ks_2samp(self.last_delay_set, self.current_delay_set)
            return p < 0.01
        else:
            raise ValueError('unknown distribution test method')

    def write_data(self, delay):
        ## collect delay
        if len(self.current_delay_set) < self.delay_buffer_size:
            self.current_delay_set.append(delay)
        else:
            if self.__is_delay_changes('ks'):
                self.last_delay_set = copy.deepcopy(self.current_delay_set)
                cdf, bins = get_cdf_function(self.current_delay_set, self.bin_step)
                pdf = to_pdf(cdf, self.bin_step)
                G, n_1_list, n_2_list = get_g(self.generate_time_interval, cdf, bins, self.lsm_buffer_size,
                                              self.min_sequential_buffer_size)
                n_1, n_arrival = recommend_n_1(n_1_list, n_2_list, G)
                # print('n_1', n_1, 'n_arrive', n_arrival)
                print('getting rc and rs')
                it_threshold = sum(pdf) * self.bin_step
                rc = get_rc(self.lsm_buffer_size, cdf, pdf, self.bin_step, self.generate_time_interval,
                            threshold=it_threshold)
                print('rc=' + str(rc))
                rs = get_rs(self.lsm_buffer_size, n_1, G, n_arrival, cdf, pdf, self.bin_step,
                            self.generate_time_interval, threshold=it_threshold)
                print('rs=' + str(rs))
                self.rc = rc
                self.rs = rs
                self.seq_rec = n_1
                # self.rc_ = rc
                # self.rs_ = rs
                # print('rc', rc, 'rs', rs)
            self.current_delay_set = []
            self.current_delay_analysis = None
        return self.rc, self.rs, self.seq_rec


if __name__ == '__main__':
    print('hello world')
    arg_time_interval = 50
    arg_buffer_size = 512
    arg_statistics_num = 200
    delay_buffer_size = 100000
    dataset_path = '/home/kyy/Documents/final_project/dataset/mixed-mu-[5, 5, 7, 5, 7]-sigma-[2, 0.5, 1.75, 1, 1.5]-t-50-10000000.npy'

    dataset = np.load(dataset_path)

    with open(
            '/home/kyy/Documents/final_project/dataset/mixed-mu-[5, 5, 7, 5, 7]-sigma-[2, 0.5, 1.75, 1, 1.5]-t-50-10000000.npy.csv',
            'w') as fileout:
        for tuple in dataset:
            fileout.write(str(tuple[0]) + ',' + str(tuple[1]) + ',' + str(tuple[2]) + '\n')

    hybrid = Hybrid(lsm_buffer_size=arg_buffer_size, generate_time_interval=arg_time_interval,
                    delay_distance_threshold=100, delay_buffer_size=delay_buffer_size,
                    cdf_split=1)
    for tuple in dataset:
        hybrid.write_data(tuple[2])
