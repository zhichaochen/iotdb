import copy
import ctypes
import multiprocessing

import matplotlib.pyplot as plt
import numpy as np

from tools import split_list


def split_list(original_list, number):
    """
    Distribute the tasks into multiple groups
    :param original_list: a list of tasks
    :param number: number of groups
    :return: a list of task groups
    """
    task_groups = []
    if number >= len(original_list):
        for element in original_list:
            task_groups.append([element])
    else:
        for i in range(number):
            task_groups.append([])
        i = 0
        for element in original_list:
            task_groups[i % number].append(element)
            i += 1
    return task_groups


def distance(set1, set2, bin_step):
    """
    Note that the 2 sets should have the same domain
    """
    if len(set1) == 0 or len(set2) == 0:
        return 99999999999999999
    max_val = max(max(set1), max(set2))
    bin_num = int(max_val / bin_step)
    bins_index = np.arange(0, bin_num, 1)
    bin_val = bins_index * bin_step
    cdf1, _, _ = plt.hist(set1, bin_val)
    cdf2, _, _ = plt.hist(set2, bin_val)
    dist = np.linalg.norm(np.array(cdf1) - np.array(cdf2), ord=2)
    plt.cla()
    return dist


def shift_left(vector, step=1, fill=1):
    assert fill == 1
    if step >= len(vector):
        return list(np.full((len(vector)), fill))
    else:
        left_part = list(vector[step:])
        right_part = list(np.ones(step))
        return left_part + right_part


def get_cdf_function(data_set, bin_interval):
    max_val = max(data_set)
    bin_num = int(max_val / bin_interval)
    print('bin_num', bin_num)
    bins_index = np.arange(0, bin_num, 1)
    bin_val = bins_index * bin_interval
    cdf, bins, _ = plt.hist(data_set, bin_val, density=1, cumulative=True)
    return cdf, bins


def cdf_function(val, cdf, bins, max_cdf=None):
    if max_cdf is None:
        max_cdf = bins[len(bins) - 1]
    step = bins[1] - bins[0]
    return 1 if val >= max_cdf else cdf[int(val / step)]


def get_g(generate_time_interval, cdf, bins, buffer_size, min_buffer_size=None):
    l_list = [-1]
    for i in range(1, buffer_size):
        last = 0 if i == 1 else l_list[len(l_list) - 1]
        l_list.append(last + cdf_function(i * generate_time_interval, cdf, bins))
    g_value_list = []
    n1_list = []
    for g_plus_n1 in range(min_buffer_size, buffer_size):
        g_value = g_plus_n1 - l_list[g_plus_n1]
        n1_value = g_plus_n1 - g_value
        g_value_list.append(g_value)
        n1_list.append(n1_value)
    g_value_list = np.array(g_value_list)
    return np.array(g_value_list), np.array(n1_list), np.array(buffer_size) - np.array(n1_list)


def recommend_n_1(n_1_list, n_2_list, G):
    # calculate N_arrive
    n_arrive = np.divide(np.multiply(n_1_list, n_2_list), G) + n_2_list
    # select the n_1 so that N_arrive is maximized
    max_n_arrive = np.max(n_arrive)
    index = np.argmax(n_arrive)
    return int(n_1_list[index]), int(max_n_arrive)


def int_clause(k_, buffer_size, pdf_function, cdf_function, bin_step, delta_t):
    to_int = np.array(pdf_function[:])
    for shift_ in range(int(1 + k_), int(buffer_size + k_ + 1)):
        # print('shift', shift_)
        # print('multiple', int(delta_t / bin_step))
        to_int *= np.array(shift_left(cdf_function, shift_ * int(delta_t / bin_step), 1))
    return np.sum(to_int) * bin_step, to_int


def zeta(buffer_size, cdf, pdf, bin_step, generate_time_interval, threshold=0.99999999):
    cur_i = 0
    # print('buffer start', buffer_size)
    val, vec = int_clause(cur_i, buffer_size, pdf, cdf, bin_step, generate_time_interval)
    # print('buffer end', buffer_size)
    ret = 1 - val
    while True:
        # print('zeta_function', 'cur_i + 1', cur_i + 1, 'mul', int(generate_time_interval / bin_step))
        vec /= np.array(shift_left(cdf, (cur_i + 1) * int(generate_time_interval / bin_step), 1))
        # print('zeta_function', 'cur_i + buffer_size + 1', cur_i + buffer_size + 1, 'mul', int(generate_time_interval / bin_step))
        # print('buffer', buffer_size)
        vec *= np.array(shift_left(cdf, (cur_i + buffer_size + 1) * int(generate_time_interval / bin_step), 1))
        sum_ = np.sum(vec) * bin_step
        # print(sum_)
        if sum_ >= threshold - 0.0000001:
            break
        ele = 1 - sum_
        ret += ele
        cur_i += 1
    return ret


def zeta_worker(shift_group, buffer_size, cdf, pdf, bin_step, generate_time_interval, lock, value, threshold):
    threshold = 0.00001
    shift_group = sorted(shift_group)
    for shift in shift_group:
        val, vec = int_clause(shift, buffer_size, pdf, cdf, bin_step, generate_time_interval)
        sum_ = np.sum(vec) * bin_step
        ele = 1 - sum_
        if ele < threshold:
            break
        with lock:
            value.value = value.value + ele


def zeta_multi_proc(buffer_size, cdf, pdf, bin_step, generate_time_interval, threshold, proc=190):
    threshold = 0.00001
    assert generate_time_interval == bin_step
    shifts = list(range(len(cdf)))
    # np.random.shuffle(shifts)
    shifts_groups = split_list(shifts, proc)
    processes = []
    ret = multiprocessing.Manager().Value(ctypes.c_double, 0.0)
    lock = multiprocessing.Lock()
    for group in shifts_groups:
        process = multiprocessing.Process(target=zeta_worker,
                                          args=(
                                              group, buffer_size, cdf, pdf, bin_step, generate_time_interval, lock, ret,
                                              threshold))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
    return ret.value


def get_rc(lsm_buffer_size, cdf, pdf, bin_step, generate_time_interval, threshold=0.999999):
    # ret = zeta_multi_proc(lsm_buffer_size, cdf, pdf, bin_step, generate_time_interval,threshold, proc=180) / lsm_buffer_size + 1
    # return ret
    return zeta(lsm_buffer_size, cdf, pdf, bin_step, generate_time_interval, threshold) / lsm_buffer_size + 1


def get_rs(buffer_size, n_1, g, n_arrival, cdf, pdf, bin_step, generate_time_interval, threshold=0.999999):
    n_2 = buffer_size - n_1
    n_ = 1 + n_2 / g[n_1] - int(n_2 / g[n_1])
    # return zeta_multi_proc(n_arrival, cdf, pdf, bin_step, generate_time_interval, threshold, proc=180) / n_arrival + 1 + (
    #         n_2 + n_) / n_arrival
    return zeta(n_arrival, cdf, pdf, bin_step, generate_time_interval, threshold) / n_arrival + 2
    # return zeta(n_arrival, cdf, pdf, bin_step, generate_time_interval, threshold) / n_arrival + 1 + (
    #         n_2 + n_) / n_arrival


def to_pdf(cdf, bin_step):
    pdf = copy.deepcopy(cdf)
    for i in range(len(pdf) - 1, -1, -1):
        pdf[i] = pdf[i] - pdf[i - 1]
    return pdf / bin_step
