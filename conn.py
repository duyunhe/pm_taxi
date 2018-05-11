# -*- coding: utf-8 -*-
# @Time    : 2018/4/16 16:40
# @Author  : 
# @简介    : 测试连接和计价器数据
# @File    : conn.py

import cx_Oracle
from datetime import timedelta, datetime
from math import *
import numpy as np
from geo import bl2xy, calc_dist
import pre
from multiprocessing import Process, Pool
import time
import os
import random
from traj import pre_traj
import map_matching


def get_distance(latA, lonA, latB, lonB):
    ra = 6378140  # radius of equator: meter
    rb = 6356755  # radius of polar: meter
    flatten = (ra - rb) / ra  # Partial rate of the earth
    if latA == latB and lonA == lonB:
        return 0.0
    # change angle to radians
    radLatA = radians(latA)
    radLonA = radians(lonA)
    radLatB = radians(latB)
    radLonB = radians(lonB)

    pA = atan(rb / ra * tan(radLatA))
    pB = atan(rb / ra * tan(radLatB))
    x = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(radLonA - radLonB))
    try:
        c1 = (sin(x) - x) * (sin(pA) + sin(pB)) ** 2 / cos(x / 2) ** 2
        c2 = (sin(x) + x) * (sin(pA) - sin(pB)) ** 2 / sin(x / 2) ** 2
    except ZeroDivisionError:
        return 0.0
    dr = flatten / 8 * (c1 - c2)
    distance = ra * (x + dr)
    return distance


class TaxiData:
    def __init__(self, px, py, stime, state, speed, car_state, direction):
        self.px, self.py, self.stime, self.state, self.speed = px, py, stime, state, speed
        self.stop_index, self.dist, self.car_state, self.direction = 0, 0, car_state, direction
        self.angle = 0

    def set_index(self, index):
        self.stop_index = index

    def set_angle(self, angle):
        self.angle = angle


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0


def get_jjq(conn, veh, begin_time):
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(days=1)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select vhic, shangche, xiache, zhongduan, zhongxin, jicheng from TB_JJQ t where vhic = '{0}'" \
          " and shangche >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss') and " \
          "shangche < to_date('{2}', 'yyyy-mm-dd hh24:mi:ss') order by shangche".format(veh, str_bt, str_et)
    cursor = conn.cursor()
    cursor.execute(sql)
    rec_list = []
    last_dep_time = None
    for item in cursor.fetchall():
        dep_time, dest_time, zd, zx, lc = item[1:]
        if last_dep_time == dep_time:
            continue
        lc = int(lc)
        try:
            sp = zx - dest_time
            ys = int(sp.total_seconds())
        except TypeError:
            ys = -1
        dt = dest_time - dep_time
        rec_list.append((veh, dep_time, dest_time, dt.total_seconds() / 60, zd, zx, ys, lc))
        last_dep_time = dep_time
    return rec_list


def get_vehicle(conn):
    sql = "select vehicle_num from tb_vehicle where el = 0 and gps_no_data = 0 and rownum < 10000"
    cursor = conn.cursor()
    cursor.execute(sql)
    veh_list = []
    for item in cursor.fetchall():
        veh = item[0]
        veh_list.append(veh)
    return veh_list


def get_vehicle_spe(conn, mark):
    sql = "select rownum, t.* from TB_VEHICLE t where zd_median < -500 and zd_mean < -500 and to_number(match_point) >= 7" \
          " and gps_median <= 30 and carstate_per > 60 and map_med is NULL"
    cursor = conn.cursor()
    cursor.execute(sql)
    veh_list = []
    for item in cursor.fetchall():
        row = int(item[0])
        veh = item[1]
        if row % 10 == mark:
            veh_list.append(veh)
    return veh_list


def get_vehicle_by_mark(conn, mark):
    sql = "select vehicle_num from tb_vehicle where mark = '{0}' and rownum <= 10000".format(mark)
    cursor = conn.cursor()
    cursor.execute(sql)
    veh_list = []
    for item in cursor.fetchall():
        veh = item[0]
        veh_list.append(veh)
    return veh_list


def exclude_abnormal(data_list):
    vec = np.array(data_list)
    n = len(vec)
    if n == 0:
        return 0.0, 0.0, 0
    arr = np.array(vec)
    # print vec
    qu, qd = np.percentile(arr, 75), np.percentile(arr, 25)
    itv = qu - qd
    zu, zd = qu + 1.5 * itv, qd - 1.5 * itv
    vec = []
    for i in range(n):
        if zd <= arr[i] <= zu:
            vec.append(arr[i])
    arr = np.array(vec)
    return len(arr), np.mean(arr), np.median(arr)


def get_gps_data(conn, begin_time, veh):
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(days=1)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select px, py, speed_time, state, speed, carstate, direction from " \
          "TB_GPS_1709 t where speed_time >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss') " \
          "and speed_time < to_date('{2}', 'yyyy-MM-dd hh24:mi:ss')" \
          " and vehicle_num = '{0}'".format(veh, str_bt, str_et)
    cursor = conn.cursor()
    cursor.execute(sql)

    trace = []
    last_point = None
    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            carstate = int(item[5])
            dir = float(item[6])
            taxi_data = TaxiData(px, py, stime, state, speed, carstate, dir)
            trace.append(taxi_data)
    # print len(trace)
    trace.sort(cmp1)

    new_trace = []
    for data in trace:
        cur_point = data
        if last_point is not None:
            dist = calc_dist([cur_point.px, cur_point.py], [last_point.px, last_point.py])
            del_time = (cur_point.stime - last_point.stime).total_seconds()
            if data.speed > 140 or del_time < 5:            # 速度阈值 时间间隔阈值
                continue
            elif dist > data.speed * 3.6 * del_time * 2:    # 距离阈值
                continue
            elif data.speed == last_point.speed and data.speed > 0 and data.direction == last_point.direction:
                # 非精确
                continue
            else:
                data.dist = dist
                # del_list.append(del_time)
                new_trace.append(data)
        else:
            data.dist = 0
            new_trace.append(data)
        last_point = cur_point
    # gps_point, gps_mean, gps_med = exclude_abnormal(del_list)

    return new_trace


def print_data(trace, bi, ei):
    for data in trace[bi:ei + 1]:
        print data.state, data.car_state, data.speed, data.stime, data.px, data.py, data.dist
        

def print_data_by_time(trace, begin_time, end_time):
    for data in trace:
        if data.stime >= begin_time:
            print data.state, data.car_state, data.speed, data.stime, data.px, data.py, data.dist
        if data.stime > end_time:
            break


def print_jjq(jjq_list):
    print "================jjq========================"
    for data in jjq_list:
        dep_time, jc_time, dest_time, zd_time, zx_time = data[1], data[3], data[2], data[4], data[5]
        str_dep = dest_time.strftime('%Y-%m-%d %H:%M:%S')
        print 'sc: ' + str(dep_time), 'xc: ' + str(str_dep), jc_time, 'zd: ' + str(zd_time), 'zx: ' + str(zx_time)


def split_trace(veh, trace):
    trace_list = []
    last_state = -1
    bi, ei, idx = -1, -1, 0
    for data in trace:
        state = data.state
        if state != last_state:
            if state == 0:          # 重车结束
                load_len = ei - bi + 1
                if load_len >= 2:   # 跳重车干扰
                    sp = trace[ei].stime - trace[bi].stime
                    trace_list.append([bi, ei, sp.total_seconds() / 60])
            else:      # 新的重车开始
                bi = idx
        else:
            ei = idx
        idx += 1
        last_state = state
    return trace_list


def print_trace(trace, trace_list):
    print "================trace========================"
    for bi, ei, sp in trace_list:
        t = trace[ei].stime - trace[bi].stime
        print trace[bi].stime, trace[ei].stime, t.total_seconds() / 60


def pre_trace(trace):
    n = len(trace)
    for i in range(1, n - 1):
        if trace[i].state == 0 and trace[i - 1].state == 1 and trace[i + 1].state == 1:
            trace[i].state = 1


def is_near_span(x, y):
    return fabs(x - y) < 2


def is_near_time(x, y):
    sp = y - x
    return fabs(sp.total_seconds()) < 60


def get_offset(trace, trace_list, jjq):
    off_set = set()
    m, n = len(trace_list), len(jjq)
    for i in range(n):
        jc, jjq_dep = jjq[i][3], jjq[i][2]
        for j in range(m):
            bi, ei, sp = trace_list[j]
            gps_dep = trace[ei].stime
            off = int((gps_dep - jjq_dep).total_seconds() / 60)
            if is_near_span(jc, sp):
                off_set.add(off)
    return off_set


def get_max_match(trace, trace_list, jjq, offset):
    m, n = len(trace_list), len(jjq)
    max_match_cnt = 0
    match = {}
    sel_off = None  # 以计价器时间为基准的偏移时间
    for off in offset:
        cnt = 0
        temp_match = {}
        for i in range(n):
            jjq_dep, jc = jjq[i][2:4]
            tar_dep = jjq_dep + timedelta(minutes=off)
            for j in range(m):
                bi, ei, sp = trace_list[j]
                gps_dep = trace[ei].stime
                if is_near_time(tar_dep, gps_dep) and is_near_span(jc, sp):
                    cnt += 1
                    temp_match[i] = j
                    break
        if cnt > max_match_cnt:
            max_match_cnt, match, sel_off = cnt, temp_match, off
    return match, sel_off


def get_max_match1(trace, trace_list, jjq, offset):
    m, n = len(trace_list), len(jjq)
    max_match_cnt = 1
    match = {}
    sel_off = None      # 以计价器时间为基准的偏移时间
    for off in offset:
        cnt, jq_j = 0, 0
        temp_match = {}
        for i in range(n):
            jjq_dep, jc = jjq[i][2:4]
            tar_dep = jjq_dep + timedelta(minutes=off)
            for j in range(jq_j, m):
                bi, ei, sp = trace_list[j]
                gps_dep = trace[ei].stime
                if is_near_time(tar_dep, gps_dep) and is_near_span(jc, sp):
                    cnt += 1
                    temp_match[i] = j
                    jq_j = j + 1
                    break
        if cnt > max_match_cnt:
            max_match_cnt, match, sel_off = cnt, temp_match, off
    return match, sel_off


def get_trace_dist_with_matching(trace, bi, ei, ti):
    if ti >= 0:
        dist = map_matching.matching_draw(trace[bi: ei + 1])
        return dist
    else:
        dist = map_matching.matching(trace[bi: ei + 1])
        return dist


def get_trace_dist(trace, bi, ei, ti):
    """
    :param trace: 轨迹(list)
    :param bi: 起点
    :param ei: 终点
    :return: 
    """
    trace_len = ei - bi + 1     # 轨迹长度
    final_dist = 0
    cnt_imp, cnt_spd = 0, 0
    last_speed = -1
    for i in range(bi + 1, ei + 1):
        data = trace[i]
        if data.car_state == 1:
            cnt_imp += 1
        if data.speed == last_speed and data.dist == 0:
            cnt_spd += 1
        last_speed = data.speed
        final_dist += data.dist
    if ti == 300:
        for i in range(bi - 1, ei + 2):
            data = trace[i]
            print data.state, data.car_state, data.speed, data.stime, data.dist
    if float(cnt_imp) / trace_len > 0.3:
        return -1
    if float(cnt_spd) / trace_len > 0.3:
        return -2
    return final_dist


def get_trace_dist_from_time(trace, bt, et, ti):
    """
    :param trace: 轨迹(list)
    :param bt: 起始时间
    :param et: 终止时间
    :return: 
    """
    final_dist = 0
    cnt_imp, cnt_spd = 0, 0
    last_speed = -1
    bi, ei = None, None
    idx = 0
    for data in trace:
        if bi is None and data.stime > bt:
            bi = idx
        if ei is None and data.stime > et:
            ei = idx
            break
        idx += 1
    if bi is None or ei is None:
        return -3, 0
    total_cnt = (et - bt).total_seconds() / 20
    if total_cnt <= 0:
        return -5, 0
    trace_len = ei - bi + 1
    dist1 = get_trace_dist_with_matching(trace, bi, ei, ti)
    for i in range(bi, ei + 1):
        data = trace[i]
        if data.car_state == 1:
            cnt_imp += 1
        if data.speed == last_speed and data.dist == 0:
            cnt_spd += 1
        last_speed = data.speed
        final_dist += data.dist

    if float(trace_len) / total_cnt < 0.3:
        return -4, dist1
    if float(cnt_imp) / trace_len > 0.15:
        return -1, 0
    if float(cnt_spd) / trace_len > 0.3:
        return -2, 0
    return final_dist, dist1


def match_jjq_gps(trace, trace_list, jjq, ys, pos):
    """
    匹配计价器与GPS数据
    :param trace: gps轨迹 (list)
    :param trace_list: 分割后的每段gps的起点和终点index (list)
    :param jjq: 计价器数据 (list)  (veh, dep_time, dest_time, jc_time, zd, zx, yanshi)
    :param ys: 延时seconds
    :return: 
    """
    offset = get_offset(trace, trace_list, jjq)
    match, offset_time = get_max_match1(trace, trace_list, jjq, offset)
    diff_list = []
    diff_median, diff_mean = None, None
    match_list = sorted(match.items(), key=lambda d: d[0])
    jjq_vis = [0] * len(jjq)
    matching_list = []      # matching 后得到的difference

    for i, j in match_list:
        if i != pos:
            continue
        jjq_dep, jjq_dest, jc, zd, zx, _, lc = jjq[i][1:]
        adj_dep = jjq_dest + timedelta(minutes=offset_time)
        bi, ei, sp = trace_list[j]
        gps_dest = trace[ei].stime
        dist = get_trace_dist(trace, bi, ei, i)
        dist1 = get_trace_dist_with_matching(trace, bi, ei, i)

        dist_diff = dist - lc * 100
        matching_diff = dist1 - lc * 100
        matching_list.append(matching_diff)
        print i, 'sc: ' + str(jjq_dep), 'xc: ' + str(jjq_dest), 'adj: ' + str(adj_dep), jc, 'zx: ' + str(zx), \
            'zd: ' + str(zd), gps_dest, '{0:.2f}'.format(sp), 'lc: ' + str(lc), 'dist: ' + str(dist), \
            'dist1: ' + str(dist1)
        if dist >= 0:
            diff_list.append(dist_diff)
        jjq_vis[i] = 1
    for i in range(len(jjq)):
        if i != pos:
            continue
        if jjq_vis[i] == 0:
            jjq_dep, jjq_dest = jjq[i][1:3]
            lc = jjq[i][7]
            gps_dep, gps_dest = jjq_dep + timedelta(seconds=ys), jjq_dest + timedelta(seconds=ys)
            dist, dist1 = get_trace_dist_from_time(trace, gps_dep, gps_dest, i)
            dist_diff = dist - lc * 100
            matching_diff = dist1 - lc * 100
            # if dist1 >= 0:
            #     matching_list.append(matching_diff)
            print i, 'sc: ' + str(jjq_dep), 'xc: ' + str(jjq_dest), 'lc: ' + str(lc), 'dist: ' + str(dist), \
                'dist1: ' + str(dist1)
            if dist >= 0:
                diff_list.append(dist_diff)

    if len(diff_list) != 0:
        vec = np.array(diff_list)
        diff_median = np.median(vec)
        diff_mean = np.mean(vec)
    vec = np.array(matching_list)
    matching_med = np.median(vec)

    return offset_time, match_list, diff_median, diff_mean, matching_med


def process_jjq(jjq_list):
    vec = []
    for jjq in jjq_list:
        zx, zd, ys = jjq[4:7]
        vec.append(ys)
    n = len(vec)
    if n == 0:
        return 0.0, 0.0, 0
    arr = np.array(vec)
    # print vec
    qu, qd = np.percentile(arr, 75), np.percentile(arr, 25)
    itv = qu - qd
    zu, zd = qu + 1.5 * itv, qd - 1.5 * itv
    vec = []
    for i in range(n):
        if zd <= arr[i] <= zu:
            vec.append(arr[i])
    arr = np.array(vec)

    return np.std(arr), np.median(arr), n


def process_data(trace):
    if len(trace) == 0:
        return 0
    cnt = 0
    for data in trace:
        if data.car_state == 0:
            cnt += 1
    return 100.0 * cnt / len(trace)


def main():
    conn = cx_Oracle.connect('lishui', 'lishui', '192.168.11.88:1521/orcl', threaded=True)
    veh_list = get_vehicle(conn)
    # get_jjq(conn, 'AT8542')
    # veh_list = pre.get_new_veh()

    for veh in veh_list:
        print veh,
        begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        trace = get_gps_data(conn, begin_time, veh)
        print len(trace),
        pre_trace(trace)
        jjq = get_jjq(conn, veh)
        process_jjq(jjq)

        # print_jjq(jjq)
        # print_data(trace)
        t_list = split_trace(veh, trace)
        print len(jjq), len(t_list)

        # print_trace(trace, t_list)
        # if len(trace) != 0:
        #     match_jjq_gps(trace, t_list, jjq)

    conn.close()


def zx_with_zd_time(mark, begin_time):
    print mark
    bt = time.clock()
    map_matching.read_xml('hz.xml')
    conn = cx_Oracle.connect('lishui', 'lishui', '192.168.11.88:1521/orcl', threaded=True)

    veh_list = get_vehicle_spe(conn, mark)
    veh_list = ['ATC160']
    cursor = conn.cursor()
    tup_list = []
    sql = "update tb_vehicle set el = :1, gps_no_data = :2, dif2 = :3, match_point = :4, jjq_point" \
          "=:5, map_med = :6 where vehicle_num = :7"
    idx = 0
    for veh in veh_list:
        print veh
        idx += 1
        if idx % 10 == 0:
            print mark, idx
        # begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        jjq = get_jjq(conn, veh, begin_time)
        trace = get_gps_data(conn, begin_time, veh)
        # print_data(trace)
        pre_trace(trace)
        # trace = pre_traj(trace)
        t_list = split_trace(veh, trace)
        # print_jjq(jjq)
        # print_trace(trace, t_list)
        ys_std, ys_median, jjq_len = process_jjq(jjq)
        dif, dist_med, dist_mean = None, None, None
        gps_no_data, el = 0, 0
        if len(trace) < 360:
            gps_no_data = 1
        elif jjq_len > 0 and len(t_list) == 0:
            el = 1
        match = []
        map_med = None
        if len(trace) != 0:
            dif, match, dist_med, dist_mean, map_med = match_jjq_gps(trace, t_list, jjq, ys_median - 126, 4)
            if dif is not None:
                dif = dif * 60
        tup = (el, gps_no_data, dif, len(match), len(jjq), map_med, veh)
        tup_list.append(tup)
        cursor.executemany(sql, tup_list)
        # conn.commit()
        tup_list = []

    conn.close()
    et = time.clock()
    # bt = datetime.strptime('2017-09-01 14:32:00', '%Y-%m-%d %H:%M:%S')
    # et = datetime.strptime('2017-09-01 14:41:00', '%Y-%m-%d %H:%M:%S')
    # print_data_by_time(trace, bt, et)
    # print "mark cost ", et - bt


def ys_with_jjq(mark, begin_time):
    bt = time.clock()
    conn = cx_Oracle.connect('lishui', 'lishui', '192.168.11.88:1521/orcl', threaded=True)

    veh_list = get_vehicle_by_mark(conn, mark)
    # veh_list = ['AT4154']
    cursor = conn.cursor()
    tup_list = []
    idx = 0

    sql = "update tb_vehicle set gps_median = :1, gps_point = :2 where vehicle_num = :3"
    for veh in veh_list:
        idx += 1
        if idx % 50 == 0:
            print mark, idx
        trace, trace_len, gps_med = get_gps_data(conn, begin_time, veh)
        # ys_std, ys_median, jjq_len = process_jjq(jjq)
        tup = (gps_med, trace_len, veh)
        tup_list.append(tup)

    cursor.executemany(sql, tup_list)
    conn.commit()
    conn.close()

    et = time.clock()
    print "mark", mark, "gps cost ", et - bt


def cmp_gps_meter(mark, begin_time):
    conn = cx_Oracle.connect('lishui', 'lishui', '192.168.11.88:1521/orcl', threaded=True)
    veh_list = get_vehicle_by_mark(conn, mark)
    # veh_list = ['AT2931']
    cursor = conn.cursor()
    tup_list = []
    sql = "update tb_vehicle set carstate_per = :0 where vehicle_num = :1"
    idx = 0
    for veh in veh_list:
        # print veh,
        idx += 1
        if idx % 10 == 0:
            print mark, idx
        # begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        trace = get_gps_data(conn, begin_time, veh)
        per = process_data(trace)


def empty_or_load_check(mark, begin_time):
    """
    检查空重车不变化的情况
    :return: 
    """
    conn = cx_Oracle.connect('lishui', 'lishui', '192.168.11.88:1521/orcl', threaded=True)
    veh_list = get_vehicle_by_mark(conn, mark)
    # veh_list = ['AT2931']
    cursor = conn.cursor()
    tup_list = []
    sql = "update tb_vehicle set carstate_per = :0 where vehicle_num = :1"
    idx = 0
    for veh in veh_list:
        # print veh,
        idx += 1
        if idx % 10 == 0:
            print mark, idx
        # begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        trace = get_gps_data(conn, begin_time, veh)
        per = process_data(trace)
        # t_list = split_trace(veh, trace)
        # print_trace(trace, t_list)
        # jjq = get_jjq(conn, veh, begin_time)
        # print len(trace), len(jjq), len(t_list)
        # gps_no_data, el = 0, 0
        # if len(trace) < 360:
        #     gps_no_data = 1
        # elif len(jjq) > 0 and len(t_list) == 0:
        #     el = 1
        tup = (per, veh)
        tup_list.append(tup)
        # cursor.execute(sql)

    cursor.executemany(sql, tup_list)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    bt = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    zx_with_zd_time(9, bt)
