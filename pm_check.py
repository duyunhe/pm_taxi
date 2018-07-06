# -*- coding: utf-8 -*-
# @Time    : 2018/6/22 10:48
# @Author  : 
# @简介    : 正式的跑码器程序，检查每一笔营运记录
# @File    : pm_check.py

import cx_Oracle
import time
from datetime import datetime, timedelta
from geo import bl2xy, calc_dist
from taxi_struct import TaxiData, cmp1
import numpy as np
import math
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def split_into_cars(data_list):
    """
    :param data_list: vehicle num, px, py, state, stime
    :return: taxi_trace_map
    """
    taxi_trace_map = {}
    for data in data_list:
        veh, lng, lat, state, speed, stime = data[0:6]
        state = int(state)
        if lng > 121 or lng < 119 or lat > 31 or lat < 29:
            continue
        px, py = bl2xy(lat, lng)
        taxi_data = TaxiData(px, py, stime, state, speed)
        try:
            taxi_trace_map[veh].append(taxi_data)
        except KeyError:
            taxi_trace_map[veh] = [taxi_data, ]
    return taxi_trace_map


def pre_trace(trace):
    trace.sort(cmp1)
    new_trace = []
    last_point = None
    for data in trace:
        cur_point = data
        if last_point is not None:
            dist = calc_dist([cur_point.px, cur_point.py], [last_point.px, last_point.py])
            del_time = (cur_point.stime - last_point.stime).total_seconds()
            if dist > 2000 and del_time < 60:
                continue
            elif del_time <= 5:
                continue
            else:
                new_trace.append(data)
        else:
            new_trace.append(data)
        last_point = cur_point
    return new_trace


def process_jjq(jjq_list):
    vec = []
    for jjq in jjq_list:
        zx, ys = jjq[4:6]
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


def get_jjq(conn, veh, begin_time):
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(days=1)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select vhic, shangche, xiache, jicheng, db_time from tb_citizen_2018 t where vhic = '{0}'" \
          " and shangche >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss') and " \
          "shangche < to_date('{2}', 'yyyy-mm-dd hh24:mi:ss') order by shangche".format(veh, str_bt, str_et)
    cursor = conn.cursor()
    cursor.execute(sql)
    rec_list = []
    last_dep_time = None
    for item in cursor.fetchall():
        dep_time, dest_time, lc, zx = item[1:]
        # 上车，下车，里程，中心时间
        if last_dep_time == dep_time:
            continue
        lc = int(lc)
        try:
            sp = zx - dest_time
            ys = int(sp.total_seconds())
        except TypeError:
            ys = -1
        dt = dest_time - dep_time
        rec_list.append((veh, dep_time, dest_time, dt.total_seconds() / 60, zx, ys, lc))
        last_dep_time = dep_time
    return rec_list


def split_trace(trace):
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


def is_near_span(x, y):
    return math.fabs(x - y) < 2


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


def match_jjq_gps(trace, trace_list, jjq, ys, pos):
    """
    匹配计价器与GPS数据
    :param trace: gps轨迹 (list)
    :param trace_list: 分割后的每段gps的起点和终点index (list)
    :param jjq: 计价器数据 (list)  (veh, dep_time, dest_time, jc_time, zd, zx, yanshi)
    :param ys: 延时seconds
    :param pos: debug用
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
        if pos != -1 and i != pos:
            continue
        jjq_dep, jjq_dest, jc, zd, zx, _, lc = jjq[i][1:]
        adj_dep = jjq_dest + timedelta(minutes=offset_time)
        bi, ei, sp = trace_list[j]
        gps_dest = trace[ei].stime
        # gps直接得到的轨迹
        dist = get_trace_dist(trace, bi, ei, i)
        # print_data(trace, bi, ei)
        # gps匹配得到的轨迹
        # dist1 = get_trace_dist_with_matching(trace, bi, ei, i)

        dist_diff = dist - lc * 100
        # matching_diff = dist1 - lc * 100
        matching_list.dist(dist_diff)
        print i, 'sc: ' + str(jjq_dep), 'xc: ' + str(jjq_dest), 'adj: ' + str(adj_dep), jc, 'zx: ' + str(zx), \
            'zd: ' + str(zd), gps_dest, '{0:.2f}'.format(sp), 'lc: ' + str(lc), 'dist: ' + str(dist), \
            'dist1: ' + str(dist)
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


def query_diary(date):
    conn = cx_Oracle.connect('hz', 'hz', '192.168.11.88:1521/orcl')
    cursor = conn.cursor()
    bt = time.clock()
    begin_time = datetime(2018, 5, date)
    print "date", date
    str_bt = begin_time.strftime('%Y-%m-%d 00:00:00')
    str_et = begin_time.strftime('%Y-%m-%d 23:59:59')

    sql = "select vehicle_num, px, py, state, speed, speed_time from TB_GPS_1805 t" \
          " where speed_time > to_date('{0}', 'yyyy-mm-dd HH24:mi:ss') " \
          "and speed_time < to_date('{1}', 'yyyy-mm-dd HH24:mi:ss')".format(str_bt, str_et)

    sql = "select vehicle_num, px, py, state, speed, speed_time from TB_GPS_1805 t" \
          " where speed_time > to_date('{0}', 'yyyy-mm-dd HH24:mi:ss') " \
          "and speed_time <= to_date('{1}', 'yyyy-mm-dd HH24:mi:ss') and vehicle_num='" \
          "浙ATE586'".format(str_bt, str_et)

    cursor.execute(sql)
    info_list = []
    for item in cursor.fetchall():
        info_list.append(item)
    et = time.clock()
    print "select costs", et - bt
    conn.close()
    bt = time.clock()
    trace_map = split_into_cars(info_list)
    et = time.clock()
    print et - bt

    conn = cx_Oracle.connect('hz', 'hz', '192.168.11.88:1521/orcl')
    for veh, trace in trace_map.iteritems():
        jjq_veh = veh[-6:]
        t_list = split_trace(trace)
        jjq = get_jjq(conn, jjq_veh, begin_time)
        ys_std, ys_median, jjq_len = process_jjq(jjq)

        if len(trace) != 0:
            dif, match, dist_med, dist_mean, map_med = match_jjq_gps(trace, t_list, jjq, ys_median - 126, -1)
            if dif is not None:
                dif = dif * 60
        print len(t_list), ys_median, jjq_len
    conn.close()


def main():
    query_diary(1)


main()

