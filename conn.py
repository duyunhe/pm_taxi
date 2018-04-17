# -*- coding: utf-8 -*-
# @Time    : 2018/4/16 16:40
# @Author  : 
# @简介    : 测试连接和计价器数据
# @File    : conn.py

from DBConn import oracle_util
from datetime import timedelta, datetime
from geo import calc_dist, bl2xy
import math


class TaxiData:
    def __init__(self, px, py, stime, state, speed):
        self.px, self.py, self.stime, self.state, self.speed = px, py, stime, state, speed
        self.stop_index = 0

    def set_index(self, index):
        self.stop_index = index


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0


def get_jjq(conn, veh):
    sql = "select vhic, shangche, xiache, zhongduan, zhongxin from TB_JJQ t where vhic = '{0}'" \
          " and shangche >= to_date('2017-09-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') and " \
          "shangche < to_date('2017-09-02 00:00:00', 'yyyy-mm-dd hh24:mi:ss') order by shangche".format(veh)
    cursor = conn.cursor()
    cursor.execute(sql)
    rec_list = []
    for item in cursor.fetchall():
        dep_time, dest_time, zd, zx = item[1:]
        dt = dest_time - dep_time
        rec_list.append((veh, dep_time, dest_time, dt.total_seconds() / 60, zd, zx))
    return rec_list


def get_vehicle(conn):
    sql = "select vehicle_num from tb_vehicle where rownum <= 10"
    cursor = conn.cursor()
    cursor.execute(sql)
    veh_list = []
    for item in cursor.fetchall():
        veh = item[0]
        veh_list.append(veh)
    return veh_list


def get_gps_data(conn, begin_time, veh):
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(days=1)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select px, py, speed_time, state, speed from " \
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
            taxi_data = TaxiData(px, py, stime, state, speed)
            trace.append(taxi_data)
    # print len(trace)
    trace.sort(cmp1)

    new_trace = []
    for data in trace:
        cur_point = data
        if last_point is not None:
            dist = calc_dist([cur_point.px, cur_point.py], [last_point.px, last_point.py])
            del_time = (cur_point.stime - last_point.stime).total_seconds()
            if dist > 2000 and del_time < 60:
                continue
            else:
                new_trace.append(data)
        else:
            new_trace.append(data)
        last_point = cur_point
    return new_trace


def print_data(trace):
    for data in trace:
        print data.state, data.speed, data.stime


def print_jjq(jjq_list):
    for data in jjq_list:
        jc_time, dep_time = data[3], data[2]
        str_dep = dep_time.strftime('%Y-%m-%d %H:%M:%S')
        print str_dep, jc_time


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
    for bi, ei, sp in trace_list:
        t = trace[ei].stime - trace[bi].stime
        print trace[ei].stime, t.total_seconds() / 60


def pre_trace(trace):
    n = len(trace)
    for i in range(1, n - 1):
        if trace[i].state == 0 and trace[i - 1].state == 1 and trace[i + 1].state == 1:
            trace[i].state = 1


def is_near_span(x, y):
    return math.fabs(x - y) < 2


def is_near_time(x, y):
    sp = y - x
    return math.fabs(sp.total_seconds()) < 60


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


def match_jjq_gps(trace, trace_list, jjq):
    """
    匹配计价器与GPS数据
    :param trace: gps轨迹 (list)
    :param trace_list: 分割后的每段gps的起点和终点index (list)
    :param jjq: 计价器数据 (list)  (veh, dep_time, dest_time, jc_time, zd, zx)
    :return: 
    """
    offset = get_offset(trace, trace_list, jjq)
    match, offset_time = get_max_match(trace, trace_list, jjq, offset)
    match_list = sorted(match.items(), key=lambda d: d[0])
    for i, j in match_list:
        jjq_dep, jc = jjq[i][2:4]
        adj_dep = jjq_dep + timedelta(minutes=offset_time)
        bi, ei, sp = trace_list[j]
        gps_dep = trace[ei].stime
        print adj_dep, jc, gps_dep, '{0:.2f}'.format(sp)


def main():
    conn = oracle_util.get_connection()
    veh_list = get_vehicle(conn)
    # get_jjq(conn, 'AT8542')
    for veh in veh_list:
        print veh
        begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        trace = get_gps_data(conn, begin_time, veh)
        pre_trace(trace)
        jjq = get_jjq(conn, veh)
        # print_jjq(jjq)
        # print_data(trace)
        t_list = split_trace(veh, trace)
        # print_trace(trace, t_list)
        if len(trace) != 0:
            match_jjq_gps(trace, t_list, jjq)
    conn.close()


main()
