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


def split_into_cars(data_list):
    """
    :param data_list: vehicle num, px, py, state, stime
    :return: taxi_trace_map
    """
    taxi_trace_map = {}
    for data in data_list:
        veh, lng, lat, state, speed, stime = data[0:6]
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


def query_diary(date):
    conn = cx_Oracle.connect('hz', 'hz', '192.168.11.88:1521/orcl')
    cursor = conn.cursor()
    bt = time.clock()
    begin_time = datetime(2018, 5, date)
    print "date", date
    str_bt = begin_time.strftime('%Y-%m-%d 08:00:00')
    str_et = begin_time.strftime('%Y-%m-%d 20:00:00')

    sql = "select vehicle_num, px, py, state, speed, speed_time from TB_GPS_1805 t" \
          " where speed_time > to_date('{0}', 'yyyy-mm-dd HH24:mi:ss') " \
          "and speed_time < to_date('{1}', 'yyyy-mm-dd HH24:mi:ss')".format(str_bt, str_et)
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
    bt = time.clock()

    conn.close()
