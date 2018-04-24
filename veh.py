# -*- coding: utf-8 -*-
# @Time    : 2018/4/19 9:14
# @Author  : 
# @简介    : 获取车辆
# @File    : veh.py

from DBConn import oracle_util
from time import clock


def get_vehicle(conn):
    cursor = conn.cursor()
    sql = 'select vehicle_num from tb_vehicle where gps_no_data = 1 and mark != 101'

    cursor.execute(sql)
    veh_list = []
    veh_set = set()
    for item in cursor.fetchall():
        veh = item[0]
        veh_set.add(veh)
    for veh in veh_set:
        veh_list.append(veh)

    return veh_list


def get_last_digit(veh):
    return int(veh[-2:])


def save_veh(conn, veh_list):
    cursor = conn.cursor()
    tup_list = []
    for veh in veh_list:
        mark = 102
        tup_list.append((mark, veh))
    sql = "update tb_vehicle set mark = :1 where vehicle_num = :2"
    cursor.executemany(sql, tup_list)
    conn.commit()
    cursor.close()


def main():
    conn = oracle_util.get_connection()
    veh_list = get_vehicle(conn)
    save_veh(conn, veh_list)
    conn.close()


main()
