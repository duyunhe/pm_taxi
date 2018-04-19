# -*- coding: utf-8 -*-
# @Time    : 2018/4/19 9:14
# @Author  : 
# @简介    : 获取车辆
# @File    : veh.py

from DBConn import oracle_util
from time import clock


def get_vehicle(conn):
    cursor = conn.cursor()
    sql = 'select vhic from tb_citizen_ss'

    cursor.execute(sql)
    veh_list = []
    veh_set = set()
    for item in cursor.fetchall():
        veh = item[0]
        veh_set.add(veh)
    for veh in veh_set:
        veh_list.append(veh)

    sql = "select * from tb_gps_1709 where vehicle_num = :1 and rownum <= 1"
    hz_list = []
    for veh in veh_list:
        tup = (veh,)
        cursor.execute(sql, tup)
        in_hz = False
        for _ in cursor.fetchall():
            in_hz = True
        if in_hz:
            hz_list.append(veh)

    return hz_list


def save_veh(conn, veh_list):
    cursor = conn.cursor()
    tup_list = []
    for veh in veh_list:
        tup_list.append((veh, ))
    sql = "insert into tb_vehicle (vehicle_num) values(:1)"
    cursor.executemany(sql, tup_list)
    conn.commit()
    cursor.close()


def main():
    conn = oracle_util.get_connection()
    veh_list = get_vehicle(conn)
    save_veh(conn, veh_list)


main()