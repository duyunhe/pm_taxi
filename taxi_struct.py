# -*- coding: utf-8 -*-
# @Time    : 2018/6/22 11:03
# @Author  : 
# @简介    : 出租车数据结构
# @File    : taxi_struct.py


class TaxiData:
    def __init__(self, px, py, stime, state, speed):
        self.px, self.py, self.stime, self.state, self.speed = px, py, stime, state, speed
        self.stop_index = -1

    def set_index(self, index):
        self.stop_index = index


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0