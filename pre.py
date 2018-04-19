# -*- coding: utf-8 -*-
# @Time    : 2018/4/18 17:15
# @Author  : 
# @简介    : 
# @File    : pre.py


def get_new_veh():
    fp = open("new.csv")
    veh_list = []
    for line in fp.readlines():
        items = line.strip('\n').split(',')
        veh = items[0]
        veh_list.append(veh)
    return veh_list


def main():
    fp = open("pm.csv")
    for line in fp.readlines():
        items = line.strip('\n').split(',')

