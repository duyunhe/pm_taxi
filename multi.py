# -*- coding: utf-8 -*-
# @Time    : 2018/4/19 14:45
# @Author  : 
# @简介    : 多线程处理gps数据
# @File    : multi.py


import threading
import conn
from datetime import datetime


class MyThread(threading.Thread):
    def __init__(self, mark, bt):
        threading.Thread.__init__(self)
        self.mark, self.begin_time = mark, bt

    def run(self):
        print self.mark, "starting"
        conn.zx_with_zd_time(self.mark, self.begin_time)


if __name__ == '__main__':
    for i in range(10):
        begin_time = datetime.strptime('2017-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        t = MyThread(i, begin_time)
        t.start()


