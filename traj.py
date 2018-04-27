import time
import datetime
import re
from geo import bl2xy
from datetime import datetime


class TaxiData:
    def __init__(self, px, py, speed, stime):
        self.px, self.py, self.speed, self.stime = px, py, speed, stime
        self.edge_info = []

    def set_edge(self, edge, score):
        self.edge_info.append([edge, score])


def cmp1(temp0, temp1):
    if temp0.stime < temp1.stime:
        return -1
    else:
        return 1


min_lat = 28.2691000
min_lon = 119.7302000
max_lat = 28.5717000
max_lon = 120.0629000


def set_bounds(b0, b1, l0, l1):
    global min_lat, max_lat, min_lon, max_lon
    min_lat, max_lat, min_lon, max_lon = b0, b1, l0, l1


def str2time(s):
    if len(s) != 14:
        return None
    year, month, date, hour, minute, second = map(int, (s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]))
    return datetime.datetime(year, month, date, hour, minute, second)


def str2geo(s):
    return float(s) / 1000000


def load_trace(filename):
    # one trace
    fp = open(filename)
    orders = {}
    for line in fp.readlines():
        px, py, order = line.split(',')
        x, y = float(px), float(py)
        if order not in orders:
            orders[order] = []
        orders[order].append([x, y])
    data = []
    cnt = 0
    for order, data_list in orders.items():
        data = data_list
        if cnt == 7:
            break
        cnt += 1
    return data


def load_traj(filename):
    # trace data file
    traj = {}

    bt = time.clock()
    fp = open(filename)
    for line in fp.readlines():
        items = line.split(',')
        if items[1] != 'didi':
            continue
        veh_no, com_id, p_time, lat, lon, _, orderid = items[0:7]

        if veh_no not in traj:
            traj[veh_no] = []
        traj[veh_no].append([str2geo(lon), str2geo(lat), str2time(p_time), orderid])
    et = time.clock()
    print "load data", et - bt

    order = {}
    # order['id'] = [[lon, lat, time], [t2].....[tn]]
    for veh_no, item in traj.items():
        for t in item:
            orderid = t[3]
            if orderid not in order:
                order[orderid] = []
            lon, lat = t[0:2]
            if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat:
                order[orderid].append(t[0:3])
    return order


def load_taxi(index):
    traj_list = []
    fp = open('data/trace/lishui_{0}.txt'.format(index))
    cnt = 0
    for line in fp.readlines():
        items = line.strip('\n').split(',')
        # _, px, py, speed, azi, state, speed_time = new_items
        longi, lati, speed = map(float, items[1:4])
        if longi > max_lon or longi < min_lon or lati > max_lat or lati < min_lat:
            continue
        px, py = bl2xy(lati, longi)
        speed_time = datetime.strptime(items[5], "%Y-%m-%d %H:%M:%S")
        data = TaxiData(px, py, speed, speed_time)
        traj_list.append(data)
    traj_list.sort(cmp1)
    return traj_list


def print_taxi(data_list):
    fp = open('traj1.txt', 'w')
    for data in data_list:
        write_str = "{0},{1},{2},{3}\n".format(data.px, data.py, data.speed, data.stime)
        fp.write(write_str)
    fp.close()


def load_lishui_taxi(filename):
    traj_list = []
    fp = open(filename, 'r')
    for line in fp.readlines():
        items = line.strip('\n').split(',')
        px, py = float(items[0]), float(items[1])
        stime = datetime.strptime(items[3], '%Y-%m-%d %H:%M:%S')
        traj_list.append(TaxiData(px, py, 0, stime))
    return traj_list

