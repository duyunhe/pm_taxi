# coding=utf-8
from xml.etree import ElementTree as ET
import matplotlib.pyplot as plt
from sklearn.neighbors import KDTree
import math
import Queue
from geo import point2segment, point_project, calc_dist, bl2xy, is_near_segment, calc_included_angle
from time import clock
import numpy as np

color = ['r-', 'b-', 'g-', 'c-', 'm-', 'y-', 'c-', 'r-', 'b-', 'orchid', 'm--', 'y--', 'c--', 'k--', 'r:']
region = {'primary': 0, 'secondary': 1, 'tertiary': 2,
          'unclassified': 5, 'trunk': 3, 'service': 4, 'trunk_link': 6,
          'primary_link': 7, 'secondary_link': 8}

EDGE_ONEWAY = 3
EDGES = 2
EDGE_INDEX = 4
EDGE_LENGTH = 5
NODE_EDGELIST = 2

map_node_dict = {}
map_edge_list = []
map_way = {}
# global data structure
nodeid_list = []


class DistNode(object):
    def __init__(self, ndid, dist):
        self.ndid = ndid
        self.dist = dist

    def __lt__(self, other):
        return self.dist < other.dist


class MapNode(object):
    """
    点表示
    point([px,py]), nodeid, link_list
    维护dict，key=nodeid, value=MapNode
    """
    def __init__(self, point, nodeid):
        self.point, self.nodeid = point, nodeid
        self.link_list = []     # 连接到其他点的列表, [[edge0, node0], [edge1, node1]....]

    def add_link(self, edge, node):
        self.link_list.append([edge, node])


class MapEdge(object):
    """
    线段表示
    nodeid_0, nodeid_1,
    oneway(true or false), edge_index, edge_length
    维护list[MapEdge]
    """
    def __init__(self, nodeid_0, nodeid_1, oneway, edge_index, edge_length):
        self.nodeid_0, self.nodeid_1 = nodeid_0, nodeid_1
        self.oneway = oneway
        self.edge_index = edge_index
        self.edge_length = edge_length


def edge2xy(e, node):
    return node[e[0]][0], node[e[0]][1], node[e[1]][0], node[e[1]][1]


def cal_max_way(way):
    max_dist = 0
    for w in way:
        lx, ly = 0, 0
        pl = way[w]
        for t in pl['node']:
            x, y = t[0], t[1]
            if lx != 0:
                dist = math.sqrt((lx - x) ** 2 + (ly - y) ** 2)
                if max_dist < dist:
                    max_dist = dist
            lx, ly = x, y
    return max_dist


def draw_map():
    for i in map_way:
        pl = map_way[i]
        node_list = pl['node']
        x, y = [], []
        for nodeid in node_list:
            x.append(map_node_dict[nodeid].point[0])
            y.append(map_node_dict[nodeid].point[1])

        try:
            c = color[region[pl['highway']]]
            plt.plot(x, y, c, alpha=0.3)
        except KeyError:
            continue
        # if 'name' in pl:
        #     name = pl['name']
        #     plt.text(x[0] + 10, y[0] + 10, name)


def draw_seg(seg, c):
    x, y = zip(*seg)
    plt.plot(x, y, c, linewidth=2)


def draw_edge_set(edge, edge_set, node):
    for i in edge_set:
        draw_edge(edge[i], 'b', node)


def draw_edge(e, c, node):
    x0, y0, x1, y1 = edge2xy(e, node)
    x, y = [x0, x1], [y0, y1]
    plt.plot(x, y, c, linewidth=2)


def draw_nodes(node_list):
    x, y = [], []
    for node in node_list:
        x.append(node[0])
        y.append(node[1])
    plt.plot(x, y, 'mo', markersize=5)


def draw_points(points):
    x, y = zip(*points)
    plt.plot(x, y, 'ro', markersize=4)


def draw_point(point, c):
    """
    :param point: [x, y]
    :return: 
    """
    plt.plot([point[0]], [point[1]], c, markersize=4)


def get_trace_dist(trace):
    last_point = None
    dist = 0.0
    for point in trace:
        if last_point is not None:
            dist += calc_dist(point, last_point)
        last_point = point
    return dist


def read_xml(filename):
    bt = clock()
    tree = ET.parse(filename)
    p = tree.find('meta')
    nds = p.findall('node')
    for x in nds:
        node_dic = x.attrib
        nodeid = node_dic['id']
        dx, dy = bl2xy(float(node_dic['lat']), float(node_dic['lon']))
        node = MapNode([dx, dy], nodeid)
        map_node_dict[nodeid] = node
    wys = p.findall('way')
    for w in wys:
        way_dic = w.attrib
        wid = way_dic['id']
        node_list = w.findall('nd')
        map_way[wid] = {}
        oneway = False
        ref = map_way[wid]
        tag_list = w.findall('tag')
        for tag in tag_list:
            tag_dic = tag.attrib
            ref[tag_dic['k']] = tag_dic['v']
        if 'oneway' in ref:
            oneway = ref['oneway'] == 'yes'

        node_in_way = []
        for nd in node_list:
            node_dic = nd.attrib
            node_in_way.append(node_dic['ref'])
        ref['node'] = node_in_way
        last_nd = ''
        ref['edge'] = []
        for nd in node_in_way:
            if last_nd != '':
                edge_index = len(map_edge_list)
                ref['edge'].append(edge_index)
                p0, p1 = map_node_dict[last_nd].point, map_node_dict[nd].point
                edge_length = calc_dist(p0, p1)
                edge = MapEdge(last_nd, nd, oneway, edge_index, edge_length)
                map_edge_list.append(edge)
            last_nd = nd

    for edge in map_edge_list:
        n0, n1 = edge.nodeid_0, edge.nodeid_1
        if edge.oneway is True:
            map_node_dict[n0].add_link(edge, n1)
        else:
            map_node_dict[n0].add_link(edge, n1)
            map_node_dict[n1].add_link(edge, n0)
    et = clock()
    print "load xml", et - bt


def get_trace_from_project(node, last_point, last_edge, cur_point, cur_edge, cnt):
    pq = Queue.PriorityQueue(maxsize=-1)
    x0, y0, x1, y1 = edge2xy(last_edge, node)
    rx, ry, _ = point_project(last_point[0], last_point[1], x0, y0, x1, y1)
    dist0, dist1 = calc_dist([rx, ry], [x0, y0]), calc_dist([rx, ry], [x1, y1])
    # 短路径优先，因此每个点只会访问一次
    # 在出队列时加入访问set中
    vis_set = set()
    if last_edge[EDGE_ONEWAY] is True:
        pq.put(DistNode(last_edge[1], dist1))
    else:
        pq.put(DistNode(last_edge[0], dist0))
        pq.put(DistNode(last_edge[1], dist1))

    x0, y0, x1, y1 = edge2xy(cur_edge, node)
    sx, sy, _ = point_project(cur_point[0], cur_point[1], x0, y0, x1, y1)
    dist0, dist1 = calc_dist([sx, sy], [x0, y0]), calc_dist([sx, sy], [x1, y1])
    obj0, obj1 = None, None
    if cur_edge[EDGE_ONEWAY] is True:
        obj0 = cur_edge[0]
    else:
        obj0, obj1 = cur_edge[0], cur_edge[1]

    if last_edge == cur_edge:
        # 就是同一条边
        return [[rx, ry], [sx, sy]]

    print_node = []
    # 维护一个反向链表last_node, ndn->...->nd3->nd2->nd1->nd0
    last_node = {}
    final_dist = 1e20
    while not pq.empty():
        cur_node = pq.get()
        cur_id, cur_dist = cur_node.ndid, cur_node.dist
        vis_set.add(cur_id)
        if cur_id == 'final':
            break
        print_node.append(node[cur_id])
        # 到达终点
        if cur_id == obj0:
            next_dist = cur_dist + dist0
            pq.put(DistNode('final', next_dist))
            if next_dist < final_dist:
                last_node['final'], final_dist = obj0, next_dist
            continue
        elif cur_id == obj1:
            next_dist = cur_dist + dist1
            pq.put(DistNode('final', next_dist))
            if next_dist < final_dist:
                last_node['final'], final_dist = obj1, next_dist
            continue
        edge_list = node[cur_id][EDGES]
        for e, nd in edge_list:
            next_dist = cur_dist + e[EDGE_LENGTH]
            if nd in vis_set:
                continue
            pq.put(DistNode(nd, next_dist))
            last_node[nd] = cur_id

    path = []
    cur_id = 'final'
    while cur_id in last_node:
        cur_id = last_node[cur_id]
        path.append(cur_id)
    path.reverse()
    trace = []
    trace.append([rx, ry])
    for nd in path:
        trace.append([node[nd][0], node[nd][1]])
    trace.append([sx, sy])
    return trace


def make_kdtree(node):
    nd_list = []
    for key, item in node.items():
        nodeid_list.append(key)
        nd_list.append([item[0], item[1]])
    X = np.array(nd_list)
    return KDTree(X, leaf_size=2, metric="euclidean"), X


def get_first_point(point, kdt, X, node, edge):
    dist, ind = kdt.query([point], k=30)

    pts = []
    seg_set = set()
    for i in ind[0]:
        pts.append([X[i][0], X[i][1]])
        nid = nodeid_list[i]
        edge_list = node[nid][EDGES]
        for e, nd in edge_list:
            seg_set.add(e[4])

    min_dist, sel = 1e20, -1
    for idx in seg_set:
        n0, n1 = edge[idx][0:2]
        p0, p1 = node[n0][0:2], node[n1][0:2]
        dist = point2segment(point[0], point[1], p0[0], p0[1], p1[0], p1[1])
        if min_dist > dist:
            min_dist, sel = dist, idx

    x0, y0, x1, y1 = node[edge[sel][0]][0], node[edge[sel][0]][1], node[edge[sel][1]][0], node[edge[sel][1]][1]
    x, y = point[0:2]
    rx, ry, _ = point_project(x, y, x0, y0, x1, y1)
    return rx, ry, edge[sel]


def get_latter_point(point, last_point, node, edge, last_edge, cnt=-1):
    """
    :param point: [x, y] 本次待匹配的GPS原始点
    :param last_point: [x, y] 上次匹配成功的GPS点
    :param node: 点数据结构
    :param edge: 边数据结构
    :param last_edge: 上一次匹配到的道路边
    :param cnt: 测试用，当前点的index
    :return: 
    """
    # calculate the maximum distance one car can drive at 80km/s speed in 30s
    dist_thread = 80000 / 120
    node_set = set()
    edge_set = set()
    # first
    x0, y0, x1, y1 = edge2xy(last_edge, node)
    rx, ry, _ = point_project(last_point[0], last_point[1], x0, y0, x1, y1)
    dist0, dist1 = calc_dist([rx, ry], [x0, y0]), calc_dist([rx, ry], [x1, y1])
    # bfs, FIFO
    candidate = Queue.Queue(maxsize=-1)

    candidate.put([last_edge[0], dist0])
    candidate.put([last_edge[1], dist1])
    node_set.add(last_edge[0])
    node_set.add(last_edge[1])
    edge_set.add(last_edge[EDGE_INDEX])

    while not candidate.empty():
        # 搜索点
        cur_nodeid, cur_dist = candidate.get()
        # 遍历边
        edge_list = node[cur_nodeid][EDGES]
        for e, nd in edge_list:
            if nd in node_set:
                continue
            # 计算方向
            n0, n1 = e[0:2]
            p0, p1 = node[n0][0:2], node[n1][0:2]
            # 只有和行驶方向相近的边才能入选
            if e[EDGE_ONEWAY] is False or is_near_segment(last_point, point, p0, p1):
                edge_set.add(e[EDGE_INDEX])
            next_dist = cur_dist + e[EDGE_LENGTH]
            # 判定能否到达新点
            if next_dist < dist_thread:
                candidate.put([nd, next_dist])
                node_set.add(nd)

    # 寻找最近的匹配
    min_score, min_dist, sel_edge = 1e20, None, None
    # if cnt == 10:
    #     draw_edge_set(edge, edge_set, node)
    for i in edge_set:
        e = edge[i]
        x0, y0, x1, y1 = edge2xy(e, node)
        w0, w1 = 1.0, 10.0
        dist = point2segment(point[0], point[1], x0, y0, x1, y1)
        score = w0 * dist + w1 * (1 - calc_included_angle(last_point, point, [x0, y0], [x1, y1]))
        if min_score > score:
            min_score, min_dist, sel_edge = score, dist, e

    if sel_edge is None:
        return 0, 0, sel_edge, edge_set, 0, None
    x0, y0, x1, y1 = edge2xy(sel_edge, node)
    x, y = point[0:2]
    rx, ry, _ = point_project(x, y, x0, y0, x1, y1)
    trace = get_trace_from_project(node, last_point, last_edge, [rx, ry], sel_edge, cnt)
    trace_dist = get_trace_dist(trace)
    # draw_seg(trace, 'b')

    return rx, ry, sel_edge, edge_set, trace_dist, min_dist


def get_mod_points0(kdt, X, traj_order, node, edge):
    """
    White00 algorithm 1, basic algorithm point to point
    """
    traj_mod = []
    # traj_point: [x, y]
    for traj_point in traj_order:
        px, py, last_edge = get_first_point(traj_point, kdt, X, node, edge)
        traj_mod.append([px, py])

    return traj_mod


def get_mod_points1(kdt, X, traj_order, node, edge):
    """
    White00 algorithm 3, point to curve with candidates
    """
    traj_mod = []
    first_point = True
    last_point, last_edge = None, None
    # traj_point: [x, y]
    cnt = 0
    for traj_point in traj_order:
        if first_point:
            first_point = False
            px, py, last_edge = get_first_point(traj_point, kdt, X, node, edge)
            traj_mod.append([px, py])
            last_point = traj_point
        elif calc_dist(last_point, traj_point) > 20:
            # 太近的点不予考虑，列为subpoint，在此处略过，后面再插入
            px, py, last_edge, can_set, trace_dist, min_dist = \
                get_latter_point(traj_point, last_point, node, edge, last_edge, cnt)
            draw_point(traj_point, 'co')
            traj_mod.append([px, py])
            last_point = traj_point
        cnt += 1
    return traj_mod


def draw():
    read_xml('hz.xml')
    draw_map()
    # kdt, X = make_kdtree(node)
    #
    # traj_order = load_trace('traj1.txt')
    # x, y = zip(*traj_order)
    # minx, maxx, miny, maxy = min(x), max(x), min(y), max(y)
    # plt.xlim(minx, maxx)
    # plt.ylim(miny, maxy)
    # plt.plot(x, y, 'k--', marker='+')
    # for i in range(len(x)):
    #     plt.text(x[i], y[i], "{0}".format(i))
    # traj_mod = get_mod_points1(kdt, X, traj_order, node, edge)
    # draw_points(traj_mod)


fig = plt.figure(figsize=(16, 8))
ax = fig.add_subplot(111)
draw()
plt.show()
