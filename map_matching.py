# coding=utf-8
from xml.etree import ElementTree as ET
import matplotlib.pyplot as plt
from traj import load_traj, load_trace
from sklearn.neighbors import KDTree
import math
import Queue
from map_struct import DistNode, MapEdge, MapNode
from geo import point2segment, point_project, calc_dist, bl2xy, is_near_segment, calc_included_angle, point_project_edge
from time import clock
import traj
import numpy as np
fp = open('point.txt', 'w')
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


def edge2xy(e):
    x0, y0 = e.node0.point[0:2]
    x1, y1 = e.node1.point[0:2]
    return x0, y0, x1, y1


def draw_map():
    for i in map_way:
        pl = map_way[i]
        node_list = pl['node']
        x, y = [], []
        for node in node_list:
            x.append(node.point[0])
            y.append(node.point[1])

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


def draw_edge(e, c):
    x0, y0, x1, y1 = edge2xy(e)
    x, y = [x0, x1], [y0, y1]
    plt.plot(x, y, c, linewidth=2)
    plt.text((x[0] + x[-1]) / 2, (y[0] + y[-1]) / 2, '{0},{1}'.format(e.edge_index, e.way_id))


def draw_edge_list(edge_list):
    for edge in edge_list:
        if edge.oneway is True:
            draw_edge(edge, 'brown')
        else:
            draw_edge(edge, 'b')


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
    plt.plot([point[0]], [point[1]], c, markersize=6)


def store_link():
    for edge in map_edge_list:
        n0, n1 = edge.node0, edge.node1
        if edge.oneway is True:
            n0.add_link(edge, n1)
            n1.add_rlink(edge, n0)
        else:
            n0.add_link(edge, n1)
            n1.add_link(edge, n0)
            n0.add_rlink(edge, n1)
            n1.add_rlink(edge, n0)


def store_node(tree):
    p = tree.find('meta')
    nds = p.findall('node')
    for x in nds:
        node_dic = x.attrib
        nodeid = node_dic['id']
        dx, dy = bl2xy(float(node_dic['lat']), float(node_dic['lon']))
        node = MapNode([dx, dy], nodeid)
        map_node_dict[nodeid] = node


def store_edge(tree):
    p = tree.find('meta')
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
            node_in_way.append(map_node_dict[node_dic['ref']])
        ref['node'] = node_in_way
        last_node = None
        ref['edge'] = []
        for node in node_in_way:
            if last_node is not None:
                edge_index = len(map_edge_list)
                ref['edge'].append(edge_index)
                p0, p1 = last_node.point, node.point
                edge_length = calc_dist(p0, p1)
                edge = MapEdge(last_node, node, oneway, edge_index, edge_length, wid)
                map_edge_list.append(edge)
            last_node = node


def calc_node_dict(node):
    """
    dijkstra算法计算最短路径
    保存在node中dist字典内
    :param node: MapNode
    :return: null
    """
    T = 80000 / 3600 * 10   # dist_thread
    node_set = set()        # node_set用于判断是否访问过
    edge_set = set()        # edge_set用于记录能够访问到的边
    q = Queue.PriorityQueue(maxsize=-1)     # 优先队列优化
    # initialize
    init_node = DistNode(node.nodeid, 0)
    node_set.add(node.nodeid)
    q.put(init_node)
    # best first search
    while not q.empty():
        cur_node = q.get()
        if cur_node.dist > T:
            break
        for edge, nextid in map_node_dict[cur_node.nodeid].link_list:
            edge_set.add(edge.edge_index)
            if nextid in node_set:
                continue
            node_set.add(nextid)
            new_node = DistNode(nextid, cur_node.dist + edge.edge_length)
            node.dist_dict[nextid] = new_node.dist
            q.put(new_node)

    # store edge indexes which can reach
    node.reach_set = edge_set


def read_xml(filename):
    t = clock()
    tree = ET.parse(filename)
    store_node(tree)
    store_edge(tree)
    store_link()
    print 'load map', clock() - t


def make_kdtree():
    nd_list = []
    for key, item in map_node_dict.items():
        nodeid_list.append(key)
        nd_list.append(item.point)
    X = np.array(nd_list)
    return KDTree(X, leaf_size=2, metric="euclidean"), X


def get_candidate_first(taxi_data, cnt=-1):
    """
    get candidate edges from road network which fit point 
    :param taxi_data: Taxi_Data  .px, .py, .speed, .stime
    :return: edge candidate list  list[edge0, edge1, edge...]
    """
    kdt, X = make_kdtree()
    dist, ind = kdt.query([[taxi_data.px, taxi_data.py]], k=50)

    pts = []
    seg_set = set()
    # fetch nearest map nodes in network around point, then check their linked edges
    for i in ind[0]:
        pts.append([X[i][0], X[i][1]])
        node_id = nodeid_list[i]
        edge_list = map_node_dict[node_id].link_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)
        # here, need reverse link,
        # for its first node can be far far away, then this edge will not be included
        edge_list = map_node_dict[node_id].rlink_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)

    edge_can_list = []
    for i in seg_set:
        edge_can_list.append(map_edge_list[i])

    return edge_can_list


def init_candidate_queue(last_point, last_edge, can_queue, node_set):
    """
    initialize the queue, add one or two points of the last edge
    """
    _, ac, state = point_project_edge(last_point, last_edge)
    project_dist = np.linalg.norm(np.array(ac))
    dist0, dist1 = project_dist, last_edge.edge_length - project_dist
    if dist0 > last_edge.edge_length:
        dist0, dist1 = last_edge.edge_length, 0

    if last_edge.oneway:
        node = last_edge.node1
        dnode = DistNode(node, dist1)
        can_queue.put(dnode)
    else:
        node = last_edge.node0
        dnode = DistNode(node, dist0)
        can_queue.put(dnode)
        node_set.add(node.nodeid)

        node = last_edge.node1
        dnode = DistNode(node, dist1)
        can_queue.put(dnode)

    node_set.add(node.nodeid)


def get_candidate_later(cur_point, last_point, last_edge, last_state, itv_time, cnt):
    """
    :param cur_point: [px, py]
    :param last_point: [px, py]
    :param last_edge: MapEdge
    :param last_state: direction of vehicle in map edge
    :return: edge_can_list [edge0, edge1....]
    """
    edge_can_list = []
    T = 100000.0 / 3600 * itv_time          # dist_thread
    node_set = set()                        # node_set用于判断是否访问过
    edge_set = set()                        # edge_set用于记录能够访问到的边

    if last_edge.oneway is False or is_near_segment(last_point, cur_point,
                                                    last_edge.node0.point, last_edge.node1.point):
        edge_set.add(last_edge.edge_index)

    q = Queue.PriorityQueue(maxsize=-1)     # 优先队列 best first search
    init_candidate_queue(last_point, last_edge, q, node_set)    # 搜索第一步，加入之前线段中的点

    while not q.empty():
        dnode = q.get()
        cur_node, cur_dist = dnode.node, dnode.dist
        if cur_dist >= T:       # 超过阈值后停止
            break
        for edge, node in cur_node.link_list:
            if node.nodeid in node_set:
                continue
            node_set.add(node.nodeid)
            # 单行线需要判断角度
            if edge.oneway is False or is_near_segment(last_point, cur_point, edge.node0.point, edge.node1.point):
                edge_set.add(edge.edge_index)
            next_dnode = DistNode(node, cur_dist + edge.edge_length)
            node.prev_node = cur_node
            q.put(next_dnode)

    for i in edge_set:
        edge_can_list.append(map_edge_list[i])

    return edge_can_list


def _get_mod_point_first(candidate, point):
    """
    :param candidate: 
    :param point: current point
    :return: project_point, sel_edge
    """
    min_dist, sel_edge = 1e20, None

    # first point
    for edge in candidate:
        # n0, n1 = edge.node0, edge.nodeid1
        p0, p1 = edge.node0.point, edge.node1.point
        dist = point2segment(point, p0, p1)
        if min_dist > dist:
            min_dist, sel_edge = dist, edge

    sel_node0, sel_node1 = sel_edge.node0, sel_edge.node1
    project_point, _, state = point_project(point, sel_node0.point, sel_node1.point)
    # print sel_edge.edge_index, min_dist
    return project_point, sel_edge, min_dist


def _get_mod_point_later(candidate, point, last_point, cnt):
    """
    :param candidate: 
    :param point: current position point
    :param last_point: last position point
    :return: project_point, sel_edge, score
    """
    min_score, sel_edge = 1e10, None

    for edge in candidate:
        p0, p1 = edge.node0.point, edge.node1.point
        w0, w1 = 1.0, 10.0
        # 加权计算分数，考虑夹角的影响
        dist = point2segment(point, p0, p1)
        angle = calc_included_angle(last_point, point, p0, p1)
        if not edge.oneway and angle < 0:
            angle = -angle
        score = w0 * dist + w1 * (1 - angle)
        if score < min_score:
            min_score, sel_edge = score, edge
        # if cnt == 147:
        #     print edge.edge_index, dist, score, angle

    if sel_edge is None:
        return None, None, 0
    project_point, _, state = point_project(point, sel_edge.node0.point, sel_edge.node1.point)
    if state == 1:
        # 点落在线段末端外
        project_point = sel_edge.node1.point
    elif state == -1:
        project_point = sel_edge.node0.point
    return project_point, sel_edge, min_score


def get_mod_point(taxi_data, candidate, last_point, cnt=-1):
    """
    get best fit point matched with candidate edges
    :param taxi_data: Taxi_Data
    :param candidate: list[edge0, edge1, edge...]
    :param last_point: last matched point 
    :return: matched point, matched edge, minimum distance from point to matched edge
    """
    point = [taxi_data.px, taxi_data.py]
    if last_point is None:
        # 第一个点
        return _get_mod_point_first(candidate, point)
    else:
        return _get_mod_point_later(candidate, point, last_point, cnt)


def get_first_point(point, kdt, X):
    """
    match point to nearest segment
    :param point: point to be matched
    :param kdt: kdtree
    :param X: 
    :return: 
    """
    dist, ind = kdt.query([point], k=30)

    pts = []
    seg_set = set()
    for i in ind[0]:
        pts.append([X[i][0], X[i][1]])
        node_id = nodeid_list[i]
        edge_list = map_node_dict[node_id].link_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)

    min_dist, sel = 1e20, -1
    for idx in seg_set:
        n0, n1 = map_edge_list[idx].nodeid0, map_edge_list[idx].nodeid1
        p0, p1 = map_node_dict[n0].point, map_node_dict[n1].point
        dist = point2segment(point, p0, p1)
        if min_dist > dist:
            min_dist, sel = dist, idx

    sel_edge = map_edge_list[sel]
    sel_node0, sel_node1 = sel_edge.nodeid0, sel_edge.nodeid1
    x0, y0 = map_node_dict[sel_node0].point[0:2]
    x1, y1 = map_node_dict[sel_node1].point[0:2]
    x, y = point[0:2]
    rx, ry, _ = point_project(x, y, x0, y0, x1, y1)
    return rx, ry, sel_edge


def get_mod_points0(traj_order):
    """
    White00 algorithm 1, basic algorithm point to point
    """
    kdt, X = make_kdtree()
    traj_mod = []
    # traj_point: [x, y]
    for taxi_data in traj_order:
        px, py, last_edge = get_first_point([taxi_data.px, taxi_data.py], kdt=kdt, X=X)
        traj_mod.append([px, py])

    return traj_mod


def get_trace_dist(trace):
    last_point = None
    trace_dist = 0
    for point in trace:
        if last_point is not None:
            dist = calc_dist(point, last_point)
            trace_dist += dist
        last_point = point
    return trace_dist


def get_trace(last_edge, edge, last_point, point):
    """
    use prev_node to generate the path reversely
    :param last_edge:  last matched edge
    :param edge:  current matched edge
    :param last_point:  last position point
    :param point:  current matched(projected) point
    :return:
    """
    spoint, _, _ = point_project_edge(last_point, last_edge)
    if last_edge == edge:
        return [spoint, point]

    trace = [point]
    n0, n1 = edge.node0, edge.node1
    if n0.prev_node == n1:      # n1 is nearer from last point
        cur_node = n1
    else:
        cur_node = n0
    while cur_node != last_edge.node0 and cur_node != last_edge.node1:
        trace.append(cur_node.point)
        cur_node = cur_node.prev_node

    trace.append(cur_node.point)
    trace.append(spoint)
    return trace


def POINT_MATCH(traj_order):
    """
    using point match with topology, 
    :param traj_order: list of Taxi_Data 
    :return: 
    """
    first_point = True
    last_point, last_edge = None, None
    last_state = 0      # 判断双向道路当前是正向或者反向
    total_dist = 0.0    # 计算路程
    last_time = None
    cnt = 0
    traj_mod = []
    for data in traj_order:
        if first_point:
            # 第一个点
            candidate_edges = get_candidate_first(data, cnt)
            # Taxi_Data .px .py .stime .speed
            first_point = False
            mod_point, last_edge, _ = get_mod_point(data, candidate_edges, last_point, cnt)
            state = 'c'
            traj_mod.append(mod_point)
            last_point = mod_point
            last_time = data.stime
        else:
            # 随后的点
            # 首先判断两个点是否离得足够远
            T = 15
            cur_point = [data.px, data.py]
            interval = calc_dist(cur_point, last_point)
            interval_time = (data.stime - last_time).total_seconds()
            # print cnt, interval
            if interval < T:
                last_time = data.stime
                continue
            candidate_edges = get_candidate_later(cur_point, last_point, last_edge, last_state, interval_time, cnt)
            # if cnt == 60:
            #     draw_edge_list(candidate_edges)

            if len(candidate_edges) == 0:
                # no match, restart
                candidate_edges = get_candidate_first(data, cnt)
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, None, cnt)
                state = 'c'
            else:
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, last_point, cnt)
                state = 'r'

            offset_dist = calc_dist(mod_point, cur_point)
            if offset_dist > 60:
                # 判断是否出现太远的情况
                candidate_edges = get_candidate_first(data, cnt)
                # draw_edge_list(candidate_edges)
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, None, cnt)
                state = 'm'

            if state == 'r':
                trace = get_trace(last_edge, cur_edge, last_point, mod_point)
                # draw_seg(trace, 'b')
                dist = get_trace_dist(trace)
            else:
                dist = calc_dist(cur_point, last_point)
            total_dist += dist

            traj_mod.append(mod_point)
            last_point, last_edge = cur_point, cur_edge

        plt.text(data.px, data.py, '{0}'.format(cnt))
        plt.text(mod_point[0], mod_point[1], '{0}'.format(cnt), color=state)

        cnt += 1
        last_time = data.stime
        # print cnt, data.px, data.py, mod_point[0], mod_point[1]
    fp.close()
    return traj_mod, total_dist


def DYN_MATCH(traj_order):
    """
    T.B.M.
    using point match with dynamic programming, 
    :param traj_order: list of Taxi_Data 
    :return: 
    """
    first_point = True
    last_point, last_edge = None, None
    last_state = 0      # 判断双向道路当前是正向或者反向
    cnt = 0

    traj_mod = []       # 存放修正偏移后的data

    for data in traj_order:
        if first_point:
            candidate_edges = get_candidate_first(data, cnt)
            # Taxi_Data .px .py .stime .speed
            first_point = False
            mod_point, last_edge, score = get_mod_point(data, candidate_edges, last_point, cnt)
            state = 'c'
            data.set_edge([last_edge, score])
            traj_mod.append(data)
            last_point = mod_point
        else:
            # 首先判断两个点是否离得足够远
            T = 10000 / 3600 * 10
            cur_point = [data.px, data.py]
            interval = calc_dist(cur_point, last_point)
            # print cnt, interval
            if interval < T:
                continue
            # 读取上一个匹配点的信息
            last_data = traj_mod[cnt - 1]
            last_point = [last_data.px, last_point.py]

            min_score, sel_edge, sel_score = 1e10, None, 0
            for last_edge, last_score in last_data.edge_info:
                candidate_edges = get_candidate_later(cur_point, last_point, last_edge, last_state, cnt)

                if len(candidate_edges) == 0:
                    # no match, restart
                    candidate_edges = get_candidate_first(data, cnt)
                    mod_point, cur_edge, score = get_mod_point(data, candidate_edges, None, cnt)
                    state = 'c'
                    cur_score = score + 1e5
                else:
                    # if cnt == 147:
                    #     draw_edge_list(candidate_edges)
                    mod_point, cur_edge, score = get_mod_point(data, candidate_edges, last_point, cnt)
                    cur_score = score + last_score
                    state = 'r'
                if cur_score < min_score:
                    min_score, sel_edge, sel_score = cur_score, cur_edge, score

            # if state == 'r':
            #     trace = get_trace(last_edge, cur_edge, last_point, mod_point)
            #     draw_seg(trace, 'b')

            offset_dist = calc_dist(mod_point, cur_point)
            if offset_dist > 50:
                # 判断是否出现太远的情况
                candidate_edges = get_candidate_first(data, cnt)
                # draw_edge_list(candidate_edges)
                mod_point, cur_edge = get_mod_point(data, candidate_edges, None, cnt)
                state = 'm'

            traj_mod.append(data)
            last_point, last_edge = cur_point, cur_edge

        plt.text(data.px, data.py, '{0}'.format(cnt))
        plt.text(mod_point[0], mod_point[1], '{0}'.format(cnt), color=state)

        cnt += 1
        print cnt, data.px, data.py

    return traj_mod


def draw_trace(traj):
    x, y = [], []
    for data in traj:
        x.append(data.px)
        y.append(data.py)
    minx, maxx, miny, maxy = min(x), max(x), min(y), max(y)
    plt.xlim(minx, maxx)
    plt.ylim(miny, maxy)
    plt.plot(x, y, 'k--', marker='+')


def matching_draw(trace):
    # read_xml('hz.xml')
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot(111)
    draw_map()
    draw_trace(trace)
    #
    traj_mod, dist = POINT_MATCH(trace)
    draw_points(traj_mod)
    plt.show()
    return dist


def matching(trace):
    # read_xml('hz.xml')
    #
    traj_mod, dist = POINT_MATCH(trace)
    return dist
