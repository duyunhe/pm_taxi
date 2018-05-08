# coding=utf-8


class DistNode(object):
    def __init__(self, node, dist):
        self.node = node
        self.dist = dist

    def __lt__(self, other):
        return self.dist < other.dist


class MapNode(object):
    """
    点表示
    point([px,py]), nodeid, link_list, rlink_list, dist_dict
    在全局维护dict, key=nodeid, value=MapNode
    """
    def __init__(self, point, nodeid):
        self.point, self.nodeid = point, nodeid
        self.link_list = []         # 连接到其他点的列表, [[edge0, node0], [edge1, node1]....]
        self.rlink_list = []
        self.prev_node = None       # bfs时寻找路径, MapNode

    def add_link(self, edge, node):
        self.link_list.append([edge, node])

    def add_rlink(self, edge, node):
        self.rlink_list.append([edge, node])


class MapEdge(object):
    """
    线段表示
    node0(MapNode), node1,
    oneway(true or false), edge_index, edge_length
    维护list[MapEdge]
    """
    def __init__(self, node0, node1, oneway, edge_index, edge_length, way_id):
        self.node0, self.node1 = node0, node1
        self.oneway = oneway
        self.edge_index = edge_index
        self.edge_length = edge_length
        self.way_id = way_id


class MatchResult(object):
    """
    匹配结果
    match_list: [mod_point, edge, last_index, dist]
    """
    def __init__(self, idx, point, match_list):
        self.idx, self.point, self.match_list = idx, point, match_list

