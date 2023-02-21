from bigxml import Parser, xml_handle_element
from dataclasses import dataclass, InitVar


@xml_handle_element('segments', 'segment', 'xSection', 'lane')
@dataclass
class Lane:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    lane_id: int = -1
    point_id = int = -1
    clazz: str = 'None'
    width: float = -1
    along_slope: float = -1
    cross_slope: float = -1
    speed_limit: int = -1
    surface_type: str = 'None'
    stacking: str = 'None'

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])

        self.lane_id = int(node.attributes['id'])
        self.point_id = int(node.attributes['pointId'])
        self.clazz = node.attributes['class']
        self.width = float(node.attributes['width'])
        self.along_slope = float(node.attributes['alongSlope'])
        self.cross_slope = float(node.attributes['crossSlope'])
        self.speed_limit = int(node.attributes['speedLimit'])
        self.surface_type = node.attributes['surfaceType']
        self.stacking = node.attributes['stacking']


@xml_handle_element('segments', 'segment', 'xSection', 'leftEdge')
@dataclass
class LeftEdge:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    type: str = 'None'
    width: float = -1
    latitude: float = -1
    longitude: float = -1
    elevation: float = -1

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.type = node.attributes['type']
        self.width = float(node.attributes['width'])
        self.latitude = float(node.attributes['lat'])
        self.longitude = float(node.attributes['long'])
        self.elevation = float(node.attributes['elevation'])


@xml_handle_element('segments', 'segment', 'xSection', 'rightEdge')
@dataclass
class RightEdge:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    type: str = 'None'
    width: float = -1
    latitude: float = -1
    longitude: float = -1
    elevation: float = -1

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.type = node.attributes['type']
        self.width = float(node.attributes['width'])
        self.latitude = float(node.attributes['lat'])
        self.longitude = float(node.attributes['long'])
        self.elevation = float(node.attributes['elevation'])


@xml_handle_element('segments', 'segment', 'xSection', 'point')
@dataclass
class Point:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    point_id: int = -1
    curvature: float = -1
    heading: float = -1
    latitude: float = -1
    longitude: float = -1
    elevation: float = -1

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.point_id = int(node.attributes['id'])
        self.curvature = float(node.attributes['curvature'])
        self.heading = float(node.attributes['curvature'])
        self.latitude = float(node.attributes['lat'])
        self.longitude = float(node.attributes['long'])
        self.elevation = float(node.attributes['elevation'])


@xml_handle_element('segments', 'segment', 'xSection', 'lane')
@dataclass
class Marker:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    marker_id: int = -1
    point_id = int = -1
    width: float = -1
    type: str = 'None'
    color: str = 'None'

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.marker_id = int(node.attributes['id'])
        self.point_id = int(node.attributes['pointId'])
        self.type = node.attributes['type']
        self.width = float(node.attributes['width'])
        self.color = node.attributes['color']


@xml_handle_element('segments', 'segment', 'xSection', 'rightBarrier')
@dataclass
class RightBarrier:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    type: str = 'None'
    width: float = -1
    height: float = 1
    latitude: float = -1
    longitude: float = -1
    elevation: float = -1

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.type = node.attributes['type']
        self.height = float(node.attributes['height'])
        self.latitude = float(node.attributes['lat'])
        self.longitude = float(node.attributes['long'])
        self.elevation = float(node.attributes['elevation'])


@xml_handle_element('segments', 'segment', 'xSection', 'leftBarrier')
@dataclass
class LeftBarrier:
    node: InitVar

    segment_id: int = -1
    x_section_id: int = -1
    type: str = 'None'
    width: float = -1
    height: float = 1
    latitude: float = -1
    longitude: float = -1
    elevation: float = -1

    def __post_init__(self, node):
        parent = node.parents[2]
        grand_parent = node.parents[1]

        self.segment_id = int(grand_parent.attributes['id'])
        self.x_section_id = int(parent.attributes['id'])
        self.type = node.attributes['type']
        self.height = float(node.attributes['height'])
        self.latitude = float(node.attributes['lat'])
        self.longitude = float(node.attributes['long'])
        self.elevation = float(node.attributes['elevation'])


def read_lanes(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Lane):
            yield item


def read_edges(file_path, side):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(LeftEdge if side == 'left' else RightEdge):
            yield item


def read_points(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Point):
            yield item


def read_markers(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Point):
            yield item


def read_barriers(file_path, side):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(LeftBarrier if side == 'left' else RightBarrier):
            yield item


def read_all(file_path):
    data = {
        'lanes': [],
        'markers': [],
        'points': [],
        'barriers': [],
        'edges': []
    }
    for item in read_lanes('data/TX/USA_TEXAS_10569.lxsx'):
        data['lanes'].append(item)

    for item in read_markers('data/TX/USA_TEXAS_10569.lxsx'):
        data['markers'].append(item)

    for item in read_points('data/TX/USA_TEXAS_10569.lxsx'):
        data['points'].append(item)

    for item in read_barriers('data/TX/USA_TEXAS_10569.lxsx', 'left'):
        data['barriers'].append(item)

    for item in read_barriers('data/TX/USA_TEXAS_10569.lxsx', 'right'):
        data['barriers'].append(item)

    for item in read_edges('data/TX/USA_TEXAS_10569.lxsx', 'left'):
        data['edges'].append(item)

    for item in read_edges('data/TX/USA_TEXAS_10569.lxsx', 'right'):
        data['edges'].append(item)

    return data

# count = 0
# for item in read_lanes('data/DC/USA_DC_41.lxsx'):
#     print(item)
#     count += 1
#     print(count)
