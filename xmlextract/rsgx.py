from bigxml import Parser, xml_handle_element
from dataclasses import dataclass, InitVar


@xml_handle_element('region', 'road')
@dataclass
class Segment:
    segment_id: int = -1
    numLanes: int = -1
    successors: str = 'None'
    predecessors: str = 'None'
    clazz: str = 'None'
    road_id: int = -1
    road_name: str = 'None'

    @xml_handle_element('segment')
    def handle_attr(self, node):
        self.segment_id = int(node.attributes['id'])
        self.numLanes = int(node.attributes['numLanes'])
        self.successors = node.attributes['successors']
        self.predecessors = node.attributes['predecessors']
        self.clazz = node.attributes['class']
        self.road_id = int(node.parents[1].attributes['id'])
        self.road_name = node.parents[1].attributes['id']


@xml_handle_element('region', 'road', 'segment')
@dataclass
class Meta:
    segment_id: int = -1
    author: str = 'None'
    method: str = 'None'
    published: str = 'None'

    @xml_handle_element('provenanceRecord')
    def handle_attr(self, node):
        self.segment_id = int(node.parents[2].attributes['id'])
        self.author = node.attributes['author']
        self.method = node.attributes['method']
        self.published = node.attributes['published']


@xml_handle_element('region', 'road', 'segment', 'laneMap', 'map')
@dataclass
class LaneMap:
    node: InitVar

    segment_id: int = -1
    lane_id: int = -1
    to_segment_id: int = -1
    to_lane_id: int = -1

    def __post_init__(self, node):
        self.segment_id = int(node.parents[2].attributes['id'])
        self.lane_id = int(node.attributes['laneId'])
        self.to_segment_id = node.attributes['toSegmentId']
        self.to_lane_id = node.attributes['toLaneId']


@xml_handle_element('region', 'road', 'segment', 'laneMap', 'transitionMap')
@dataclass
class TransitionMap:
    node: InitVar

    segment_id: int = -1
    lane_id: int = -1
    to_segment_id: int = -1
    to_lane_id: int = -1

    def __post_init__(self, node):
        self.segment_id = int(node.parents[2].attributes['id'])
        self.lane_id = int(node.attributes['laneId'])
        self.to_segment_id = node.attributes['toSegmentId']
        self.to_lane_id = node.attributes['toLaneId']


def read_segments(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Segment):
            yield item


def read_meta(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Meta):
            yield item


def read_lane_map(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(LaneMap):
            yield item


def read_transition_map(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(TransitionMap):
            yield item


def read_all(file_path):
    return {
        'transitionMap': list(map(lambda x: x, read_transition_map(file_path))),
        'laneMap': list(map(lambda x: x, read_lane_map(file_path))),
        'metas': list(map(lambda x: x, read_meta(file_path))),
        'segments': list(map(lambda x: x, read_segments(file_path)))
    }


# for k, v in read_all('data/TX/USA_TEXAS_10569.rsgx').items():
#     print(k, len(v))
