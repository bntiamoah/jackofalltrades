from bigxml import Parser, xml_handle_element
from dataclasses import dataclass


@xml_handle_element('gmfa', 'segment')
@dataclass
class Gmfa:
    segment_id: int = -1
    lanes: str = 'N/A'
    type: str = 'N/A'
    start_x_section_id: int = -1
    end_x_section_id: int = -1

    @xml_handle_element('gmfaSection')
    def handle_attr(self, node):
        self.segment_id = node.parents[1].attributes['id']
        self.lanes = node.attributes['lanes']
        self.type = node.attributes['type']
        self.start_x_section_id = node.attributes['startXsectionId']
        self.end_x_section_id = node.attributes['endXsectionId']


def read_gmfa(file_path):
    with open(file_path, 'rb') as f:
        for item in Parser(f).iter_from(Gmfa):
            yield item
