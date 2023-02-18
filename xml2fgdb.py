import datetime
import os
import sys
import threading
import time
from datetime import datetime as dt
from glob import glob
from zipfile import ZipFile

import arcpy
from lxml import objectify

from multiproc import ProdConsumer

lck = threading.Lock()
arcpy.env.overwriteOutput = True


def log(message, _output_location=None):
    global lck
    lck.acquire()
    message = '[{}]: {}\n'.format(dt.now(), message)
    arcpy.AddMessage(message)
    if _output_location:
        with open(os.path.join(_output_location, 'logs.txt'), 'a') as file_obj:
            file_obj.write(message)
    lck.release()


def get_lane_side(lane_id, num_lanes):
    if num_lanes == 1:
        return 'Both'

    if lane_id == 1:
        return 'Left'
    if 1 < lane_id < num_lanes:
        return 'Both'
    return 'Right'


def get_files(folder):
    files = glob('{}/**/*.*'.format(folder), recursive=True)
    folder_files = []
    for file in files:
        if file.endswith('fasx') or file.endswith('lxsx') or file.endswith('rsgx'):
            folder_files.append(file)
    return folder_files


def unpack_zip(zipfile, out_loc):
    extract_path = out_loc + '/'
    if not os.path.exists(extract_path):
        parent_archive = ZipFile(zipfile)
        parent_archive.extractall(extract_path)
        parent_archive.close()
    namelist = get_files(extract_path)
    return os.path.basename(zipfile), namelist


def extract_sign(segment_id, sign, lanes):
    sign_id = int(sign['@id'])
    sign_type = sign['@type']
    sign_lat = float(sign['@lat'])
    sign_lon = float(sign['@long'])
    sign_elev = float(sign['@elevation'])

    lane_id = int(sign['assignment']['@lanes'])
    start_xsection_id = int(sign['assignment']['@startXsectionId'])
    end_xsection_id = int(sign['assignment']['@endXsectionId'])

    if not isinstance(lanes, list):
        lanes = [lanes]
    lane = list(filter(lambda _m: int(_m['@laneId']) == lane_id, lanes))[0]
    start_lane_id = int(lane['@laneId'])
    end_lane_id = int(lane['@toLaneId'])
    return (segment_id, start_lane_id, end_lane_id, start_xsection_id, end_xsection_id, 'Sign',
            sign_id, sign_lat, sign_lon, sign_elev, sign_type, (sign_lon, sign_lat))


def get_segment_connectivities(lanes, segment_id, successor_ids, predecessors_ids):
    conns = []
    for lane in lanes:
        conns.append(
            (predecessors_ids, -1, int(segment_id), int(lane.attrib['laneId']), successor_ids,
             int(lane.attrib['toLaneId'])))
    return conns


def extract_signs(rsgx_content):
    result = []
    for road in rsgx_content['region']['road']:
        segment_id = int(road['segment']['@id'])

        if 'laneMap' in road['segment']:
            lanes = road['segment']['laneMap']['map']
            if 'signs' in road['segment']:
                signs = road['segment']['signs']['sign']
                if not isinstance(signs, list):
                    signs = [signs]
                result += list(map(lambda s: extract_sign(segment_id, s, lanes), signs))
    return result


def extract_connectivities(rsgx_content):
    result = []
    roads = rsgx_content['region']['road']
    for road in roads:
        segment_id = int(road['segment']['@id'])
        num_lanes = int(road['segment']['@numLanes'])
        successor_ids = road['segment']['@successors']
        predecessors_ids = road['segment']['@predecessors']
        if 'laneMap' in road['segment']:
            lanes = road['segment']['laneMap']['map']
            result += get_segment_connectivities(lanes, segment_id,
                                                 successor_ids, predecessors_ids)
    return result


def extract_segments(rsgx_content):
    result = []
    region = rsgx_content['region']['@name']
    for road in rsgx_content['region']['road']:
        road_id = int(road['@id'])
        road_name = road['@name']
        segment_id = int(road['segment']['@id'])
        num_lanes = int(road['segment']['@numLanes'])
        successor_ids = road['segment']['@successors']
        predecessors_ids = road['segment']['@predecessors']

        road_class = road['segment']['@class']
        provenance = road['segment']['provenanceRecord']
        result.append(
            (region, road_id, road_class, road_name, segment_id, num_lanes, successor_ids, predecessors_ids, None,
             None,
             None,
             provenance['@author'], provenance['@published'], provenance['@method'])
        )
    return result


def _extract_lanes_and_crossings(segment_id, xsection_id, lane, lanes_count, points, markers, poly, data):
    lane_id = int(lane.attrib['id'])
    point_id = int(lane.attrib['pointId'])
    clasz = lane.attrib['class']
    width = float(lane.attrib['width'])
    along_slope = float(lane.attrib['alongSlope'])
    cross_slope = float(lane.attrib['crossSlope'])
    speed_limit = int(lane.attrib['speedLimit'])
    surface_type = lane.attrib['surfaceType']
    stacking = lane.attrib['stacking']

    point_heading = -1
    point_lat = -1
    point_lng = -1
    point_elevation = -1
    marker_id = -1
    marker_point_id = -1
    marker_type = ''
    marker_color = ''
    point_curvature = -1

    crossings_records = []

    lst = list(filter(lambda p: int(p.attrib['id']) == point_id, points))
    if len(lst) > 0:
        point = lst[0]
        point_heading = float(point.attrib['heading'])
        point_lat = float(point.attrib['lat'])
        point_lng = float(point.attrib['long'])
        point_elevation = float(point.attrib['elevation'])
        point_curvature = float(point.attrib['curvature'])

        if poly and not is_point_in_polygon(point_lat, point_lng, poly):
            return -1, -1
    else:
        return -1, -1

    lst = list(filter(lambda _m: int(_m.attrib['pointId']) == point_id, markers))
    if len(lst) > 0:
        marker = lst[0]
        marker_id = int(marker.attrib['id'])
        marker_point_id = int(marker.attrib['pointId'])
        marker_type = marker.attrib['type']
        marker_color = marker.attrib['color']

    if hasattr(lane, 'crossings') and 'CROSSINGS' in data:
        for crossing in lane.crossings:
            for xing in crossing.crossing:
                crossings_records.append((segment_id, xsection_id, lane_id, point_id,
                                          xing.attrib['type'], float(xing.attrib['elevation'])))

    num_crossings = 0
    if hasattr(lane, 'crossings'):
        num_crossings = len(lane.crossings)

    left_right = get_lane_side(lane_id, lanes_count)

    if point_lat == -1 or point_lng == -1:
        raise Exception('lat/lngs required')

    return crossings_records, (segment_id, xsection_id, lane_id, point_id, clasz, width, along_slope, cross_slope,
                               speed_limit, surface_type, stacking, point_heading, point_lat, point_lng,
                               point_elevation, marker_id, marker_point_id, marker_type, marker_color,
                               num_crossings, lanes_count, left_right, point_curvature, (point_lng, point_lat))


def _extract_lanes(segment_id, xsection_id, lane, lanes_count, points, markers, _data):
    crossings_records = []

    lane_id = int(lane.attrib['id'])
    point_id = int(lane.attrib['pointId'])
    clasz = lane.attrib['class']
    width = float(lane.attrib['width'])
    along_slope = float(lane.attrib['alongSlope'])
    cross_slope = float(lane.attrib['crossSlope'])
    speed_limit = int(lane.attrib['speedLimit'])
    surface_type = lane.attrib['surfaceType']
    stacking = lane.attrib['stacking']

    point_heading = -1
    point_lat = -1
    point_lng = -1
    point_elevation = -1
    marker_id = -1
    marker_point_id = -1
    marker_type = ''
    marker_color = ''
    point_curvature = -1

    lst = list(filter(lambda p: int(p.attrib['id']) == point_id, points))
    if len(lst) > 0:
        point = lst[0]
        point_heading = float(point.attrib['heading'])
        point_lat = float(point.attrib['lat'])
        point_lng = float(point.attrib['long'])
        point_elevation = float(point.attrib['elevation'])
        point_curvature = float(point.attrib['curvature'])

    lst = list(filter(lambda _m: int(_m.attrib['pointId']) == point_id, markers))
    if len(lst) > 0:
        marker = lst[0]
        marker_id = int(marker.attrib['id'])
        marker_point_id = int(marker.attrib['pointId'])
        marker_type = marker.attrib['type']
        marker_color = marker.attrib['color']

    if hasattr(lane, 'crossings') and 'CROSSINGS' in _data:
        for crossing in lane.crossings:
            for xing in crossing.crossing:
                crossings_records.append((segment_id, xsection_id, lane_id, point_id,
                                          xing.attrib['type'], float(xing.attrib['elevation'])))

    num_crossings = len(crossings_records)
    left_right = get_lane_side(lane_id, lanes_count)

    if point_lat == -1 or point_lng == -1:
        raise Exception('lat/lngs are required')
    return crossings_records, (segment_id, xsection_id, lane_id, point_id, clasz, width, along_slope, cross_slope,
                               speed_limit, surface_type, stacking, point_heading, point_lat, point_lng,
                               point_elevation, marker_id, marker_point_id, marker_type, marker_color,
                               num_crossings, lanes_count, left_right, point_curvature, (point_lng, point_lat))


def create_edge_record(edge, segment_id, xsection_id):
    left_edge_type = edge['@type']
    left_edge_width = float(edge['@width'])
    left_edge_lat = float(edge['@lat'])
    left_edge_lng = float(edge['@long'])
    left_edge_elevation = float(edge['@elevation'])

    return (segment_id, xsection_id, 'Left', left_edge_type, left_edge_width, left_edge_lat,
            left_edge_lng, left_edge_elevation, (left_edge_lng, left_edge_lat))


def create_barrier_record(barrier, segment_id, xsection_id, side):
    _type = barrier.attrib['type']
    height = barrier.attrib['height']
    lat = float(barrier.attrib['lat'])
    lon = float(barrier.attrib['long'])
    elevation = float(barrier.attrib['elevation'])
    return segment_id, xsection_id, height, _type, elevation, side, (lon, lat)


def _create_gmfa_record(_id, gmfa):
    lane_id = gmfa.attrib['lanes']
    __type = gmfa.attrib['type']
    start_xsection_id = int(gmfa.attrib['startXsectionId'])
    end_xsection_id = int(gmfa.attrib['endXsectionId'])
    return int(_id), lane_id, __type, start_xsection_id, end_xsection_id


def read_gmfa_content(segment):
    __id = segment.attrib['id']
    if hasattr(segment, 'gmfaSection'):
        gmfas = segment.gmfaSection
        return list(map(lambda x: _create_gmfa_record(__id, x), gmfas))
    return []


def extract_gmfa(xml_file, segment_ids):
    gmfas = []
    xml_data = objectify.parse(xml_file)
    root = xml_data.getroot()
    segments = list(filter(lambda x: int(x.attrib['id']) in segment_ids, root.segment))
    if len(segments) > 0:
        gmfas += list(map(lambda x: read_gmfa_content(x), segments))
    return gmfas


def read_signs_data(sign, lanes, segment_id):
    sign_id = int(sign.attrib['id'])
    sign_type = sign.attrib['type']
    sign_lat = float(sign.attrib['lat'])
    sign_lon = float(sign.attrib['long'])
    sign_elev = float(sign.attrib['elevation'])

    lane_id = int(sign.assignment.attrib['lanes'])
    start_xsection_id = int(sign.assignment.attrib['startXsectionId'])
    end_xsection_id = int(sign.assignment.attrib['endXsectionId'])

    start_lane_id = None
    end_lane_id = None
    if len(lanes) > 0:
        lane = list(filter(lambda _m: int(_m.attrib['laneId']) == lane_id, lanes))[0]
        start_lane_id = int(lane.attrib['laneId'])
        end_lane_id = int(lane.attrib['toLaneId'])
    return (segment_id, start_lane_id, end_lane_id, start_xsection_id,
            end_xsection_id, 'Sign', sign_id, sign_lat, sign_lon, sign_elev,
            sign_type, (sign_lon, sign_lat))


def read_rsgx_segments(segment, region_name, road_id, road_name, lanes, _data):
    _segments = []
    _conn = []
    _signs = []

    segment_id = segment.attrib['id']
    numb_lanes = int(segment.attrib['numLanes'])
    cls = segment.attrib['class']
    successors = segment.attrib['successors']
    predecessors = segment.attrib['predecessors']

    if 'SEGMENTS' in _data:
        author = None
        timestamp = None
        method = None
        if hasattr(segment, 'provenanceRecord'):
            author = segment.provenanceRecord.attrib['author']
            timestamp = segment.provenanceRecord.attrib['published']
            method = segment.provenanceRecord.attrib['method']
        _segments.append((region_name, road_id, cls, road_name, segment_id,
                          numb_lanes, successors, predecessors, None, None, None,
                          author, timestamp, method))

    if hasattr(segment, 'laneMap') and 'CONNECTIVITIES' in _data:
        lanes = segment.laneMap.map

        if hasattr(segment.laneMap, 'transitionMap'):
            transition_map = segment.laneMap.transitionMap
            _conn.append((predecessors, -1, int(segment_id), int(transition_map.attrib['laneId']),
                          successors, int(transition_map.attrib['toLaneId'])))
        _conn += get_segment_connectivities(lanes, segment_id,
                                            successors, predecessors)
    if hasattr(segment, 'signs') and 'SIGNS' in _data:
        _signs += list(map(lambda s: read_signs_data(s, lanes, segment_id), segment.signs.sign))
    return _conn, _segments, _signs


def extract_signs_conn_segments(xml_file, segment_ids, _data):
    _SIGNS = []
    _CONN = []
    _SEGMENTS = []

    xml_data = objectify.parse(xml_file)  # Parse XML data
    root = xml_data.getroot()  # Root element
    # region_id = root.attrib['id']
    region_name = root.attrib['name']

    def proc(road, rn, _data, segments_ids):
        c = []
        se = []
        si = []
        road_id = road.attrib['id']
        road_name = road.attrib['name']
        lanes = []

        road_segments = list(filter(lambda segment: int(segment.attrib['id']) in segments_ids, road.segment))
        list_of_tuples = list(map(lambda rs:
                                  read_rsgx_segments(rs, rn, road_id, road_name, lanes, _data),
                                  road_segments))
        for l in list_of_tuples:
            c += l[0]
            se += l[1]
            si += l[2]
        return c, se, si

    lot = list(map(lambda r: proc(r, region_name, _data, segment_ids), root.road))
    for l in lot:
        _CONN += l[0]
        _SEGMENTS += l[1]
        _SIGNS += l[2]
    return _SIGNS, _CONN, _SEGMENTS


def extract_markers(marker, points, segment_id, xsection_id, side):
    marker_points = list(
        filter(lambda p: p.attrib['id'] == marker.attrib['pointId'], points))

    pt_lat = -1
    pt_lng = -1
    if len(marker_points) > 0:
        pt_lat = float(marker_points[0].attrib['lat'])
        pt_lng = float(marker_points[0].attrib['long'])
    return (
        segment_id, xsection_id, int(marker.attrib['pointId']),
        int(marker.attrib['id']),
        float(marker.attrib['width']), marker.attrib['type'],
        marker.attrib['color'], pt_lat, pt_lng, side, (pt_lng, pt_lat)
    )


def extract_markers_barriers_edges(segment_id, xsection, _data, lane, lanes_count):
    edges_records = []
    marker_records = []
    barrier_records = []

    xsection_id = int(xsection.attrib['id'])

    if 'MARKERS' in _data:
        if hasattr(xsection, 'marker'):
            side = get_lane_side(int(lane.attrib['id']), lanes_count)
            marker_records += list(map(lambda m:
                                       extract_markers(m, xsection.point, segment_id, xsection_id, side),
                                       xsection.marker))

    if 'BARRIERS' in _data:
        if hasattr(xsection, 'leftBarrier'):
            barrier_records.append(create_barrier_record(xsection.leftBarrier, segment_id, xsection_id, 'left'))

        if hasattr(xsection, 'rightBarrier'):
            barrier_records.append(
                create_barrier_record(xsection.rightBarrier, segment_id, xsection_id, 'right'))

    if 'EDGES' in _data:
        if hasattr(xsection, 'leftEdge'):
            left_edge = xsection.leftEdge
            left_edge_type = left_edge.attrib['type']
            left_edge_width = float(left_edge.attrib['width'])
            left_edge_lat = float(left_edge.attrib['lat'])
            left_edge_lng = float(left_edge.attrib['long'])
            left_edge_elevation = float(left_edge.attrib['elevation'])
            edges_records.append(
                (segment_id, xsection_id, 'Left', left_edge_type, left_edge_width, left_edge_lat,
                 left_edge_lng, left_edge_elevation, (left_edge_lng, left_edge_lat)))
        if hasattr(xsection, 'rightEdge'):
            right_edge_type = xsection.rightEdge.attrib['type']
            right_edge_width = float(xsection.rightEdge.attrib['width'])
            right_edge_lat = float(xsection.rightEdge.attrib['lat'])
            right_edge_lng = float(xsection.rightEdge.attrib['long'])
            right_edge_elevation = float(xsection.rightEdge.attrib['elevation'])
            edges_records.append((segment_id, xsection_id, 'Right', right_edge_type, right_edge_width,
                                  right_edge_lat, right_edge_lng, right_edge_elevation,
                                  (right_edge_lng, right_edge_lat)))
    return edges_records, marker_records, barrier_records


def insert_records_into_featureclass_or_table(workspace, table_name, fields, records, is_table=False):
    if not records or len(records) == 0:
        return
    log('Add features to {}...'.format(table_name), os.path.dirname(workspace))

    ds = os.path.join(workspace, table_name)

    lck2 = threading.Lock()
    lck2.acquire()
    if not arcpy.Exists(ds):
        log('{} does not existing. Creating it...'.format(table_name), os.path.dirname(workspace))
        if is_table:
            ds = arcpy.CreateTable_management(out_path=workspace, out_name=table_name).getOutput(0)
        else:
            ds = arcpy.CreateFeatureclass_management(out_path=workspace, out_name=table_name, geometry_type='POINT',
                                                     spatial_reference=arcpy.SpatialReference(4326)).getOutput(0)
        for field in fields:
            if field[0] in ['lat', 'lng', 'point_lat', 'point_long']:
                arcpy.AddField_management(in_table=ds, field_name=field[0], field_type=field[1],
                                          field_scale=15)
            else:
                arcpy.AddField_management(in_table=ds, field_name=field[0], field_type=field[1])

    field_names = list(map(lambda f: f[0], fields))
    if not is_table:
        field_names.append('SHAPE@XY')

    cursor_ = arcpy.da.InsertCursor(ds, field_names)

    for row in records:
        try:
            cursor_.insertRow(row)
        except Exception as ex:
            log(ex, os.path.dirname(workspace))
    del cursor_
    log('Features/Rows added to {} successfully'.format(ds), os.path.dirname(workspace))
    lck2.release()


def add_lanes(workspace, _part_number, lane_records, region):
    name = region + "_LANES_" + str(_part_number)

    log('Processing {}...'.format(name), os.path.dirname(workspace))

    fields = [('segment_id', 'LONG'), ('xsection_id', 'LONG'), ('lane_id', 'LONG'), ('point_id', 'LONG'),
              ('class', 'TEXT'), ('width', 'DOUBLE'), ('along_slope', 'DOUBLE'), ('cross_slope', 'DOUBLE'),
              ('speed_limit', 'LONG'), ('surface_type', 'TEXT'), ('stacking', 'TEXT'), ('point_heading', 'DOUBLE'),
              ('point_lat', 'DOUBLE'), ('point_long', 'DOUBLE'), ('point_elevation', 'DOUBLE'), ('marker_id', 'LONG'),
              ('marker_point_id', 'LONG'), ('marker_type', 'TEXT'), ('marker_color', 'TEXT'), ('num_crossings', 'LONG'),
              ('num_lanes', 'LONG'), ('left_right', 'TEXT'), ('point_curvature', 'DOUBLE')]
    insert_records_into_featureclass_or_table(workspace, name, fields, lane_records)


def add_signs(workspace, part_number_, sign_records, region):
    name = region + "_SIGN_OBJECT_" + str(part_number_)

    fields = [('segment_id', 'LONG'), ('start_lane_id', 'LONG'), ('end_lane_id', 'LONG'), ('start_xsection_id', 'LONG'),
              ('end_xsection_id', 'LONG'), ('object_sign', 'TEXT'), ('object_sign_id', 'TEXT'),
              ('lat', 'DOUBLE'), ('long', 'DOUBLE'), ('elevation', 'DOUBLE'), ('sign_type', 'TEXT')]

    insert_records_into_featureclass_or_table(workspace, name, fields, sign_records)


def add_connectivity(workspace, part_number, records, region):
    if len(records) == 0:
        return

    name = region + '_CONNECTIVITY_{}'.format(part_number)
    fields = [('predecessor_segment_id', 'TEXT'), ('predecessor_lane_id', 'LONG'), ('segment_id', 'LONG'),
              ('lane_id', 'LONG'), ('successor_segment_id', 'TEXT'),
              ('successor_lane_id', 'LONG')]
    insert_records_into_featureclass_or_table(workspace, name, fields, records, True)


def add_road_segment(workspace, part_number, roadsegments_records, region):
    name = region + "_ROAD_SEGMENTS_" + str(part_number)
    fields = [('region_name', 'TEXT'),
              ('road_id', 'LONG'),
              ('road_type', 'TEXT'),
              ('road_name', 'TEXT'),
              ('segment_id', 'LONG'),
              ('num_lanes', 'LONG'),
              ('successors', 'TEXT'),
              ('predecessors', 'TEXT'),
              ('intersection_id', 'LONG'),
              ('intersection_relation', 'TEXT'),
              ('intersection_maneuver', 'TEXT'),
              ('provenance_author', 'TEXT'),
              ('provenance_timestamp', 'TEXT'),
              ('provenance_method', 'TEXT')]
    insert_records_into_featureclass_or_table(workspace, name, fields, roadsegments_records, True)


def add_edges(workspace, part_number, edges_records, region):
    name = region + "_EDGES_" + str(part_number)

    log('Processing {}...'.format(name), os.path.dirname(workspace))

    fields = [('segment_id', 'LONG'), ('xsection_id', 'LONG'), ('side', 'TEXT'), ('type', 'TEXT'),
              ('width', 'DOUBLE'), ('lat', 'DOUBLE'), ('long', 'DOUBLE'),
              ('elevation', 'DOUBLE')]
    insert_records_into_featureclass_or_table(workspace, name, fields, edges_records)


def add_crossings(workspace, part_number, crossing_records, region):
    if not crossing_records or len(crossing_records) == 0:
        return
    name = region + '_CROSSING_{}'.format(part_number)
    fields = [('segment_id', 'LONG'), ('xsection_id', 'LONG'), ('lane_id', 'LONG'), ('point_id', 'LONG'),
              ('crossing_type', 'TEXT'), ('elevation', 'DOUBLE')]
    insert_records_into_featureclass_or_table(workspace, name, fields, crossing_records, True)


def add_gmfas(workspace, records, part__number, region):
    if len(records) == 0:
        return
    records = [r[0] for r in records]
    name = region + '_GMFA_{}'.format(part__number)
    fields = [('segment_id', 'LONG'), ('lane_id', 'TEXT'), ('type', 'TEXT'), ('start_xsectionId', 'LONG'),
              ('end_xsectionId', 'LONG')]
    insert_records_into_featureclass_or_table(workspace, name, fields, records, True)


def add_markers(workspace, part_number, marker_records, region):
    if not marker_records or len(marker_records) == 0:
        return
    name = region + '_MARKERS_{}'.format(part_number)
    fields = [('segment_id', 'LONG'), ('xsection_id', 'LONG'), ('point_id', 'LONG'),
              ('marker_id', 'LONG'), ('width', 'DOUBLE'), ('type', 'TEXT'), ('color', 'TEXT'), ('lat', 'DOUBLE'),
              ('lon', 'DOUBLE'), ('left_right', 'TEXT')]
    insert_records_into_featureclass_or_table(workspace, name, fields, marker_records, False)


def add_barriers(workspace, part_number, barrier_records, region):
    if not barrier_records or len(barrier_records) == 0:
        return
    name = region + '_BARRIERS_{}'.format(part_number)
    fields = [('segment_id', 'LONG'), ('xsection_id', 'LONG'),
              ('height', 'DOUBLE'), ('type', 'TEXT'),
              ('elevation', 'DOUBLE'), ('side', 'TEXT')]
    insert_records_into_featureclass_or_table(workspace, name, fields, barrier_records, False)


def extract_lane_file(arg):
    lxsx, rsgx_xml, gmfa_xml, data, part_number, poly, output_location = arg

    status_out = os.path.join(output_location, 'status')
    if not os.path.exists(status_out):
        os.mkdir(status_out)

    lane_records = []
    sign_records = []
    connectivity_records = []
    segment_records = []
    edges_records = []
    crossing_records = []
    marker_records = []
    barrier_records = []
    gmfa_records = []

    try:
        status_file = os.path.join(status_out, os.path.basename(lxsx))
        if os.path.exists(status_file):
            return lane_records, sign_records, connectivity_records, segment_records, \
                   edges_records, crossing_records, marker_records, barrier_records, gmfa_records

        xml_data = objectify.parse(lxsx)
        root = xml_data.getroot()

        if not hasattr(root, 'segment'):
            return lane_records, sign_records, connectivity_records, segment_records, \
                   edges_records, crossing_records, marker_records, barrier_records, gmfa_records

        for segment in root.segment:
            segment_id = int(segment.attrib['id'])
            for xsection in segment.xSection:
                xsection_id = int(xsection.attrib['id'])
                if hasattr(xsection, 'lane'):
                    for lane in xsection.lane:
                        _crossings, _lane_record = \
                            _extract_lanes_and_crossings(segment_id, xsection_id, lane, len(xsection.lane),
                                                         xsection.point,
                                                         xsection.marker, poly, data)
                        if _crossings == -1:
                            continue
                        if _lane_record:
                            lane_records.append(_lane_record)

                            # extract other layers
                            edges, markers, barriers = \
                                extract_markers_barriers_edges(segment_id, xsection, data, lane, len(xsection.lane))

                            edges_records += edges
                            crossing_records += _crossings
                            marker_records += markers
                            barrier_records += barriers

        if len(lane_records) > 0:
            segment_ids = list(set(list(map(lambda l: l[0], lane_records))))
            if 'GMFA' in data:
                # gmfa_xml = list(filter(lambda x: os.path.basename(x).split('.')[0] == file_name, fasx_files))[0]
                gmfa_records += extract_gmfa(gmfa_xml, segment_ids)

            if 'SIGNS' in data or 'CONNECTIVITIES' in data or 'SEGMENTS' in data:
                # rsgx_xml = list(filter(lambda x: os.path.basename(x).split('.')[0] == file_name, rsgx_files))[0]
                signs, conns, segments = extract_signs_conn_segments(rsgx_xml, segment_ids, data)
                sign_records += signs
                connectivity_records += conns
                segment_records += segments

        with open(status_file, 'w') as file:
            file.write('completed')
    except Exception as err:
        log(str(err))

    return lane_records, sign_records, connectivity_records, segment_records, \
           edges_records, crossing_records, marker_records, barrier_records, gmfa_records


def find_file(xfile, extension):
    path = os.path.dirname(xfile)
    file_name = os.path.basename(xfile).split('.')[0]

    file = os.path.join(path, '{}{}'.format(file_name, extension))
    if not os.path.exists(file):
        print('ERROR: {} NOT FOUND'.format(file))
        return ''
    return file


def read_status(count, output_location, msg):
    files = glob(os.path.join(output_location, 'status/*.*'), recursive=False)
    counter = len(files)
    message = '{}/{} ({}%)'.format(counter, count, round(counter / count * 100))
    if message == msg:
        return msg

    log(message, output_location)
    return message


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def extract_all_features(arg):
    lxsx_files, data, bounding_shp, part_number, region, region_output_location = arg
    polygon = None
    if bounding_shp:
        polygon = None
        with arcpy.da.SearchCursor(bounding_shp, ['SHAPE@']) as cursor:
            for row in cursor:
                polygon = row[0]

    log('Creating processing arguments...', region_output_location)
    args = [(l, find_file(l, '.rsgx'), find_file(l, '.fasx'), data, part_number, polygon, region_output_location)
            for l in lxsx_files]

    log('{} arguments found'.format(len(args)), region_output_location)

    chunks = divide_chunks(args, 100)

    fgdb = os.path.join(region_output_location, region + "_" + str(part_number)) + ".gdb"

    if not arcpy.Exists(fgdb):
        fgdb = arcpy.CreateFileGDB_management(os.path.dirname(fgdb), os.path.basename(fgdb).split('.')[0]).getOutput(0)

    for chunk in chunks:
        lane_records = []
        sign_records = []
        connectivity_records = []
        segment_records = []
        edges_records = []
        crossing_records = []
        marker_records = []
        barrier_records = []
        gmfa_records = []
        log('Processing started...')

        m = ProdConsumer(extract_lane_file, chunk, 6)
        m.start()

        message = None
        while True:
            message = read_status(len(lxsx_files), region_output_location, message)
            if m.is_completed():
                break
            time.sleep(60)
        list_of_list = m.get_results()

        log('Reading xml files completed.', region_output_location)
        for tpl in list_of_list:
            lane_records += tpl[0]
            sign_records += tpl[1]
            connectivity_records += tpl[2]
            segment_records += tpl[3]
            edges_records += tpl[4]
            crossing_records += tpl[5]
            marker_records += tpl[6]
            barrier_records += tpl[7]
            gmfa_records += tpl[8]
        m.close()

        if len(lane_records) > 0:
            add_lanes(fgdb, part_number, lane_records, region)
            lane_records.clear()

        if len(sign_records) > 0:
            add_signs(fgdb, part_number, sign_records, region)
            sign_records.clear()

        if len(connectivity_records) > 0:
            add_connectivity(fgdb, part_number, connectivity_records, region)
            connectivity_records.clear()

        if len(segment_records) > 0:
            add_road_segment(fgdb, part_number, segment_records, region)
            segment_records.clear()

        if len(edges_records) > 0:
            add_edges(fgdb, part_number, edges_records, region)
            edges_records.clear()

        if len(crossing_records) > 0:
            add_crossings(fgdb, part_number, crossing_records, region)
            crossing_records.clear()

        if len(marker_records) > 0:
            add_markers(fgdb, part_number, marker_records, region)
            marker_records.clear()

        if len(barrier_records) > 0:
            add_barriers(fgdb, part_number, barrier_records, region)
            barrier_records.clear()

        if len(gmfa_records) > 0:
            add_gmfas(fgdb, gmfa_records, part_number, region)
            gmfa_records.clear()

        message = ''
        message = read_status(len(lxsx_files), region_output_location, message)


def is_point_in_polygon(lat, lng, poly):
    pt = arcpy.Point(lng, lat)
    is_within = poly.contains(pt) or pt.within(poly)
    return is_within


def extract_region(arg):
    try:
        start = datetime.datetime.now()

        zip_file, region, output_location = arg

        region_output_location = os.path.join(output_location, region)

        if not os.path.exists(region_output_location):
            os.mkdir(region_output_location)

        log('Process({}) Unzipping {} file...'.format(os.getpid(), region), region_output_location)
        extracted_folder = os.path.join(region_output_location, 'extracted')
        zipf, files = unpack_zip(zip_file, extracted_folder)

        lxsx_files = list(filter(lambda x: x.endswith('lxsx'), files))
        log('Unzipping {} completed in {}'.format(region, datetime.datetime.now() - start), region_output_location)
        return region, lxsx_files, region_output_location
    except Exception as ex:
        log(str(ex))


def process_region(kwds):
    try:
        zip_files = kwds['zip_files']
        regions = kwds['regions']

        assert(len(zip_files) == len(regions))

        part_number = kwds['part_number']
        data = kwds['data']
        bounds = kwds['bounds']

        args = []
        for i in range(len(regions)):
            zip_file = zip_files[i]
            region = regions[i]
            args.append((zip_file, region, kwds['output_location']))

        m = ProdConsumer(extract_region, args, 10)
        m.start()

        while True:
            if m.is_completed():
                break
            time.sleep(10)
        regions_files = m.get_results()
        m.close()

        args = []
        for region_lxsx_files in regions_files:
            region, lxsx_files, region_output_location = region_lxsx_files
            #args.append((lxsx_files, data, bounds, part_number, region, region_output_location))
            extract_all_features((lxsx_files, data, bounds, part_number, region, region_output_location))

        # m = ProdConsumer(extract_all_features, args, 10)
        # m.start()
        #
        # while True:
        #     log('Checking status...', kwds['output_location'])
        #     if m.is_completed():
        #         break
        #     time.sleep(60)
        # m.get_results()
        # m.close()

        with open(os.path.join(kwds['output_location'], 'completed.txt'), 'w') as writer:
            writer.write(str(datetime.datetime.now()))
    except Exception as ex:
        log(str(ex), kwds['output_location'])
        with open(os.path.join(kwds['output_location'], 'error.txt'), 'w') as writer:
            writer.write(str(ex))


if __name__ == '__main__':
    start = dt.now()

    try:

        with open('xml.info', 'r') as cursor:
            lines = cursor.readlines()
            xml_zip = lines[0].split('=')[1].strip('\n').strip(' ')
            regions = lines[1].split('=')[1].strip('\n').strip(' ')
            aoi = lines[2].split('=')[1].strip('\n').strip(' ')
            layers = lines[3].split('=')[1].strip('\n').strip(' ')
            part_number = lines[4].split('=')[1].strip('\n').strip(' ')
            output_location = lines[5].split('=')[1].strip('\n').strip(' ')

        if not xml_zip or not os.path.exists(xml_zip) or not xml_zip.endswith('.zip'):
            raise Exception('Zip file not provided or does not exist or invalid')

        ALL_REGIONS = ['CAN_ALBERTA', 'CAN_BRITISH_COLUMBIA', 'CAN_MANITOBA', 'CAN_NEW_BRUNSWICK', 'CAN_NOVA_SCOTIA',
                       'CAN_ONTARIO', 'CAN_QUEBEC', 'CAN_SASKATCHEWAN',
                       'USA_ALABAMA', 'USA_ARIZONA', 'USA_ARKANSAS', 'USA_CALIFORNIA', 'USA_COLORADO',
                       'USA_CONNECTICUT', 'USA_DC', 'USA_DELAWARE', 'USA_FLORIDA', 'USA_GEORGIA', 'USA_IDAHO',
                       'USA_ILLINOIS', 'USA_INDIANA', 'USA_IOWA', 'USA_KANSAS', 'USA_KENTUCKY', 'USA_LOUISIANA',
                       'USA_MAINE', 'USA_MARYLAND', 'USA_MASSACHUSETTS', 'USA_MICHIGAN', 'USA_MINNESOTA',
                       'USA_MISSISSIPPI', 'USA_MISSOURI', 'USA_MONTANA', 'USA_NEBRASKA', 'USA_NEVADA',
                       'USA_NEW_HAMPSHIRE', 'USA_NEW_JERSEY', 'USA_NEW_MEXICO', 'USA_NEW_YORK', 'USA_NORTH_CAROLINA',
                       'USA_NORTH_DAKOTA', 'USA_OHIO', 'USA_OKLAHOMA', 'USA_OREGON', 'USA_PENNSYLVANIA',
                       'USA_RHODE_ISLAND', 'USA_SOUTH_CAROLINA', 'USA_SOUTH_DAKOTA', 'USA_TENNESSEE', 'USA_TEXAS',
                       'USA_UTAH', 'USA_VERMONT', 'USA_VIRGINIA', 'USA_WASHINGTON', 'USA_WEST_VIRGINIA',
                       'USA_WISCONSIN', 'USA_WYOMING']


        if regions == 'ALL' or not regions or len(regions) == 0:
            regions = ','.join(ALL_REGIONS)
        elif regions == 'US':
            regions = ','.join(list(filter(lambda x: x.startswith('USA'), ALL_REGIONS)))
        elif regions == 'CA':
            regions = ','.join(list(filter(lambda x: x.startswith('CAN'), ALL_REGIONS)))
        print('REGIONS: ' + regions)

        if not layers or len(layers) == 0:
            layers = 'GMFA,SEGMENTS,CONNECTIVITIES,SIGNS,MARKERS,BARRIERS,EDGES,CROSSINGS'
            print('Layers not provided. All layers will be extracted')

        if not part_number:
            part_number = 'NO_PART_NUMBER'

        if not aoi or len(aoi) == 0:
            aoi = None
            print('Area of Interest not provided. The entire selected state/province will be extracted.')

        if not output_location:
            raise Exception('Output location is required')

        data_file = xml_zip
        regions = regions.split(',') if not isinstance(regions, list) else regions
        layers = layers.split(',') if not isinstance(layers, list) else layers

        # out = os.path.join(output_location, 'xml2fgdb_' + str(part_number) + '_' + str(time.time())).replace('.', '')
        # if not os.path.exists(out):
        #     os.mkdir(out)
        #
        # argstr = '\n==============ARGUMENTS================='
        # argstr += '\nZip file: {}'.format(data_file)
        # argstr += '\nPart Number: {}'.format(part_number)
        # argstr += '\nStates: {}'.format(regions)
        # argstr += '\nLayers: {}'.format(layers)
        # argstr += '\nAOI: {}'.format(aoi)
        # argstr += '\nOutput Location: {}'.format(out)
        #
        # log(argstr, out)
        #
        # log('Extracting {}...'.format(data_file), out)
        # with ZipFile(data_file) as zip_file:
        #     members = list(
        #         filter(lambda member: os.path.basename(member).replace('.zip', '') in regions,
        #                zip_file.namelist()))
        #     if len(members) > 0:
        #         for m in members:
        #             zip_file.extract(m, out)
        # log('Extracting completed', out)

        #select all regional zip files
        out = r'E:\xml2fgdb\xml2fgdb_86535387_1676446747541511'
        data_folder = os.path.join(out, 'xmls_zipped')
        compressed_files = list(
            filter(lambda f: os.path.basename(f)[:-4] in regions, glob(os.path.join(data_folder, '*.zip'))))

        if len(compressed_files) == 0:
            print('0 zipped files found')
            sys.exit(0)

        arg = {
            'zip_files': compressed_files,
            'output_location': out,
            'part_number': part_number,
            'data': layers,
            'bounds': aoi,
            'regions': regions
        }

        process_region(arg)
    except Exception as err:
        print(err)

    print(dt.now() - start)
