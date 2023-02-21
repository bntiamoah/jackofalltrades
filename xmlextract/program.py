import fasx
import lxsx
import json
from datetime import datetime as dt
from lxsx import read_all
import threading


def func(funct, var=None):
    data = []
    if var:
        for item in funct('data/TX/USA_TEXAS_10569.lxsx', var):
            # print(item)
            data.append(item)
    else:
        for item in funct('data/TX/USA_TEXAS_10569.lxsx'):
            # print(item)
            data.append(item)
    print('{}, {}'.format(funct, len(data)))
    return data


if __name__ == '__main__':
    start = dt.now()

    data = read_all('data/TX/USA_TEXAS_10569.lxsx')

    print(dt.now() - start)

    print(json.dumps(data))

    # t1 = threading.Thread(target=func, args=(read_lanes,))
    # # for item in read_lanes('data/TX/USA_TEXAS_10569.lxsx'):
    # #     print(item)
    #
    # t2 = threading.Thread(target=func, args=(read_markers,))
    # # for item in read_markers('data/TX/USA_TEXAS_10569.lxsx'):
    # #     print(item)
    #
    # t3 = threading.Thread(target=func, args=(read_points,))
    # # for item in read_points('data/TX/USA_TEXAS_10569.lxsx'):
    # #     print(item)
    #
    # t4 = threading.Thread(target=func, args=(read_barriers, 'left'))
    # # for item in read_barriers('data/TX/USA_TEXAS_10569.lxsx', 'left'):
    # #     print(item)
    #
    # t5 = threading.Thread(target=func, args=(read_barriers, 'right'))
    # # for item in read_barriers('data/TX/USA_TEXAS_10569.lxsx', 'right'):
    # #     print(item)
    #
    # t6 = threading.Thread(target=func, args=(read_edges, 'left'))
    # # for item in read_edges('data/TX/USA_TEXAS_10569.lxsx', 'left'):
    # #     print(item)
    #
    # t7 = threading.Thread(target=func, args=(read_edges, 'right'))
    # # for item in read_edges('data/TX/USA_TEXAS_10569.lxsx', 'right'):
    # #     print(item)

    # t1.start()
    # t2.start()
    # t3.start()
    # t4.start()
    # t5.start()
    # t6.start()
    # t7.start()
    #
    # t1.join()
    # t2.join()
    # t3.join()
    # t4.join()
    # t5.join()
    # t6.join()
    # t7.join()

# from bigxml import Parser, xml_handle_element
# from dataclasses import dataclass
#
#
# @xml_handle_element('segments', 'segment', 'xSection')
# def handler(node):
#     yield node.attributes['id']
#
#
# @xml_handle_element('feed', 'entry', 'link')
# def handler2(node):
#     yield node.attributes['href']
#
#
# @xml_handle_element('segm', 'entry')
# @dataclass
# class Entry:
#     title: str = 'N/A'
#     link: str = 'N/A'
#
#     @xml_handle_element('title')
#     def handle_title(self, node):
#         self.title = node.text
#
#     @xml_handle_element('link')
#     def handle_link(self, node):
#         self.link = node.attributes['href']
#
#
# with open('data/atom.xml', 'rb') as f:
#     for item in Parser(f).iter_from(Entry):
#         print(item)
