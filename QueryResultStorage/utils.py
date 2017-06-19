import datetime
import hashlib
import json
import os
from collections import OrderedDict
from functools import wraps
from time import time
from urllib import parse


def obj_to_json(obj, indent=4):
    def json_serialization_default_handler(obj_of_handler):
        if isinstance(obj_of_handler, datetime.datetime):
            return obj_of_handler.isoformat()

        raise TypeError("Unknown type")

    return json.dumps(
        obj, ensure_ascii=False, indent=indent, sort_keys=True,
        default=json_serialization_default_handler
    )


def dict_to_string(dic):
    ordered_dict = OrderedDict(reversed(sorted(dic.items(), key=lambda x: x[0])))
    return parse.urlencode(ordered_dict, encoding='utf-8')


def timestamp_parse(str_time):
    return datetime.datetime.strptime(str_time, '%Y%m%d_%H%M%S')


def timestamp_parse_path(path_file):
    filename = os.path.basename(path_file)
    filename, _ = os.path.splitext(filename)
    stamp_date, stamp_time, data = filename.split('_', maxsplit=2)
    timestamp_str = '{}_{}'.format(stamp_date, stamp_time)

    return timestamp_parse(timestamp_str), data


def point_inside_polygon(pt, poly):
    x, y = pt
    n = len(poly)
    inside = False

    p1x, p1y = poly[0]
    for i in range(n + 1):
        p2x, p2y = poly[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    xinters = None
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside


def point_in_polygon(pt, poly_corners, inf):
    def ccw(a, b, c):
        return (c[1] - a[1]) * (b[0] - a[0]) > (b[1] - a[1]) * (c[0] - a[0])

    # Return true if line segments AB and CD intersect
    def intersect(a, b, c, d):
        return ccw(a, c, d) != ccw(b, c, d) and ccw(a, b, c) != ccw(a, b, d)

    result = False
    for i in range(len(poly_corners) - 1):
        is_intersect = intersect(
            [poly_corners[i].x, poly_corners[i][1]],
            [poly_corners[i + 1][0], poly_corners[i + 1][1]],
            [pt[0], pt[1]],
            [inf, pt[1]]
        )
        if is_intersect:
            result = not result
    is_intersect = intersect(
        [poly_corners[-1][0], poly_corners[-1][1]],
        [poly_corners[0][0], poly_corners[0][1]],
        [pt[0], pt[1]],
        [inf, pt[1]]
    )
    if is_intersect:
        result = not result
    return result


def timing(f, logger):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logger.debug('Function Timing: {} - {:2.4f} [sec]'.format(f.__name__, te - ts))
        return result

    return wrap


def hash_string(str_src):
    return hashlib.sha1(str_src.encode('utf-8')).hexdigest()


def hash_dict(dic):
    return hash_string(obj_to_json(dic))


def timestamp(time_obj=None):
    if time_obj is None:
        time_obj = datetime.datetime.now()

    return '{:%Y%m%d_%H%M%S}'.format(time_obj)
