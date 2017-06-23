import datetime
import hashlib
import json


def obj_to_json(obj, indent=4):
    def json_serialization_default_handler(obj_of_handler):
        if isinstance(obj_of_handler, datetime.datetime):
            return obj_of_handler.isoformat()

        raise TypeError("Unknown type")

    return json.dumps(
        obj, ensure_ascii=False, indent=indent, sort_keys=True,
        default=json_serialization_default_handler
    )


def timestamp_parse(str_time):
    return datetime.datetime.strptime(str_time, '%Y%m%d_%H%M%S')


def hash_string(str_src):
    return hashlib.sha1(str_src.encode('utf-8')).hexdigest()


def hash_dict(dic):
    return hash_string(obj_to_json(dic))


def timestamp(time_obj=None):
    if time_obj is None:
        time_obj = datetime.datetime.now()

    return '{:%Y%m%d_%H%M%S}'.format(time_obj)
