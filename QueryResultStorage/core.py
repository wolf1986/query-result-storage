import datetime
import glob
import gzip
import json
import os
from collections import defaultdict
from logging import getLogger
from typing import Dict
from typing import List

from QueryResultStorage.utils import timestamp_parse, obj_to_json, timestamp, hash_dict

DIR_NAME_QUERIES = 'queries'
DIR_NAME_RESULTS = 'results'

logger = getLogger('QueryResultStorage')


class IndexStorage:
    def __init__(self, result_storage):
        self.result_storage = result_storage

        self.ids_queries = self.result_storage.find_query_ids()
        self.ids_results = self.result_storage.find_result_ids()
        self.index_result_dates = defaultdict(list)  # type: Dict[str, List[datetime.datetime]]

        for id_result in self.ids_results:
            query_date, query_id = ResultsStorage.parse_result_id(id_result)
            self.index_result_dates[query_id].append(query_date)

    def load_all_queries(self):
        return [
            self.result_storage.load_query(query_id)
            for query_id in self.ids_queries
        ]


class QueryResultRaw:
    def __init__(self, query_obj, result_obj, sample_time=None):
        """
            Assume data is encoded as utf-8
        """

        if sample_time is None:
            sample_time = datetime.datetime.now()

        self.query_obj = query_obj
        self.result_obj = result_obj

        self.sample_time = sample_time
        self.query_id = None
        self.result_id = None


class ResultsStorage:
    def __init__(self, path_dir_storage):
        self.path_dir_storage = path_dir_storage
        self.init_storage_dir()

    def init_storage_dir(self):
        os.makedirs(os.path.join(self.path_dir_storage, DIR_NAME_QUERIES), exist_ok=True)
        os.makedirs(os.path.join(self.path_dir_storage, DIR_NAME_RESULTS), exist_ok=True)

    def find_result_ids(self):
        list_paths = glob.glob(os.path.join(self.path_dir_storage, DIR_NAME_RESULTS, '*.json'))
        results = []
        for path_file in list_paths:
            filename = os.path.basename(path_file)
            result_id, _ = os.path.splitext(filename)
            results.append(result_id)

        return results

    def find_query_ids(self):
        list_paths = glob.glob(os.path.join(self.path_dir_storage, DIR_NAME_QUERIES, '*.json'))
        queries = []
        for path_file in list_paths:
            filename = os.path.basename(path_file)
            query_id, _ = os.path.splitext(filename)
            queries.append(query_id)

        return queries

    def path_query(self, query_id):
        return os.path.join(self.path_dir_storage, DIR_NAME_QUERIES, query_id + '.json')

    def path_result(self, result_id):
        return os.path.join(self.path_dir_storage, DIR_NAME_RESULTS, result_id + '.json.gz')

    def load_query(self, query_id):
        with open(self.path_query(query_id), 'rt', encoding='utf-8') as fp:
            return json.loads(fp.read())

    def save_query_and_result(self, query_obj, result_obj, sample_time=None):
        """
            Assume data is encoded as utf-8
        """

        if sample_time is None:
            sample_time = datetime.datetime.now()

        query_obj = query_obj
        result_obj = result_obj

        sample_time = sample_time
        query_id = hash_dict(query_obj)

        result_id = '{}_{}'.format(timestamp(sample_time), query_id)

        logger.debug('Saving result: {}'.format(result_id))

        path_file = self.path_query(query_id)
        if not os.path.exists(path_file):
            with open(path_file, 'wt', encoding='utf-8') as fp:
                fp.write(obj_to_json(query_obj))

        path_file = self.path_result(result_id)
        with gzip.open(path_file, 'wt', encoding='utf-8') as fp:
            fp.write(obj_to_json(result_obj))

        return query_id, result_id

    @classmethod
    def parse_result_id(cls, result_id):
        """
            :return: query_time, query_id
        """
        str_date, str_time, query_id = result_id.split('_', maxsplit=2)
        query_time = timestamp_parse('{}_{}'.format(str_date, str_time))
        return query_time, query_id

    @classmethod
    def parse_result_path(cls, path_file):
        file_no_extension, _ = os.path.splitext(os.path.basename(path_file))
        return cls.parse_result_id(file_no_extension)

    def load_result_id(self, result_id) -> QueryResultRaw:
        """
            :param result_id: '%Y%m%d_%H%M%S_HASH'
        """

        _, query_id = self.parse_result_id(result_id)
        with open(self.path_query(query_id), 'rt', encoding='utf-8') as fp:
            query_str = fp.read()

        with gzip.open(self.path_result(result_id), 'rt', encoding='utf-8') as fp:
            result_str = fp.read()

        query_time, str_hash = self.parse_result_id(result_id)
        query_result = QueryResultRaw(
            json.loads(query_str),
            json.loads(result_str),
            query_time
        )
        query_result.query_id = query_id
        query_result.result_id = result_id

        if str_hash != query_id:
            raise Exception("Bad result encountered, result hash doesn't match")

        return query_result


# class QueryResultRawOld:
    # def __init__(self, result_storage: ResultsStorage, query_obj, result_obj, sample_time=None):
    #     """
    #         Assume data is encoded as utf-8
    #     """
    #
    #     self.result_storage = result_storage
    #
    #     if sample_time is None:
    #         sample_time = datetime.datetime.now()
    #
    #     self.query_obj = query_obj
    #     self.result_obj = result_obj
    #
    #     self.sample_time = sample_time
    #     self.query_id = hash_dict(query_obj)
    #
    #     self.result_id = '{}_{}'.format(timestamp(self.sample_time), self.query_id)

    # def get_path_query_of_result(self, result_id):
    #     _, query_id = self.parse_result_id(result_id)
    #     return self.result_storage.path_query(query_id)

    # def get_path_result_diff(self, result_id):
    #     path_regular = Path(self.result_storage.path_result(result_id))
    #     return str(
    #         path_regular.with_suffix(".diff" + path_regular.suffix)
    #     )

    # def save(self):
    #     logger.debug('Saving result: {}'.format(self.result_id))
    #
    #     path_file = self.result_storage.path_query(self.query_id)
    #     if not os.path.exists(path_file):
    #         with open(path_file, 'wt', encoding='utf-8') as fp:
    #             fp.write(obj_to_json(self.query_obj))
    #
    #     path_file = self.result_storage.path_result(self.result_id)
    #     with open(path_file, 'wt', encoding='utf-8') as fp:
    #         fp.write(obj_to_json(self.result_obj))

    # def save_result_diff(self, obj_relative, result_to_id, obj_to_results):
    #     # Create an index with previous results by id
    #     dict_obj_previous = {}
    #     for result in obj_to_results(obj_relative):
    #         dict_obj_previous[result_to_id(result)] = hash_dict(result)
    #
    #     obj_diff_records = {}
    #
    #     # Compare current results with previous - New & Changed
    #     ids_current = set()
    #     for result in self.result_obj:
    #         res_id = result_to_id(result)
    #         ids_current.add(res_id)
    #         res_hash = hash_dict(result)
    #         if res_id not in dict_obj_previous and res_hash != dict_obj_previous[res_id]:
    #             # New or updated Result
    #             obj_diff_records[res_id] = result
    #
    #     # Mark as deleted
    #     ids_previous = set(dict_obj_previous.keys())
    #     ids_deleted = ids_previous.difference(ids_current)
    #     for res_id in ids_deleted:
    #         obj_diff_records[res_id] = None
    #
    #     # Add additional fields
    #     obj_diff = {
    #         '__type': 'diff',
    #         'path_previous': self.result_storage.path_result(self.result_id),
    #         'records': obj_diff_records,
    #     }
    #
    #     path_file = self.get_path_result_diff(self.result_id)
    #     with open(path_file, 'wt', encoding='utf-8') as fp:
    #         fp.write(obj_to_json(obj_diff))
