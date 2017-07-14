import datetime
import glob
import gzip
import json
import os
from collections import defaultdict
from logging import getLogger
from typing import Dict
from typing import List

import itertools

from .utils import timestamp_parse, obj_to_json, timestamp, hash_dict

logger = getLogger('query_result_storage')


class IndexStorage:
    def __init__(self, result_storage):
        self.result_storage = result_storage

        self.ids_queries = self.result_storage.find_query_ids()
        self.ids_results = self.result_storage.find_result_ids()

        logger.info('Discovered - Queries: {}; Results: {}'.format(len(self.ids_queries), len(self.ids_results)))

        self.index_query_result_dates = defaultdict(list)  # type: Dict[str, List[datetime.datetime]]
        self.index_query_result_ids = defaultdict(list)  # type: Dict[str, List[str]]

        for id_result in self.ids_results:
            query_date, query_id = ResultsStorage.parse_result_id(id_result)
            self.index_query_result_dates[query_id].append(query_date)
            self.index_query_result_ids[query_id].append(id_result)

    def filter_query_ids(self, filter_by_query, queries=None):
        if queries is None:
            queries = self.result_storage.load_queries(self.ids_queries)

        query_ids = [
            self.ids_queries[index]
            for index, query in enumerate(queries)
            if filter_by_query(query)
        ]

        logger.info('Found {} queries for given filter'.format(len(query_ids)))

        return query_ids, queries

    def get_query_result_ids(self, query_ids):
        result_ids = list(
            itertools.chain(*[
                self.index_query_result_ids[query_id]
                for query_id in query_ids
            ])
        )

        logger.debug('Found a total of {} results'.format(len(result_ids)))

        return result_ids


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
    def __init__(self, path_dir_storage, compress_fs_objects=True):
        self.path_dir_storage = path_dir_storage

        # Initialize default values
        self.DIR_NAME_QUERIES = 'queries'
        self.DIR_NAME_RESULTS = 'results'
        self.FILE_EXTENSION = '.json'

        if compress_fs_objects:
            self.FILE_EXTENSION += '.gz'

        if not compress_fs_objects:
            self._open_func = open
        else:
            self._open_func = gzip.open

        self.init_storage_dir()

    def init_storage_dir(self):
        os.makedirs(os.path.join(self.path_dir_storage, self.DIR_NAME_QUERIES), exist_ok=True)
        os.makedirs(os.path.join(self.path_dir_storage, self.DIR_NAME_RESULTS), exist_ok=True)

    def find_result_ids(self):
        list_paths = glob.glob(os.path.join(self.path_dir_storage, self.DIR_NAME_RESULTS, '*' + self.FILE_EXTENSION))
        results = []
        for path_file in list_paths:
            filename = os.path.basename(path_file)
            result_id = filename[:-len(self.FILE_EXTENSION)]
            results.append(result_id)

        return results

    def find_query_ids(self):
        list_paths = glob.glob(os.path.join(self.path_dir_storage, self.DIR_NAME_QUERIES, '*' + self.FILE_EXTENSION))
        queries = []
        for path_file in list_paths:
            filename = os.path.basename(path_file)
            query_id = filename[:-len(self.FILE_EXTENSION)]
            queries.append(query_id)

        return queries

    def path_query(self, query_id):
        return os.path.join(self.path_dir_storage, self.DIR_NAME_QUERIES, query_id + self.FILE_EXTENSION)

    def path_result(self, result_id):
        return os.path.join(self.path_dir_storage, self.DIR_NAME_RESULTS, result_id + self.FILE_EXTENSION)

    def load_query(self, query_id):
        with self._open_func(self.path_query(query_id), 'rt', encoding='utf-8') as fp:
            query = json.loads(fp.read())

        logger.debug('{}: {}'.format(query_id, obj_to_json(query, indent=None)))
        return query

    def load_queries(self, query_ids):
        return [self.load_query(query_id) for query_id in query_ids]

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
            with self._open_func(path_file, 'wt', encoding='utf-8') as fp:
                fp.write(obj_to_json(query_obj))

        path_file = self.path_result(result_id)
        with self._open_func(path_file, 'wt', encoding='utf-8') as fp:
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
        with self._open_func(self.path_query(query_id), 'rt', encoding='utf-8') as fp:
            query_str = fp.read()

        with self._open_func(self.path_result(result_id), 'rt', encoding='utf-8') as fp:
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
