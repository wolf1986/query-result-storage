import datetime
import os
import shutil
from pathlib import Path
from unittest import TestCase

from QueryResultStorage.core import ResultsStorage


class Paths:
    DirData = Path(__file__).parent / 'temp' / 'storage'  # type: Path


class Mocks:
    ObjQuery = {
        'id': 'test',
        'query_str': 'some query string'
    }

    ObjResult1 = {
        'id': 'res1',
        'result': 'data1'
    }

    ObjResult2 = {
        'id': 'res2',
        'result': 'data2'
    }


class TestQueryResultStorage(TestCase):
    results_storage = None  # type: ResultsStorage

    @classmethod
    def setUpClass(cls):
        if Paths.DirData.exists():
            shutil.rmtree(str(Paths.DirData))

        os.makedirs(str(Paths.DirData), exist_ok=True)
        cls.results_storage = ResultsStorage(str(Paths.DirData))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(str(Paths.DirData))

    def test_nominal_flow(self):
        query_id, result_id1 = self.results_storage.save_query_and_result(
            Mocks.ObjQuery, Mocks.ObjResult1, datetime.datetime.now() + datetime.timedelta(seconds=0)
        )

        query_id, result_id2 = self.results_storage.save_query_and_result(
            Mocks.ObjQuery, Mocks.ObjResult2, datetime.datetime.now() + datetime.timedelta(seconds=1)
        )

        self._load_verify_objects(Mocks.ObjQuery, Mocks.ObjResult1, query_id, result_id1)
        self._load_verify_objects(Mocks.ObjQuery, Mocks.ObjResult2, query_id, result_id2)

    def _load_verify_objects(self, obj_query, obj_result, query_id, result_id):
        query_result_raw = self.results_storage.load_result_id(result_id)

        self.assertEqual(query_result_raw.query_obj, obj_query)
        self.assertEqual(query_result_raw.query_id, query_id)
        self.assertEqual(query_result_raw.result_obj, obj_result)
        self.assertEqual(query_result_raw.result_id, result_id)
