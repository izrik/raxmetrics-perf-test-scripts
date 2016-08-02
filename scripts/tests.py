
from __future__ import division

import sys
import time
import unittest
import random
import math

import ingest
import ingestenum
import query
import annotationsingest
import abstract_thread
import thread_manager as tm
from abstract_thread import AbstractThread, generate_enum_metric_name

try:
    from com.xhaus.jyson import JysonCodec as json
except ImportError:
    import json
import pprint

pp = pprint.pprint
sleep_time = -1
get_url = None
post_url = None
post_payload = None


def mock_sleep(cls, x):
    global sleep_time
    sleep_time = x


class MockReq():
    def POST(self, url, payload):
        global post_url, post_payload
        post_url = url
        post_payload = payload
        return url, payload

    def GET(self, url):
        global get_url
        get_url = url
        return url

requests_by_type = {
    ingest.IngestThread:                        MockReq(),
    ingestenum.EnumIngestThread:                MockReq(),
    annotationsingest.AnnotationsIngestThread:  MockReq(),
    query.QueryThread:                          None,
    query.SinglePlotQuery:                      MockReq(),
    query.MultiPlotQuery:                       MockReq(),
    query.SearchQuery:                          MockReq(),
    query.EnumSearchQuery:                      MockReq(),
    query.EnumSinglePlotQuery:                  MockReq(),
    query.EnumMultiPlotQuery:                   MockReq(),
    query.AnnotationsQuery:                     MockReq(),
}


grinder_props = {
    'grinder.script': '../scripts/tests.py',
    'grinder.package_path': '/Library/Python/2.7/site-packages',
    'grinder.runs': '1',
    'grinder.threads': '45',
    'grinder.useConsole': 'false',
    'grinder.logDirectory': 'resources/logs',
    'grinder.bf.name_fmt': 't4.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d',
    'grinder.bf.report_interval': '10000',
    'grinder.bf.annotations_num_tenants': '4',
    'grinder.bf.num_tenants': '4',
    'grinder.bf.enum_num_tenants': '4',
    'grinder.bf.metrics_per_tenant': '15',
    'grinder.bf.enum_metrics_per_tenant': '5',
    'grinder.bf.batch_size': '5',
    'grinder.bf.ingest_concurrency': '15',
    'grinder.bf.enum_ingest_concurrency': '15',
    'grinder.bf.annotations_per_tenant': '5',
    'grinder.bf.annotations_concurrency': '5',
    'grinder.bf.num_nodes': '1',
    'grinder.bf.url': 'http://qe01.metrics-ingest.api.rackspacecloud.com',
    'grinder.bf.query_url': 'http://qe01.metrics.api.rackspacecloud.com',
    'grinder.bf.query_concurrency': '10',
    'grinder.bf.max_multiplot_metrics': '10',
    'grinder.bf.search_queries_per_interval': '10',
    'grinder.bf.enum_search_queries_per_interval': '10',
    'grinder.bf.multiplot_per_interval': '10',
    'grinder.bf.singleplot_per_interval': '10',
    'grinder.bf.enum_single_plot_queries_per_interval': '10',
    'grinder.bf.enum_multiplot_per_interval': '10',
    'grinder.bf.annotations_queries_per_interval': '8',
}


class InitProcessTest(unittest.TestCase):
    def setUp(self):
        self.real_shuffle = random.shuffle
        self.real_randint = random.randint
        self.real_time = abstract_thread.AbstractThread.time
        self.real_sleep = abstract_thread.AbstractThread.sleep
        self.tm = tm.ThreadManager(grinder_props, requests_by_type)
        req = MockReq()
        ingest.IngestThread.request = req
        ingestenum.EnumIngestThread.request = req
        annotationsingest.AnnotationsIngestThread.request = req
        for x in query.QueryThread.query_types:
            x.query_request = req
        random.shuffle = lambda x: None
        random.randint = lambda x, y: 0
        abstract_thread.AbstractThread.time = lambda x: 1000
        abstract_thread.AbstractThread.sleep = mock_sleep

        self.test_config = {'report_interval': (1000 * 6),
                            'num_tenants': 3,
                            'enum_num_tenants': 4,
                            'annotations_num_tenants': 3,
                            'metrics_per_tenant': 7,
                            'enum_metrics_per_tenant': 2,
                            'annotations_per_tenant': 2,
                            'batch_size': 3,
                            'ingest_concurrency': 2,
                            'enum_ingest_concurrency': 2,
                            'query_concurrency': 20,
                            'annotations_concurrency': 2,
                            'singleplot_per_interval': 11,
                            'multiplot_per_interval': 10,
                            'search_queries_per_interval': 9,
                            'enum_search_queries_per_interval': 9,
                            'enum_single_plot_queries_per_interval': 10,
                            'enum_multiplot_per_interval': 10,
                            'annotations_queries_per_interval': 8,
                            'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
                            'num_nodes': 2}

        ingest.default_config.update(self.test_config)

        self.num_query_nodes = self.test_config['num_nodes']
        self.single_plot_queries_agent0 = int(math.ceil(
            self.test_config['singleplot_per_interval'] /
            self.num_query_nodes))
        self.multi_plot_queries_agent0 = int(math.ceil(
            self.test_config['multiplot_per_interval'] /
            self.num_query_nodes))
        self.search_queries_agent0 = int(math.ceil(
            self.test_config[
                'search_queries_per_interval'] / self.num_query_nodes))
        self.enum_search_queries_agent0 = int(math.ceil(
            self.test_config[
                'enum_search_queries_per_interval'] / self.num_query_nodes))
        self.enum_single_plot_queries_agent0 = int(math.ceil(
            self.test_config[
                'enum_single_plot_queries_per_interval'] /
            self.num_query_nodes))
        self.enum_multi_plot_queries_agent0 = int(math.ceil(
            self.test_config[
                'enum_multiplot_per_interval'] / self.num_query_nodes))
        self.annotation_queries_agent0 = int(math.ceil(
            self.test_config[
                'annotations_queries_per_interval'] / self.num_query_nodes))


        self.single_plot_queries_agent1 = \
            self.test_config['singleplot_per_interval'] - \
            self.single_plot_queries_agent0
        self.multi_plot_queries_agent1 = \
            self.test_config['multiplot_per_interval'] - \
            self.multi_plot_queries_agent0
        self.search_queries_agent1 = \
            self.test_config['search_queries_per_interval'] - \
            self.search_queries_agent0
        self.enum_search_queries_agent1 = \
            self.test_config['enum_search_queries_per_interval'] - \
            self.enum_search_queries_agent0
        self.enum_single_plot_queries_agent1 = \
            self.test_config['enum_single_plot_queries_per_interval'] - \
            self.enum_single_plot_queries_agent0
        self.annotation_queries_agent1 = \
            self.test_config['annotations_queries_per_interval'] - \
            self.annotation_queries_agent0
        self.enum_multi_plot_queries_agent1 = \
            self.test_config['enum_multiplot_per_interval'] - \
            self.enum_multi_plot_queries_agent0

    def test_setup_thread_zero(self):
        # confirm that threadnum 0 is an ingest thread
        t1 = self.tm.setup_thread(0, 0)
        self.assertEqual(type(t1), ingest.IngestThread)

    def test_setup_thread_second_type(self):
        # confirm that the threadnum after all ingest threads is
        # EnumIngestThread
        t1 = self.tm.setup_thread(
            self.test_config['enum_ingest_concurrency'], 0)
        self.assertEqual(type(t1), ingestenum.EnumIngestThread)

    def test_setup_thread_third_type(self):
        # confirm that the threadnum after all ingest threads is a query thread
        t1 = self.tm.setup_thread(self.test_config['ingest_concurrency'] +
                                  self.test_config[
                                      'enum_ingest_concurrency'],
                                  0)
        self.assertEqual(type(t1), query.QueryThread)

    def test_setup_thread_fourth_type(self):
        # confirm that the threadnum after all ingest+query threads is an
        # annotations query thread
        t1 = self.tm.setup_thread(self.test_config['ingest_concurrency'] +
                                  self.test_config[
                                      'enum_ingest_concurrency'] +
                                  self.test_config['query_concurrency'],
                                  0)
        self.assertEqual(type(t1), annotationsingest.AnnotationsIngestThread)

    def test_setup_thread_invalid_thread_type(self):
        # confirm that a threadnum after all valid thread types raises an
        # exception
        tot_threads = (
            self.test_config['ingest_concurrency'] +
            self.test_config['enum_ingest_concurrency'] +
            self.test_config['query_concurrency'] +
            self.test_config['annotations_concurrency'])
        self.assertRaises(Exception, self.tm.setup_thread, (tot_threads, 0))

    def test_init_process_annotationsingest_agent_zero(self):

        # confirm that the correct batches of ingest metrics are created for
        # worker 0
        agent_num = 0
        # confirm annotationsingest
        annotationsingest.AnnotationsIngestThread.create_metrics(agent_num)

        self.assertEqual(annotationsingest.AnnotationsIngestThread.annotations,
                         [[0, 0], [0, 1], [1, 0], [1, 1]])

        thread = annotationsingest.AnnotationsIngestThread(
            0, agent_num, MockReq())
        self.assertEqual(thread.slice, [[0, 0], [0, 1]])

        thread = annotationsingest.AnnotationsIngestThread(
            1, agent_num, MockReq())
        self.assertEqual(thread.slice, [[1, 0], [1, 1]])

    def test_init_process_enumingest_agent_zero(self):
        agent_num = 0
        # confirm enum metrics ingest
        ingestenum.EnumIngestThread.create_metrics(agent_num)

        self.assertEqual(ingestenum.EnumIngestThread.metrics,
                         [
                             [[0, 0], [0, 1], [1, 0]],
                             [[1, 1]]
                         ])

        thread = ingestenum.EnumIngestThread(0, agent_num, MockReq())
        self.assertEqual(thread.slice, [[[0, 0], [0, 1], [1, 0]]])

        thread = ingestenum.EnumIngestThread(1, agent_num, MockReq())
        self.assertEqual(thread.slice, [[[1, 1]]])

    def test_init_process_ingest_agent_zero(self):

        agent_num = 0

        # confirm metrics ingest
        ingest.IngestThread.create_metrics(agent_num)

        self.assertEqual(ingest.IngestThread.metrics,
                         [[[0, 0], [0, 1], [0, 2]],
                          [[0, 3], [0, 4], [0, 5]],
                          [[0, 6], [1, 0], [1, 1]],
                          [[1, 2], [1, 3], [1, 4]],
                          [[1, 5], [1, 6]]])

        # confirm that the correct batch slices are created for individual
        # threads
        thread = ingest.IngestThread(0, agent_num, MockReq())
        self.assertEqual(thread.slice,
                         [[[0, 0], [0, 1], [0, 2]],
                          [[0, 3], [0, 4], [0, 5]],
                          [[0, 6], [1, 0], [1, 1]]])
        thread = ingest.IngestThread(1, agent_num, MockReq())
        self.assertEqual(thread.slice,
                         [[[1, 2], [1, 3], [1, 4]],
                          [[1, 5], [1, 6]]])

    def test_init_process_query_agent_zero_create_all_metrics(self):
        agent_num = 0
        # confirm that the number of queries is correctly distributed across
        #  each thread in this worker process
        queries = query.QueryThread._create_metrics(
            agent_num, query.QueryThread.query_types)

        self.assertEqual(
            queries,
            ([query.SinglePlotQuery] * self.single_plot_queries_agent0 +
             [query.MultiPlotQuery] * self.multi_plot_queries_agent0 +
             [query.SearchQuery] * self.search_queries_agent0 +
             [query.EnumSearchQuery] * self.enum_search_queries_agent0 +
             [query.EnumSinglePlotQuery] * self.enum_single_plot_queries_agent0 +
             [query.AnnotationsQuery] * self.annotation_queries_agent0) +
            [query.EnumMultiPlotQuery] * self.enum_multi_plot_queries_agent0)

    def test_init_process_query_agent_zero(self):
        agent_num = 0
        # confirm that the number of queries is correctly distributed across
        #  each thread in this worker process

        thread = query.QueryThread(0, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.SinglePlotQuery)
        self.assertIs(thread.slice[1], query.SinglePlotQuery)

        thread = query.QueryThread(3, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.MultiPlotQuery)
        self.assertIs(thread.slice[1], query.MultiPlotQuery)

        thread = query.QueryThread(6, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.SearchQuery)
        self.assertIs(thread.slice[1], query.SearchQuery)

        thread = query.QueryThread(9, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumSearchQuery)
        self.assertIs(thread.slice[1], query.EnumSearchQuery)

        thread = query.QueryThread(12, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumSinglePlotQuery)
        self.assertIs(thread.slice[1], query.EnumSinglePlotQuery)

        thread = query.QueryThread(14, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.AnnotationsQuery)
        self.assertIs(thread.slice[1], query.AnnotationsQuery)

        thread = query.QueryThread(16, agent_num, requests_by_type)
        self.assertEqual(1, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumMultiPlotQuery)

    def test_init_process_ingest_agent_one(self):

        agent_num = 1

        # confirm that the correct batches of ingest metrics are created for
        # worker 1
        ingest.IngestThread.create_metrics(agent_num)

        self.assertEqual(ingest.IngestThread.metrics,
                         [[[2, 0], [2, 1], [2, 2]],
                          [[2, 3], [2, 4], [2, 5]],
                          [[2, 6]]])

        thread = ingest.IngestThread(0, agent_num, MockReq())
        self.assertEqual(thread.slice,
                         [[[2, 0], [2, 1], [2, 2]],
                          [[2, 3], [2, 4], [2, 5]]])
        thread = ingest.IngestThread(1, agent_num, MockReq())
        self.assertEqual(thread.slice,
                         [[[2, 6]]])

    def test_init_process_annotationsingest_agent_one(self):
        agent_num = 1
        annotationsingest.AnnotationsIngestThread.create_metrics(agent_num)
        self.assertEqual(annotationsingest.AnnotationsIngestThread.annotations,
                         [[2, 0], [2, 1]])

    def test_init_process_query_agent_one_create_all_metrics(self):
        agent_num = 1
        queries = query.QueryThread._create_metrics(
            agent_num, query.QueryThread.query_types)

        # confirm that the correct batches of queries are created for worker 1

        self.assertEqual(
            queries,
            ([query.SinglePlotQuery] * self.single_plot_queries_agent1 +
             [query.MultiPlotQuery] * self.multi_plot_queries_agent1 +
             [query.SearchQuery] * self.search_queries_agent1 +
             [query.EnumSearchQuery] * self.enum_search_queries_agent1 +
             [query.EnumSinglePlotQuery] * self.enum_single_plot_queries_agent1 +
             [query.AnnotationsQuery] * self.annotation_queries_agent1) +
            [query.EnumMultiPlotQuery] * self.enum_multi_plot_queries_agent1)

    def test_init_process_query_agent_one(self):
        agent_num = 1

        thread = query.QueryThread(0, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.SinglePlotQuery)
        self.assertIs(thread.slice[1], query.SinglePlotQuery)

        thread = query.QueryThread(4, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.MultiPlotQuery)
        self.assertIs(thread.slice[1], query.MultiPlotQuery)

        thread = query.QueryThread(6, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.SearchQuery)
        self.assertIs(thread.slice[1], query.SearchQuery)

        thread = query.QueryThread(8, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumSearchQuery)
        self.assertIs(thread.slice[1], query.EnumSearchQuery)

        thread = query.QueryThread(10, agent_num, requests_by_type)
        self.assertEqual(2, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumSinglePlotQuery)
        self.assertIs(thread.slice[1], query.EnumSinglePlotQuery)

        thread = query.QueryThread(12, agent_num, requests_by_type)
        self.assertEqual(1, len(thread.slice))
        self.assertIs(thread.slice[0], query.AnnotationsQuery)

        thread = query.QueryThread(16, agent_num, requests_by_type)
        self.assertEqual(1, len(thread.slice))
        self.assertIs(thread.slice[0], query.EnumMultiPlotQuery)

    def tearDown(self):
        random.shuffle = self.real_shuffle
        random.randint = self.real_randint
        abstract_thread.AbstractThread.time = self.real_time
        abstract_thread.AbstractThread.sleep = self.real_sleep


class GeneratePayloadTest(unittest.TestCase):
    def setUp(self):
        self.real_shuffle = random.shuffle
        self.real_randint = random.randint
        self.real_time = abstract_thread.AbstractThread.time
        self.real_sleep = abstract_thread.AbstractThread.sleep
        self.tm = tm.ThreadManager(grinder_props, requests_by_type)
        req = MockReq()
        ingest.IngestThread.request = req
        ingestenum.EnumIngestThread.request = req
        annotationsingest.AnnotationsIngestThread.request = req
        for x in query.QueryThread.query_types:
            x.query_request = req
        random.shuffle = lambda x: None
        random.randint = lambda x, y: 0
        abstract_thread.AbstractThread.time = lambda x: 1000
        abstract_thread.AbstractThread.sleep = mock_sleep

        self.test_config = {'report_interval': (1000 * 6),
                            'num_tenants': 3,
                            'enum_num_tenants': 4,
                            'annotations_num_tenants': 3,
                            'metrics_per_tenant': 7,
                            'enum_metrics_per_tenant': 2,
                            'annotations_per_tenant': 2,
                            'batch_size': 3,
                            'ingest_concurrency': 2,
                            'enum_ingest_concurrency': 2,
                            'query_concurrency': 20,
                            'annotations_concurrency': 2,
                            'singleplot_per_interval': 11,
                            'multiplot_per_interval': 10,
                            'search_queries_per_interval': 9,
                            'enum_search_queries_per_interval': 9,
                            'enum_single_plot_queries_per_interval': 10,
                            'enum_multiplot_per_interval': 10,
                            'annotations_queries_per_interval': 8,
                            'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
                            'num_nodes': 2}

        ingest.default_config.update(self.test_config)

    def test_generate_payload(self):
        agent_num = 1
        ingest.IngestThread.create_metrics(agent_num)
        thread = ingest.IngestThread(0, agent_num, MockReq())
        payload = json.loads(
            thread.generate_payload(0, [[2, 3], [2, 4], [2, 5]]))
        valid_payload = [{u'collectionTime': 0,
                          u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3',
                          u'metricValue': 0,
                          u'tenantId': u'2',
                          u'ttlInSeconds': 172800,
                          u'unit': u'days'},
                         {u'collectionTime': 0,
                          u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4',
                          u'metricValue': 0,
                          u'tenantId': u'2',
                          u'ttlInSeconds': 172800,
                          u'unit': u'days'},
                         {u'collectionTime': 0,
                          u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5',
                          u'metricValue': 0,
                          u'tenantId': u'2',
                          u'ttlInSeconds': 172800,
                          u'unit': u'days'}]
        self.assertEqual(payload, valid_payload)

    def test_generate_enum_payload(self):
        agent_num = 1
        ingestenum.EnumIngestThread.create_metrics(agent_num)
        thread = ingestenum.EnumIngestThread(0, agent_num, MockReq())
        payload = json.loads(thread.generate_payload(1, [[2, 1], [2, 2]]))
        valid_payload = [{u'timestamp': 1,
                          u'tenantId': u'2',
                          u'enums': [{u'value': u'e_g_1_0',
                                      u'name': generate_enum_metric_name(
                                          1)}]},
                         {u'timestamp': 1,
                          u'tenantId': u'2',
                          u'enums': [{u'value': u'e_g_2_0',
                                      u'name': generate_enum_metric_name(
                                          2)}]}
                         ]
        self.assertEqual(payload, valid_payload)

    def test_generate_annotations_payload(self):
        agent_num = 1
        annotationsingest.AnnotationsIngestThread.create_metrics(agent_num)
        thread = annotationsingest.AnnotationsIngestThread(
            0, agent_num, MockReq())
        payload = json.loads(thread.generate_payload(0, 3))
        valid_payload = {
            'what': 'annotation int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3',
            'when': 0,
            'tags': 'tag',
            'data': 'data'}
        self.assertEqual(payload, valid_payload)

    def tearDown(self):
        random.shuffle = self.real_shuffle
        random.randint = self.real_randint
        abstract_thread.AbstractThread.time = self.real_time
        abstract_thread.AbstractThread.sleep = self.real_sleep


class MakeRequestsTest(unittest.TestCase):
    def setUp(self):
        self.real_shuffle = random.shuffle
        self.real_randint = random.randint
        self.real_time = abstract_thread.AbstractThread.time
        self.real_sleep = abstract_thread.AbstractThread.sleep
        self.tm = tm.ThreadManager(grinder_props, requests_by_type)
        req = MockReq()
        ingest.IngestThread.request = req
        ingestenum.EnumIngestThread.request = req
        annotationsingest.AnnotationsIngestThread.request = req
        for x in query.QueryThread.query_types:
            x.query_request = req
        random.shuffle = lambda x: None
        random.randint = lambda x, y: 0
        abstract_thread.AbstractThread.time = lambda x: 1000
        abstract_thread.AbstractThread.sleep = mock_sleep

        self.test_config = {'report_interval': (1000 * 6),
                            'num_tenants': 3,
                            'enum_num_tenants': 4,
                            'annotations_num_tenants': 3,
                            'metrics_per_tenant': 7,
                            'enum_metrics_per_tenant': 2,
                            'annotations_per_tenant': 2,
                            'batch_size': 3,
                            'ingest_concurrency': 2,
                            'enum_ingest_concurrency': 2,
                            'query_concurrency': 20,
                            'annotations_concurrency': 2,
                            'singleplot_per_interval': 11,
                            'multiplot_per_interval': 10,
                            'search_queries_per_interval': 9,
                            'enum_search_queries_per_interval': 9,
                            'enum_single_plot_queries_per_interval': 10,
                            'enum_multiplot_per_interval': 10,
                            'annotations_queries_per_interval': 8,
                            'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
                            'num_nodes': 2}

        ingest.default_config.update(self.test_config)

    def test_annotationsingest_make_request(self):
        global sleep_time
        agent_num = 0
        thread = annotationsingest.AnnotationsIngestThread(
            0, agent_num, MockReq())
        thread.slice = [[2, 0]]
        thread.position = 0
        thread.finish_time = 10000
        valid_payload = {
            "what": "annotation int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0",
            "when": 1000, "tags": "tag", "data": "data"}

        url, payload = thread.make_request(pp)
        # confirm request generates proper URL and payload
        self.assertEqual(
            url,
            'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/2/events')
        self.assertEqual(eval(payload), valid_payload)

        # confirm request increments position if not at end of report interval
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 10000)
        thread.position = 2
        thread.make_request(pp)

        # confirm request resets position at end of report interval
        self.assertEqual(sleep_time, 9000)
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 16000)

    def test_ingest_make_request(self):
        global sleep_time
        agent_num = 0
        thread = ingest.IngestThread(0, agent_num, MockReq())
        thread.slice = [[[2, 0], [2, 1]]]
        thread.position = 0
        thread.finish_time = 10000
        valid_payload = [
            {"collectionTime": 1000, "ttlInSeconds": 172800, "tenantId": "2",
             "metricValue": 0, "unit": "days",
             "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0"},
            {"collectionTime": 1000, "ttlInSeconds": 172800, "tenantId": "2",
             "metricValue": 0, "unit": "days",
             "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1"}]

        url, payload = thread.make_request(pp)
        # confirm request generates proper URL and payload
        self.assertEqual(url,
                         'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/tenantId/ingest/multi')
        self.assertEqual(eval(payload), valid_payload)

        # confirm request increments position if not at end of report interval
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 10000)
        thread.position = 2
        thread.make_request(pp)
        # confirm request resets position at end of report interval
        self.assertEqual(sleep_time, 9000)
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 16000)

    def test_ingest_enum_make_request(self):
        global sleep_time
        agent_num = 0
        thread = ingestenum.EnumIngestThread(0, agent_num, MockReq())
        thread.slice = [[[2, 0], [2, 1]]]
        thread.position = 0
        thread.finish_time = 10000
        valid_payload = [{'tenantId': '2', 'timestamp': 1000, 'enums': [
            {'value': 'e_g_0_0', 'name': generate_enum_metric_name(0)}]},
                         {'tenantId': '2', 'timestamp': 1000, 'enums': [
                             {'value': 'e_g_1_0',
                              'name': generate_enum_metric_name(1)}]}]

        url, payload = thread.make_request(pp)
        # confirm request generates proper URL and payload
        self.assertEqual(url,
                         'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/tenantId/ingest/aggregated/multi')
        self.assertEqual(eval(payload), valid_payload)

        # confirm request increments position if not at end of report interval
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 10000)
        thread.position = 2
        thread.make_request(pp)
        # confirm request resets position at end of report interval
        self.assertEqual(sleep_time, 9000)
        self.assertEqual(thread.position, 1)
        self.assertEqual(thread.finish_time, 16000)

    def test_query_make_request(self):
        agent_num = 0
        thread = query.QueryThread(0, agent_num, requests_by_type)
        thread.slice = [query.SinglePlotQuery, query.SearchQuery,
                        query.MultiPlotQuery, query.AnnotationsQuery,
                        query.EnumSearchQuery, query.EnumSinglePlotQuery,
                        query.EnumMultiPlotQuery]
        thread.position = 0
        thread.make_request(pp)
        self.assertEqual(get_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/0/views/int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0?from=-86399000&to=1000&resolution=FULL")

        random.randint = lambda x, y: 10
        thread.make_request(pp)
        self.assertEqual(get_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/10/metrics/search?query=int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.*")

        random.randint = lambda x, y: 20
        thread.make_request(pp)
        self.assertEqual(post_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/20/views?from=-86399000&to=1000&resolution=FULL")
        self.assertEqual(eval(post_payload), [
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.2",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.6",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.7",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.8",
            "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.9"])

        random.randint = lambda x, y: 30
        thread.make_request(pp)
        self.assertEqual(get_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/30/events/getEvents?from=-86399000&until=1000")

        random.randint = lambda x, y: 40
        thread.make_request(pp)
        self.assertEqual(get_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/40/metrics/search?query=enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.*&include_enum_values=true")

        random.randint = lambda x, y: 50
        thread.make_request(pp)
        self.assertEqual(get_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/50/views/enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.50?from=-86399000&to=1000&resolution=FULL")

        random.randint = lambda x, y: 4
        thread.make_request(pp)
        self.assertEqual(post_url,
                         "http://qe01.metrics.api.rackspacecloud.com/v2.0/4/views?from=-86399000&to=1000&resolution=FULL")
        self.assertEqual(eval(post_payload), [
            "enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0",
            "enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1",
            "enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.2",
            "enum_grinder_int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3"])

    def tearDown(self):
        random.shuffle = self.real_shuffle
        random.randint = self.real_randint
        abstract_thread.AbstractThread.time = self.real_time
        abstract_thread.AbstractThread.sleep = self.real_sleep


suite = unittest.TestSuite()
loader = unittest.TestLoader()
suite.addTest(loader.loadTestsFromTestCase(InitProcessTest))
suite.addTest(loader.loadTestsFromTestCase(GeneratePayloadTest))
suite.addTest(loader.loadTestsFromTestCase(MakeRequestsTest))
unittest.TextTestRunner().run(suite)


class TestRunner:
    def __init__(self):
        pass

    def __call__(self):
        pass
