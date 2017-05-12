import random

try:
    from com.xhaus.jyson import JysonCodec as json
except ImportError:
    import json
from abstract_generator import AbstractGenerator, generate_metric_name
from request import Request

try:
    from HTTPClient import NVPair
except ImportError:
    from nvpair import NVPair


class AbstractQueryGenerator(AbstractGenerator):
    one_day = (1000 * 60 * 60 * 24)

    query_interval_name = None

    def __init__(self, thread_num, agent_number, request, config, user=None):
        AbstractGenerator.__init__(self, thread_num, agent_number, request,
                                   config, user)
        self.thread_num = thread_num
        self.config = config
        self.request = request


class SinglePlotQueryGenerator(AbstractQueryGenerator):
    query_interval_name = 'singleplot_query_weight'

    def generate_request(self, logger, time, tenant_id=None, metric_name=None):
        if tenant_id is None:
            tenant_id = self.user.get_tenant_id()
        if metric_name is None:
            metric_name = generate_metric_name(
                random.randint(0, self.config['ingest_metrics_per_tenant']),
                self.config)
        to = time
        frm = time - self.one_day
        resolution = 'MIN5'
        url = "%s/v2.0/%s/views/%s?from=%d&to=%s&resolution=%s" % (
            self.config['query_url'],
            tenant_id, metric_name, frm,
            to, resolution)
        return Request(url, 'GET', extra=(tenant_id, metric_name))

    def send_request(self, request):
        return self.request.GET(request.url)

    def after_request_sent(self, request, response, logger):
        pass

    def make_request(self, logger, time, tenant_id=None,
                     metric_name=None):

        request = self.generate_request(logger, time, tenant_id, metric_name)
        response = self.send_request(request)
        self.after_request_sent(request, response, logger)
        return response


class MultiPlotQueryGenerator(AbstractQueryGenerator):
    query_interval_name = 'multiplot_query_weight'

    def generate_multiplot_payload(self):
        metrics_count = min(self.config['max_multiplot_metrics'],
                            random.randint(0, self.config[
                                'ingest_metrics_per_tenant']))
        metrics_list = [
            generate_metric_name(i, self.config) for i in range(metrics_count)
        ]
        return json.dumps(metrics_list)

    def generate_request(self, logger, time, tenant_id=None, payload=None):
        if tenant_id is None:
            tenant_id = self.user.get_tenant_id()
        if payload is None:
            payload = self.generate_multiplot_payload()
        to = time
        frm = time - self.one_day
        resolution = 'MIN5'
        url = "%s/v2.0/%s/views?from=%d&to=%d&resolution=%s" % (
            self.config['query_url'],
            tenant_id, frm,
            to, resolution)
        headers = ( NVPair("Content-Type", "application/json"), )
        return Request(url, 'POST', headers, payload, extra=tenant_id)

    def send_request(self, request):
        return self.request.POST(request.url, request.body, request.headers)

    def after_request_sent(self, request, response, logger):
        pass

    def make_request(self, logger, time, tenant_id=None, payload=None):
        request = self.generate_request(logger, time, tenant_id, payload)
        response = self.send_request(request)
        self.after_request_sent(request, response, logger)
        return response


class SearchQueryGenerator(AbstractQueryGenerator):
    query_interval_name = 'search_query_weight'

    def generate_metrics_regex(self):
        metric_name = generate_metric_name(
            random.randint(0, self.config['ingest_metrics_per_tenant']),
            self.config)
        return ".".join(metric_name.split('.')[0:-1]) + ".*"

    def generate_request(self, logger, time, tenant_id=None,
                         metric_regex=None):
        if tenant_id is None:
            tenant_id = self.user.get_tenant_id()
        if metric_regex is None:
            metric_regex = self.generate_metrics_regex()
        url = "%s/v2.0/%s/metrics/search?query=%s" % (
            self.config['query_url'],
            tenant_id, metric_regex)
        return Request(url, 'GET', extra=(tenant_id,metric_regex))

    def send_request(self, request):
        return self.request.GET(request.url)

    def after_request_sent(self, request, response, logger):
        pass

    def make_request(self, logger, time, tenant_id=None,
                     metric_regex=None):
        request = self.generate_request(logger, time, tenant_id, metric_regex)
        response = self.send_request(request)
        self.after_request_sent(request, response, logger)
        return response


class AnnotationsQueryGenerator(AbstractQueryGenerator):
    query_interval_name = 'annotations_query_weight'

    def generate_request(self, logger, time, tenant_id=None):
        if tenant_id is None:
            tenant_id = self.user.get_tenant_id()
        to = time
        frm = time - self.one_day
        url = "%s/v2.0/%s/events/getEvents?from=%d&until=%d" % (
            self.config['query_url'], tenant_id, frm, to)
        return Request(url, 'GET', extra=(tenant_id,))

    def send_request(self, request):
        return self.request.GET(request.url)

    def after_request_sent(self, request, response, logger):
        pass

    def make_request(self, logger, time, tenant_id=None):
        request = self.generate_request(logger, time, tenant_id)
        response = self.send_request(request)
        self.after_request_sent(request, response, logger)
        return response

