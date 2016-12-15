
import random
import threading

try:
    from com.xhaus.jyson import JysonCodec as json
except ImportError:
    import json
from abstract_thread import AbstractThread, generate_metric_name
from throttling_group import NullThrottlingGroup

try:
    from HTTPClient import NVPair
except ImportError:
    from nvpair import NVPair


class AnnotationsIngestThread(AbstractThread):

    def generate_annotation(self, time, metric_id):
        metric_name = generate_metric_name(metric_id, self.config)
        return {'what': 'annotation ' + metric_name,
                'when': time,
                'tags': 'tag',
                'data': 'data'}

    def generate_payload(self, time, metric_id):
        payload = self.generate_annotation(time, metric_id)
        return json.dumps(payload)

    def ingest_url(self, tenant_id=None):
        if tenant_id is None:
            tenant_id = self.user.get_tenant_id()
        return "%s/v2.0/%s/events" % (self.config['url'], tenant_id)

    def make_request(self, logger, time, tenant_and_metric=None):

        if tenant_and_metric is None:
            tenant_id = self.user.get_tenant_id()
            metric_id = random.randint(
                1, self.config['annotations_per_tenant'])
            tenant_and_metric = [tenant_id, metric_id]
            url = self.ingest_url()
        else:
            tenant_id, metric_id = tenant_and_metric
            url = self.ingest_url(tenant_id)

        payload = self.generate_payload(time, metric_id)

        headers = ( NVPair("Content-Type", "application/json"), )
        result = self.request.POST(url, payload, headers)
        if result.getStatusCode() >= 400:
            logger("Error: status code=" + str(result.getStatusCode()) +
                   " req url=" + url +
                   " req headers=" + str(headers) +
                   " req payload=" + payload +
                   " resp payload=" + result.getText())
        if 200 <= result.getStatusCode() < 300:
            self._record_metrics_sent(1)
        return result

    _count = 0
    _lock = threading.RLock()

    def _record_metrics_sent(self, delta):
        def record_metrics_sent_sync():
            self._count += delta
            f = open('annotation_count.txt', 'w')
            try:
                f.write(str(self._count))
                f.write('\n')
            finally:
                f.close()

        self._lock.acquire()
        try:
            record_metrics_sent_sync()
        finally:
            self._lock.release()
