
class Request(object):

    def __init__(self, url, method=None, headers=None, body=None, extra=None):

        if method is None:
            method = 'GET'
        if headers is None:
            headers = []

        self.url = url
        self.method = method
        self.headers = headers
        self.body = body
        self.extra = extra
