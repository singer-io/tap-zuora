class RateLimitException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super(RateLimitException, self).__init__("Rate Limit Exceeded (429) - {}".format(self.resp.content))

class ApiException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super(ApiException, self).__init__("{0.status_code}: {0.content}".format(self.resp))

class RetryableException(ApiException):
    """Class to mark an ApiException as retryable."""
