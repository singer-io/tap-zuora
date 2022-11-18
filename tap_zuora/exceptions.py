class RateLimitException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super().__init__(f"Rate Limit Exceeded (429) - {self.resp.content}")


class ApiException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super().__init__("{0.status_code}: {0.content}".format(self.resp))


class RetryableException(ApiException):
    """Class to mark an ApiException as retryable."""


class BadCredentialsException(Exception):
    """Exception for BadCredentials."""


class FileIdNotFoundException(Exception):
    """Handler for file id not found exception."""
