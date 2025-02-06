class RateLimitException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super().__init__(f"Rate Limit Exceeded (429) - {self.resp.content}")


class ApiException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super().__init__(f"{resp.status_code}: {resp.content}")

class InvalidValueException(Exception):
    def __init__(self, resp, stream_name):
        self.resp = resp
        self.stream_name = stream_name
        invalid_value_errors = [e for e in resp.json()['Errors'] if e["Code"] == "INVALID_VALUE"]
        super().__init__(f"{stream_name} - Invalid Values in Request ({resp.status_code}), Errors: {invalid_value_errors}")

class RetryableException(ApiException):
    """Class to mark an ApiException as retryable."""


class BadCredentialsException(Exception):
    """Exception for BadCredentials."""


class FileIdNotFoundException(Exception):
    """Handler for file id not found exception."""
