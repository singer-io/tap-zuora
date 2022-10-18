import requests

class MockResponse:
    """
    Creates an HTTP mock response
    """

    def __init__(self, status_code, json, raise_error, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.content = content

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text


def get_response(status_code, json=None, raise_error=False, content=None):
    if json is None:
        json = {}
    return MockResponse(status_code, json, raise_error, content)