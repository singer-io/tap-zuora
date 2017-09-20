import requests

import singer

# The keys are hashes of (sandbox, european) booleans
URLS = {
    (False, False): "https://www.zuora.com/",
    (True, False): "https://apisandbox.zuora.com/",
    (False, True): "https://rest.sandbox.eu.zuora.com/",
    (True, True): "https://rest.eu.zuora.com/",
}

LATEST_WSDL_VERSION = "87.0"

LOGGER = singer.get_logger()


class ApiException(Exception):
    def __init__(self, resp):
        super(ApiException, self).__init__("Bad API response {0.status_code}: {0.content}".format(resp))


class Client:
    def __init__(self, username, password, sandbox=False, european=False):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.european = european
        self._session = requests.Session()

    @property
    def base_url(self):
        return URLS[(self.sandbox, self.european)]

    @property
    def aqua_auth(self):
        return (self.username, self.password)

    @property
    def rest_headers(self):
        return {
            'apiAccessKeyId': self.username,
            'apiSecretAccessKey': self.password,
            'x-zuora-wsdl-version': LATEST_WSDL_VERSION,
            'Content-Type': 'application/json',
        }

    def _request(self, method, url, stream=False, **kwargs):
        url = self.base_url + url
        req = requests.Request(method, url, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        resp = self._session.send(req, stream=stream)
        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def aqua_request(self, method, url, **kwargs):
        return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method, url, **kwargs):
        return self._request(method, url, headers=self.rest_headers, **kwargs)
