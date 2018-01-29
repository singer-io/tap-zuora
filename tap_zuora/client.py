import requests

import singer


IS_AQUA = False
IS_REST = True
IS_PROD = False
IS_SAND = True
NOT_EURO = False
IS_EURO = True

URLS = {
    (IS_AQUA, IS_PROD, NOT_EURO): "https://www.zuora.com/",
    (IS_AQUA, IS_SAND, NOT_EURO): "https://apisandbox.zuora.com/",
    (IS_AQUA, IS_PROD, IS_EURO ): "https://rest.eu.zuora.com/",
    (IS_AQUA, IS_SAND, IS_EURO ): "https://rest.sandbox.eu.zuora.com/",
    (IS_REST, IS_PROD, NOT_EURO): "https://rest.zuora.com/",
    (IS_REST, IS_SAND, NOT_EURO): "https://rest.apisandbox.zuora.com/",
    (IS_REST, IS_PROD, IS_EURO ): "https://rest.eu.zuora.com/",
    (IS_REST, IS_SAND, IS_EURO ): "https://rest.sandbox.eu.zuora.com/",
}

LATEST_WSDL_VERSION = "87.0"

LOGGER = singer.get_logger()


class ApiException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super(ApiException, self).__init__("{0.status_code}: {0.content}".format(self.resp))


class Client:
    def __init__(self, username, password, partner_id, sandbox=False, european=False):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.european = european
        self.partner_id = partner_id
        self._session = requests.Session()

    @staticmethod
    def from_config(config):
        sandbox = config.get('sandbox', False) == 'true'
        european = config.get('european', False) == 'true'
        partner_id = config.get('partner_id', None)
        return Client(config['username'], config['password'], partner_id, sandbox, european)

    def get_url(self, url, rest=False):
        return URLS[(rest, self.sandbox, self.european)] + url

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
        req = requests.Request(method, url, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        resp = self._session.send(req, stream=stream)
        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def aqua_request(self, method, url, **kwargs):
        url = self.get_url(url, rest=False)
        return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method, url, **kwargs):
        url = self.get_url(url, rest=True)
        return self._request(method, url, headers=self.rest_headers, **kwargs)
