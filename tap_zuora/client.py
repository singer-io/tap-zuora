import requests
import pendulum
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

LATEST_WSDL_VERSION = "91.0"

LOGGER = singer.get_logger()


class ApiException(Exception):
    def __init__(self, resp):
        self.resp = resp
        super(ApiException, self).__init__("{0.status_code}: {0.content}".format(self.resp))


class Client:
    def __init__(self, username, password, partner_id, sandbox=False, european=False, use_oauth2=False):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.european = european
        self.partner_id = partner_id
        self._session = requests.Session()
        self.oauth2_token = None
        self.use_oauth2 = use_oauth2
        self.token_expiration_date = None

        adapter = requests.adapters.HTTPAdapter(max_retries=1) # Try again in the case the TCP socket closes
        self._session.mount('https://', adapter)

    @staticmethod
    def from_config(config):
        sandbox = config.get('sandbox', False) == 'true'
        european = config.get('european', False) == 'true'
        partner_id = config.get('partner_id', None)
        use_oauth2 = config.get('use_oauth2', False) == 'true'
        return Client(config['username'], config['password'], partner_id, sandbox, european, use_oauth2)

    def get_url(self, url, rest=False):
        return URLS[(rest, self.sandbox, self.european)] + url

    @property
    def aqua_auth(self):
        return (self.username, self.password)

    @property
    def rest_headers(self):
        if self.use_oauth2:
            return {
                'Authorization': 'Bearer ' + self.oauth2_token['access_token'],
                'Content-Type': 'application/json',
        }
        else:
            return {
                'apiAccessKeyId': self.username,
                'apiSecretAccessKey': self.password,
                'X-Zuora-WSDL-Version': LATEST_WSDL_VERSION,
                'Content-Type': 'application/json',
            }

    def _request(self, method, url, stream=False, **kwargs):
        req = requests.Request(method, url, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        resp = self._session.send(req, stream=stream)
        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def is_auth_token_valid(self):
        if self.oauth2_token and self.token_expiration_date and pendulum.now().utcnow().diff(self.token_expiration_date).in_seconds() > 60: # Allows at least one minute of breathing room
            return True
        
        return False

    def ensure_valid_auth_token(self):
        if not self.is_auth_token_valid():
            self.oauth2_token = self.request_token()

    def request_token(self):
        url = self.get_url('oauth/token', rest=True)
        payload = {
            'client_id': self.username,
            'client_secret': self.password,
            'grant_type': 'client_credentials',
        }

        token = self._request('POST', url, data=payload).json()
        self.token_expiration_date = pendulum.now().utcnow().add(seconds=token['expires_in'])

        return token

    def aqua_request(self, method, url, **kwargs):
        if self.use_oauth2:
            self.ensure_valid_auth_token()

        url = self.get_url(url, rest=False)

        if self.use_oauth2:
            return self._request(method, url, headers=self.rest_headers, **kwargs)
        else:
            return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method, url, **kwargs):
        if self.use_oauth2:
            self.ensure_valid_auth_token()

        url = self.get_url(url, rest=True)
        return self._request(method, url, headers=self.rest_headers, **kwargs)
