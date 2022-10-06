import backoff
import requests
import singer
from singer import metrics
from tap_zuora.apis import Aqua
from tap_zuora.exceptions import RateLimitException, ApiException, RetryableException
from typing import Dict, Tuple

IS_AQUA = False
IS_REST = True
IS_PROD = False
IS_SAND = True
NOT_EURO = False
IS_EURO = True

URLS = {
    (IS_PROD, NOT_EURO): ["https://rest.na.zuora.com/", "https://rest.zuora.com/"],
    (IS_SAND, NOT_EURO): ["https://rest.sandbox.na.zuora.com/", "https://rest.apisandbox.zuora.com/"],
    (IS_PROD, IS_EURO): ["https://rest.eu.zuora.com/"],
    (IS_SAND, IS_EURO): ["https://rest.sandbox.eu.zuora.com/"],
}


LATEST_WSDL_VERSION = "91.0"

LOGGER = singer.get_logger()


class Client:  # pylint: disable=too-many-instance-attributes
    def __init__(self, username: str, password: str, partner_id: str, sandbox: bool = False, european: bool = False):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.european = european
        self.partner_id = partner_id
        self._session = requests.Session()

        self.rest_url = self.get_url(rest=True)
        self.aqua_url = self.get_url(rest=False)

        adapter = requests.adapters.HTTPAdapter(max_retries=1)  # Try again in the case the TCP socket closes
        self._session.mount("https://", adapter)

    @staticmethod
    def from_config(config: Dict):
        sandbox = config.get("sandbox", False) == "true"
        european = config.get("european", False) == "true"
        partner_id = config.get("partner_id", None)
        return Client(config["username"], config["password"], partner_id, sandbox, european)

    def get_url(self, rest: bool) -> str:
        potential_urls = URLS[(self.sandbox, self.european)]
        stream_name = "Account"
        for url_prefix in potential_urls:
            if rest:
                resp = self._retryable_request("GET", f"{url_prefix}v1/describe/{stream_name}",
                                               headers=self.rest_headers)

            else:
                query = f"select * from {stream_name} limit 1"
                post_url = f"{url_prefix}v1/batch-query/"
                payload = Aqua.make_payload("discover", query, self.partner_id)
                resp = self._retryable_request("POST", post_url, auth=self.aqua_auth, json=payload)
                if resp.status_code == 200:
                    resp_json = resp.json()
                    delete_id = resp_json["id"]
                    delete_url = f"{url_prefix}v1/batch-query/jobs/{delete_id}"
                    self._retryable_request("DELETE", delete_url, auth=self.aqua_auth)
            if resp.status_code == 401:
                continue
            resp.raise_for_status()
            return url_prefix
        raise Exception(f'Could not discover {"EU-based" if self.european else "US-based"} '
                        f'{"REST" if rest else "AQuA"} '
                        f'{"Sandbox" if self.sandbox else "Production"} '
                        f'data center url out of {potential_urls}')

    @property
    def aqua_auth(self) -> Tuple:
        return self.username, self.password

    @property
    def rest_headers(self) -> Dict:
        return {
            "apiAccessKeyId": self.username,
            "apiSecretAccessKey": self.password,
            "X-Zuora-WSDL-Version": LATEST_WSDL_VERSION,
            "Content-Type": "application/json",
        }

    # NB> Backoff as recommended by Zuora here:
    # https://community.zuora.com/t5/Release-Notifications/Upcoming-Change-for-AQuA-and-Data-Source-Export-January-2021/ba-p/35024
    @backoff.on_exception(backoff.expo,
                          (RateLimitException, RetryableException),
                          max_time=5 * 60,  # in seconds
                          factor=30,
                          jitter=None)
    def _retryable_request(self, method: str, url: str, stream=False, **kwargs) -> requests.Response:
        req = requests.Request(method, url, **kwargs).prepare()
        resp = self._session.send(req, stream=stream)

        if resp.status_code == 429:
            raise RateLimitException(resp)
        if resp.status_code == 500:
            raise RetryableException(resp)
        return resp

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        LOGGER.info(f"{method}: {url}")
        resp = self._retryable_request(method, url, **kwargs)

        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def aqua_request(self, method: str, path: str, **kwargs) -> requests.Response:
        with metrics.http_request_timer(path):
            url = self.aqua_url+path
            return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method: str, path: str, **kwargs) -> requests.Response:
        with metrics.http_request_timer(path):
            url = self.rest_url+path
            return self._request(method, url, headers=self.rest_headers, **kwargs)
