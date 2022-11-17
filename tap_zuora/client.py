from typing import Dict, Tuple

import backoff
import requests
import singer
from singer import metrics

from tap_zuora.exceptions import (
    ApiException,
    BadCredentialsException,
    RateLimitException,
    RetryableException,
)
from tap_zuora.utils import make_aqua_payload

IS_AQUA = False
IS_REST = True
IS_PROD = False
IS_SAND = True
NOT_EURO = False
IS_EURO = True

URLS = {
    (IS_PROD, NOT_EURO): ["https://rest.na.zuora.com/", "https://rest.zuora.com/"],
    (IS_SAND, NOT_EURO): [
        "https://rest.sandbox.na.zuora.com/",
        "https://rest.apisandbox.zuora.com/",
    ],
    (IS_PROD, IS_EURO): ["https://rest.eu.zuora.com/"],
    (IS_SAND, IS_EURO): ["https://rest.sandbox.eu.zuora.com/"],
}

LATEST_WSDL_VERSION = "91.0"

LOGGER = singer.get_logger()


class Client:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        username: str,
        password: str,
        partner_id: str,
        sandbox: bool = False,
        european: bool = False,
        is_rest: bool = False,
    ):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.european = european
        self.partner_id = partner_id
        self.is_rest = is_rest
        self._session = requests.Session()

        self.base_url = self.get_url()

        adapter = requests.adapters.HTTPAdapter(max_retries=5)  # Try again in the case the TCP socket closes
        self._session.mount("https://", adapter)

    @staticmethod
    def from_config(config: Dict):
        sandbox = config.get("sandbox", False) == "true"
        european = config.get("european", False) == "true"
        partner_id = config.get("partner_id", None)
        is_rest = config.get("api_type") == "REST"
        return Client(
            config["username"],
            config["password"],
            partner_id,
            sandbox,
            european,
            is_rest,
        )

    def get_url(self) -> str:
        """gets the base_url from potential_urls based on configurations."""
        potential_urls = URLS[(self.sandbox, self.european)]
        stream_name = "Account"
        for url_prefix in potential_urls:
            if self.is_rest:
                resp = self._retryable_request(
                    "GET",
                    f"{url_prefix}v1/describe/{stream_name}",
                    url_check=True,
                    headers=self.rest_headers,
                )

            else:
                query = f"select * from {stream_name} limit 1"
                post_url = f"{url_prefix}v1/batch-query/"
                payload = make_aqua_payload("discover", query, self.partner_id)
                resp = self._retryable_request("POST", post_url, url_check=True, auth=self.aqua_auth, json=payload)
                if resp.status_code == 200:
                    resp_json = resp.json()
                    if "errorCode" in resp_json:
                        # Zuora sends 200 status code for an unrecognized partner ID in AQuA calls.
                        raise Exception(
                            resp_json.get(
                                "message",
                                "Partner ID is not recognized."
                                " To obtain a partner ID,"
                                " submit a request with Zuora Global Support",
                            )
                        )

                    delete_id = resp_json["id"]
                    delete_url = f"{url_prefix}v1/batch-query/jobs/{delete_id}"
                    self._retryable_request("DELETE", delete_url, auth=self.aqua_auth)
            if resp.status_code == 401:
                continue
            else:
                return url_prefix
        raise BadCredentialsException(
            f'Could not discover {"EU-based" if self.european else "US-based"} '
            f'{"REST" if self.is_rest else "AQuA"} '
            f'{"Sandbox" if self.sandbox else "Production"} '
            f"data center url out of {potential_urls} "
            f"for provided credentials."
        )

    @property
    def aqua_auth(self) -> Tuple:
        return self.username, self.password

    @property
    def rest_headers(self) -> Dict:
        """Returns headers for HTTP request."""
        return {
            "apiAccessKeyId": self.username,
            "apiSecretAccessKey": self.password,
            "X-Zuora-WSDL-Version": LATEST_WSDL_VERSION,
            "Content-Type": "application/json",
        }

    # NB> Backoff as recommended by Zuora here:
    # https://community.zuora.com/t5/Release-Notifications/Upcoming-Change-for-AQuA-and-Data-Source-Export-January-2021/ba-p/35024
    @backoff.on_exception(
        backoff.expo,
        (RateLimitException, RetryableException),
        max_tries=5,
        factor=30,
        jitter=None,
    )
    def _retryable_request(self, method: str, url: str, stream=False, url_check=False, **kwargs) -> requests.Response:
        """
        Performs HTTP request
        Retries the request for 5 times upon encountering exception
        Args:
            method (str): HTTP Method type
            url (str): API base_url + endpoint
        """
        req = requests.Request(method, url, **kwargs).prepare()
        resp = self._session.send(req, stream=stream)

        if resp.status_code == 429:
            raise RateLimitException(resp)
        # retries the request when response is either 500(Internal Server Error)
        # 502(Bad Gateway), 503(service unavailable), 504(Gateway Timeout)
        if resp.status_code in [500, 502, 503, 504]:
            raise RetryableException(resp)
        self.check_for_error(resp, url_check)
        return resp

    @staticmethod
    def check_for_error(resp, url_check):
        """
        Args:
            url_check [boolean]: checks if api call is for validating the domain url
            resp [requests.Response]: HTTP Response object
        """
        # If condition skip raising 400 exception when we test for stream availability
        # When some Stream is not available then api returns a 400 error with message noSuchDataSource
        if (
            not url_check
            and resp.status_code == 400
            and "noSuchDataSource" in resp.json().get("Errors", [{"Message": ""}])[0]["Message"]
        ):
            return

        if not url_check:
            resp.raise_for_status()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        LOGGER.info(f"{method}: {url}")
        resp = self._retryable_request(method, url, **kwargs)

        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def aqua_request(self, method: str, path: str, **kwargs) -> requests.Response:
        with metrics.http_request_timer(path):
            url = self.base_url + path
            return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method: str, path: str, **kwargs) -> requests.Response:
        with metrics.http_request_timer(path):
            url = self.base_url + path
            return self._request(method, url, headers=self.rest_headers, **kwargs)
