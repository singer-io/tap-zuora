import unittest
from unittest import mock

import requests
from utils import get_response

import tap_zuora
from tap_zuora.client import Client
from tap_zuora.exceptions import (
    BadCredentialsException,
    RateLimitException,
    RetryableException,
)


class MockConfigAqua:
    """Mocks config params for AQuA API calls."""

    config = {
        "start_date": "",
        "username": "",
        "password": "",
        "api_type": "AQUA",
        "partner_id": "salesforce",
    }


class MockConfigRest:
    """Mocks config params for REST API calls."""

    config = {"start_date": "", "username": "", "password": "", "api_type": "REST"}


@mock.patch("requests.Session.send")
@mock.patch("requests.Request")
@mock.patch("time.sleep")
class TestHttpExceptionErrors(unittest.TestCase):
    def test_http_429_error(self, mock_time, mock_http_request, mock_http_send):
        """Test if API request gets retried for 5 times after encountering
        ratelimit exception."""
        client_object = Client.from_config(MockConfigAqua.config)
        mock_http_request.return_value = requests.Request()
        mock_http_send.return_value = get_response(429)
        # Set the call_count values to zero since .from_config method calls it for base_url
        mock_http_request.call_count = 0
        mock_http_send.call_count = 0
        with self.assertRaises(RateLimitException):
            client_object._request("GET", "")
        # Assert the number of retries to 5
        self.assertEqual(mock_http_send.call_count, 5)
        self.assertEqual(mock_http_request.call_count, 5)

    def test_http_5xx_error(self, mock_time, mock_http_request, mock_http_send):
        """Test if API request gets retried for 5 times after encountering 500,
        502, 503, 504 exceptions."""
        client_object = Client.from_config(MockConfigAqua.config)
        mock_http_request.return_value = requests.Request()
        for error_code in [500, 502, 503, 504]:
            mock_http_send.return_value = get_response(error_code)
            # Set the call_count values to zero since .from_config method calls it for base_url
            mock_http_request.call_count = 0
            mock_http_send.call_count = 0
            with self.assertRaises(RetryableException):
                client_object._request("GET", "")
            # Assert the number of retries to 5
            self.assertEqual(mock_http_send.call_count, 5)
            self.assertEqual(mock_http_request.call_count, 5)


@mock.patch("singer.utils.parse_args")
class TestGetUrlScenarios(unittest.TestCase):
    def test_bad_credentials_aqua(self, mock_args):
        """Test for BadCredentialsException for incorrect credentials for AQuA
        API calls."""
        mock_args.return_value = MockConfigAqua
        with self.assertRaises(BadCredentialsException):
            tap_zuora.main()

    def test_bad_credentials_rest(self, mock_args):
        """Test for BadCredentialsException for incorrect credentials for REST
        API calls."""
        mock_args.return_value = MockConfigRest
        with self.assertRaises(BadCredentialsException):
            tap_zuora.main()
