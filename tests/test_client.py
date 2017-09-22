import unittest
import unittest.mock

import requests_mock

from tap_zuora import client


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = client.Client("testuser", "testpass")

    def test_get_url(self):
        # US prod AQUA
        self.assertEqual("https://www.zuora.com/test", self.client.get_url("test"))

        # US prod REST
        self.assertEqual("https://rest.zuora.com/test", self.client.get_url("test", rest=True))

        self.client.sandbox = True
        # US sandbox AQUA
        self.assertEqual("https://apisandbox.zuora.com/test", self.client.get_url("test"))

        # US sandbox REST
        self.assertEqual("https://rest.apisandbox.zuora.com/test", self.client.get_url("test", rest=True))

        self.client.sandbox = False
        self.client.european = True
        # EU prod AQUA
        self.assertEqual("https://rest.eu.zuora.com/test", self.client.get_url("test"))

        # EU prod REST
        self.assertEqual("https://rest.eu.zuora.com/test", self.client.get_url("test", rest=True))

        self.client.sandbox = True
        # EU sandbox AQUA
        self.assertEqual("https://rest.sandbox.eu.zuora.com/test", self.client.get_url("test"))

        # EU sandbox REST
        self.assertEqual("https://rest.sandbox.eu.zuora.com/test", self.client.get_url("test", rest=True))

    def test_aqua_auth(self):
        self.assertEqual(("testuser", "testpass"), self.client.aqua_auth)

    def test_rest_headers(self):
        self.assertDictEqual({
            "apiAccessKeyId": "testuser",
            "apiSecretAccessKey": "testpass",
            "x-zuora-wsdl-version": "87.0",
            "Content-Type": "application/json",
        }, self.client.rest_headers)

    def test_request(self):
        with requests_mock.mock() as mock:
            mock.get("https://test.com", text="winnar")
            resp = self.client._request("GET", "https://test.com")
            self.assertEqual(200, resp.status_code)
            self.assertEqual(b"winnar", resp.content)

    def test_request_non_200(self):
        with requests_mock.mock() as mock:
            mock.get("https://test.com", status_code=500, text="losar")
            with self.assertRaises(client.ApiException, msg="Bad API response 500: b'losar'"):
                self.client._request("GET", "https://test.com")

    def test_aqua_request(self):
        self.client._request = unittest.mock.MagicMock()
        self.client.aqua_request("GET", "test", stream=True)
        self.client._request.assert_called_once_with(
            "GET",
            "https://www.zuora.com/test",
            auth=("testuser", "testpass"),
            stream=True)

    def test_rest_request(self):
        self.client._request = unittest.mock.MagicMock()
        self.client.rest_request("GET", "test", stream=True)
        self.client._request.assert_called_once_with(
            "GET",
            "https://rest.zuora.com/test",
            headers={
                "apiAccessKeyId": "testuser",
                "apiSecretAccessKey": "testpass",
                "x-zuora-wsdl-version": "87.0",
                "Content-Type": "application/json",
            },
            stream=True)
