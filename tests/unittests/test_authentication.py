import unittest
import requests
from unittest.mock import patch, Mock
from tap_zuora.client import get_access_token, BadCredentialsException, Client


class TestGetAccessToken(unittest.TestCase):

    @patch("tap_zuora.client.requests.post")
    def test_get_access_token_success_on_first_attempt(self, mock_post):
        """
        Test to ensure that the access token is returned successfully on the first attempt
        """
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"access_token": "test_token"}
        mock_post.return_value = mock_response

        client_id = "test_client_id"
        client_secret = "test_client_secret"
        base_urls = ["https://api.example.com/"]

        token = get_access_token(client_id, client_secret, base_urls)
        self.assertEqual(token, "test_token")
        self.assertEqual(mock_post.call_count, 1)
        mock_post.assert_called_once_with(
            "https://api.example.com/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"},
        )

    @patch("tap_zuora.client.requests.post")
    def test_get_access_token_success_on_second_attempt(self, mock_post):
        """
        Test to ensure that the access token is returned successfully on the second attempt
        """
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"access_token": "test_token"}
        mock_post.side_effect = [requests.exceptions.HTTPError("Error"), mock_response]

        client_id = "test_client_id"
        client_secret = "test_client_secret"
        base_urls = ["https://api.example_1.com/", "https://api.example_2.com/"]

        token = get_access_token(client_id, client_secret, base_urls)
        self.assertEqual(token, "test_token")
        self.assertEqual(mock_post.call_count, len(base_urls))
        mock_post.assert_called_with(
            "https://api.example_2.com/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"},
        )

    @patch("tap_zuora.client.requests.post")
    def test_get_access_token_failure(self, mock_post):
        """
        Test to ensure that an exception is raised when the access token cannot be retrieved
        """
        mock_post.side_effect = [requests.exceptions.HTTPError("Error"), requests.exceptions.HTTPError("Error")]

        client_id = "test_client_id"
        client_secret = "test_client_secret"
        base_urls = ["https://api.example.com/"]

        with self.assertRaises(BadCredentialsException):
            get_access_token(client_id, client_secret, base_urls)
        self.assertEqual(mock_post.call_count, len(base_urls))

class TestDiffAuthTypeValues(unittest.TestCase):

    @patch("tap_zuora.client.get_access_token")
    @patch("tap_zuora.client.Client.get_url")
    def test_existing_connection_without_auth_type(self, mock_get_url, mock_get_access_token):
        """
        Test to ensure that an auth_type is set to Basic when auth_type is not provided
        """
        config = {
            "username": "test_user",
            "password": "test_pass",
        }
        client = Client.from_config(config)
        self.assertEqual(client.auth_type, "Basic")
        self.assertEqual(client.access_token, None)

    def test_existing_connection_with_auth_type_None(self):
        """
        Test to ensure that an exception is raised when auth_type is set to None
        """
        config = {
            "username": "test_user",
            "password": "test_pass",
            "auth_type": None,
        }
        with self.assertRaises(BadCredentialsException) as e:
            client = Client.from_config(config)
            self.assertEqual(str(e), "auth_type must not be set to an empty string or None")

    def test_existing_connection_with_auth_type_empty_string(self):
        """
        Test to ensure that an exception is raised when auth_type is set to an empty string
        """
        config = {
            "username": "test_user",
            "password": "test_pass",
            "auth_type": "",
        }
        with self.assertRaises(BadCredentialsException) as e:
            client = Client.from_config(config)
            self.assertEqual(str(e), "auth_type must not be set to an empty string or None")

    @patch("tap_zuora.client.get_access_token")
    @patch("tap_zuora.client.Client.get_url")
    def test_existing_connection_with_auth_type_basi(self, mock_get_url, mock_get_access_token):
        """
        Test to ensure that an auth_type is set to Basic when auth_type is set to Basic in the config
        """
        config = {
            "username": "test_user",
            "password": "test_pass",
            "auth_type": "Basic",
        }
        client = Client.from_config(config)
        self.assertEqual(client.auth_type, "Basic")
        self.assertEqual(client.access_token, None)

    @patch("tap_zuora.client.get_access_token", return_value="dummy_token")
    @patch("tap_zuora.client.Client.get_url")
    def test_existing_connection_with_auth_type_oauth(self, mock_get_url, mock_get_access_token):
        """
        Test to ensure that an auth_type is set to OAuth when auth_type is set to OAuth in the config
        """
        config = {
            "username": "test_user",
            "password": "test_pass",
            "auth_type": "OAuth",
        }
        client = Client.from_config(config)
        self.assertEqual(client.auth_type, "OAuth")
        self.assertEqual(client.access_token, "dummy_token")