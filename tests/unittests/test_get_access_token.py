import unittest
import requests
from unittest.mock import patch, Mock
from tap_zuora.client import get_access_token, BadCredentialsException


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
