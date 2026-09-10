import json
import pathlib
import unittest
from unittest import mock

import requests
from utils import get_response

from tap_zuora.apis import (
    Aqua,
    InvalidFieldExportFailed,
    Rest,
    parse_invalid_fields,
    parse_invalid_fields_from_response,
)
from tap_zuora.client import Client

p = pathlib.Path(__file__).with_name("sample_stream_metadata.json")
with p.open("r") as f:
    STREAM_METADATA = json.load(f)


class TestAquaApis(unittest.TestCase):
    def test_get_query(self):
        """Test to ensure we get correct SQL query based on stream metadata for
        AQuA calls."""
        self.assertEqual(
            Aqua.get_query(STREAM_METADATA),
            "select Field1, UpdatedDate, Id from Stream1 order by UpdatedDate asc",
        )

    def test_get_payload(self):
        """Test to ensure that we get correct payload based on stream_metadata
        and State file."""
        state_file = {"bookmarks": {"Stream1": {"version": 123456, "UpdatedDate": "2022-10-01T00:00:00Z"}}}
        expected_payload = {
            "name": "Stream1_123456",
            "partner": "partner_id",
            "project": "Stream1_123456",
            "format": "csv",
            "version": "1.2",
            "encrypted": "none",
            "useQueryLabels": "true",
            "dateTimeUtc": "true",
            "queries": [
                {
                    "name": "Stream1_123456",
                    "query": "select Field1, UpdatedDate, Id from Stream1 " "order by UpdatedDate asc",
                    "type": "zoqlexport",
                }
            ],
            "incrementalTime": "2022-09-30 17:00:00",
        }
        self.assertEqual(
            Aqua.get_payload(state_file, STREAM_METADATA, "partner_id"),
            expected_payload,
        )


class TestRestApis(unittest.TestCase):
    def test_get_query(self):
        """Test to ensure we get correct SQL query based on stream metadata for
        REST calls."""
        self.assertEqual(
            Rest.get_query(STREAM_METADATA, "2022-10-01", "2022-10-17"),
            "select Field1, UpdatedDate, Id from Stream1 where "
            "UpdatedDate >= '2022-10-01T00:00:00Z' and UpdatedDate < '2022-10-17T00:00:00Z'",
        )

    def test_get_payload(self):
        """Test to ensure that we get correct payload based on stream_metadata
        and State file for REST calls."""
        expected_payload = {
            "Format": "csv",
            "Query": "select Field1, UpdatedDate, Id from Stream1 "
            "where UpdatedDate >= '2022-10-01T00:00:00Z' and UpdatedDate < '2022-10-17T00:00:00Z'",
        }

        self.assertEqual(
            Rest.get_payload(STREAM_METADATA, "2022-10-01", "2022-10-17"),
            expected_payload,
        )

    @mock.patch("tap_zuora.client.Client.rest_request")
    @mock.patch("requests.Session.send")
    @mock.patch("requests.Request")
    def test_job_ready_raises_invalid_field_export_failed(self, mock_request, mock_send, mock_rest_request):
        """Test to ensure Rest.job_ready raises InvalidFieldExportFailed when
        Zuora rejects a field that doesn't exist on this tenant."""
        mock_request.return_value = requests.Request()
        mock_send.return_value = get_response(200, json={"id": 1234})
        client_object = Client.from_config({"username": "", "password": ""})
        mock_rest_request.return_value = get_response(
            200,
            json={
                "Status": "Failed",
                "StatusReason": "OrderLineItem - Invalid Values in Request (400), Errors: "
                "[{'Message': 'There is no field named RevenueAmortizationMethod.', "
                "'Code': 'INVALID_VALUE'}]",
            },
        )
        with self.assertRaises(InvalidFieldExportFailed) as ctx:
            Rest.job_ready(client_object, "job123")
        self.assertEqual(ctx.exception.invalid_fields, ["RevenueAmortizationMethod"])


class TestInvalidFieldParsing(unittest.TestCase):
    def test_parse_invalid_fields(self):
        """Test to ensure we correctly extract the invalid field name from a
        Zuora error message."""
        message = "There is no field named RevenueAmortizationMethod."
        self.assertEqual(parse_invalid_fields(message), ["RevenueAmortizationMethod"])

    def test_parse_invalid_fields_no_match(self):
        """Test to ensure no fields are returned when the message doesn't
        match the known invalid field pattern."""
        self.assertEqual(parse_invalid_fields("Some other error"), [])

    def test_parse_invalid_fields_from_response(self):
        """Test to ensure we extract invalid field names from a Zuora REST
        400 Errors response body."""
        resp = get_response(
            400,
            json={
                "Errors": [
                    {"Message": "There is no field named RevenueAmortizationMethod.", "Code": "INVALID_VALUE"}
                ]
            },
        )
        self.assertEqual(parse_invalid_fields_from_response(resp), ["RevenueAmortizationMethod"])

    def test_parse_invalid_fields_from_response_ignores_other_errors(self):
        """Test to ensure non-INVALID_VALUE errors don't contribute field
        names."""
        resp = get_response(
            400,
            json={"Errors": [{"Message": "Some unrelated failure.", "Code": "OTHER"}]},
        )
        self.assertEqual(parse_invalid_fields_from_response(resp), [])

    @mock.patch("tap_zuora.client.Client.aqua_request")
    @mock.patch("requests.Session.send")
    @mock.patch("requests.Request")
    def test_aqua_job_ready_raises_invalid_field_export_failed(self, mock_request, mock_send, mock_aqua_request):
        """Test to ensure Aqua.job_ready raises InvalidFieldExportFailed when
        Zuora rejects a field that doesn't exist on this tenant."""
        mock_request.return_value = requests.Request()
        mock_send.return_value = get_response(200, json={"id": 1234})
        client_object = Client.from_config({"username": "", "password": ""})
        mock_aqua_request.return_value = get_response(
            200,
            json={
                "status": "failed",
                "batches": [
                    {
                        "message": "OrderLineItem - Invalid Values in Request (400), Errors: "
                        "[{'Message': 'There is no field named RevenueAmortizationMethod.', "
                        "'Code': 'INVALID_VALUE'}]"
                    }
                ],
            },
        )
        with self.assertRaises(InvalidFieldExportFailed) as ctx:
            Aqua.job_ready(client_object, "job123")
        self.assertEqual(ctx.exception.invalid_fields, ["RevenueAmortizationMethod"])
