import json
import pathlib
import unittest

from tap_zuora.apis import Aqua, Rest

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
