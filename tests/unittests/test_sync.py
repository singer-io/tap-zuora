import copy
import json
import pathlib
import unittest
from unittest import mock

from tap_zuora import apis, sync

p = pathlib.Path(__file__).with_name("sample_stream_metadata.json")
with p.open("r") as f:
    STREAM_METADATA = json.load(f)


class TestMarkFieldsUnsupported(unittest.TestCase):
    def test_mark_fields_unsupported(self):
        """Test to ensure a field is marked unsupported in stream metadata,
        excluding it from future queries."""
        stream = copy.deepcopy(STREAM_METADATA)
        sync.mark_fields_unsupported(stream, ["Field1"])
        query = apis.Aqua.get_query(stream)
        self.assertNotIn("Field1", query)

    def test_mark_fields_unsupported_ignores_unknown_field(self):
        """Test to ensure marking an unknown field is a no-op."""
        stream = copy.deepcopy(STREAM_METADATA)
        sync.mark_fields_unsupported(stream, ["NotARealField"])
        query = apis.Aqua.get_query(stream)
        self.assertIn("Field1", query)


class TestSyncStreamInvalidFieldRetry(unittest.TestCase):
    @mock.patch("tap_zuora.sync.sync_aqua_stream")
    def test_sync_stream_retries_without_invalid_joined_field(self, mock_sync_aqua_stream):
        stream = copy.deepcopy(STREAM_METADATA)
        stream["schema"]["properties"]["Stream3Id"] = {"type": ["string", "null"]}
        stream["metadata"].append({
            "breadcrumb": ["properties", "Stream3Id"],
            "metadata": {
                "inclusion": "available",
                "selected": True,
                "tap-zuora.joined_object": "Stream3",
            },
        })
        api_client = mock.Mock()
        api_client.aqua_request.return_value.json.return_value = {
            "status": "failed",
            "batches": [{"message": "There is no field named Stream3.Id."}],
        }
        with self.assertRaises(apis.InvalidFieldExportFailed) as ctx:
            apis.Aqua.job_ready(api_client, "job123")

        self.assertEqual(ctx.exception.invalid_fields, ["Stream3Id"])
        client = mock.Mock(is_rest=False)
        mock_sync_aqua_stream.side_effect = [ctx.exception, "counter"]

        self.assertEqual(sync.sync_stream(client, {}, stream), "counter")

        self.assertEqual(mock_sync_aqua_stream.call_count, 2)
        self.assertNotIn("Stream3.Id", apis.Aqua.get_query(stream))

    @mock.patch("tap_zuora.sync.sync_aqua_stream")
    def test_sync_stream_retries_without_invalid_field(self, mock_sync_aqua_stream):
        """Test to ensure sync_stream drops an invalid field and retries the
        sync instead of failing immediately."""
        stream = copy.deepcopy(STREAM_METADATA)
        client = mock.Mock(is_rest=False)

        mock_sync_aqua_stream.side_effect = [
            apis.InvalidFieldExportFailed("bad field", ["Field1"]),
            "counter",
        ]

        result = sync.sync_stream(client, {}, stream)

        self.assertEqual(result, "counter")
        self.assertEqual(mock_sync_aqua_stream.call_count, 2)
        query = apis.Aqua.get_query(stream)
        self.assertNotIn("Field1", query)

    @mock.patch("tap_zuora.sync.sync_aqua_stream")
    def test_sync_stream_raises_after_max_retries(self, mock_sync_aqua_stream):
        """Test to ensure sync_stream gives up after MAX_INVALID_FIELD_RETRIES
        instead of retrying forever."""
        stream = copy.deepcopy(STREAM_METADATA)
        client = mock.Mock(is_rest=False)

        mock_sync_aqua_stream.side_effect = apis.InvalidFieldExportFailed("bad field", ["Field1"])

        with self.assertRaises(apis.InvalidFieldExportFailed):
            sync.sync_stream(client, {}, stream)

        self.assertEqual(mock_sync_aqua_stream.call_count, sync.MAX_INVALID_FIELD_RETRIES + 1)
