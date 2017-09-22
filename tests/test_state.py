import datetime
import unittest

import freezegun
from singer.schema import Schema

from tap_zuora import entity, state


class TestState(unittest.TestCase):
    def setUp(self):
        self.entity = entity.Entity(
            "test",
            Schema.from_dict({
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "selected": True},
                    "field": {"type": "string", "selected": True},
                    "updated": {"type": "string", "format": "date-time", "selected": True},
                },
            }),
            "updated",
            ["id"])

        self.entity_no_rep = entity.Entity(
            "nokey",
            Schema.from_dict({
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "selected": True},
                    "field": {"type": "string", "selected": True},
                },
            }),
            None,
            ["id"])

        self.state = state.State({
            "bookmarks": {
                "test": {
                    "updated": "2017-01-01T00:00:00Z",
                },
            },
            "current_stream": "test",
        }, "2016-01-01T00:00:00Z")

    def test_to_dict(self):
        self.assertDictEqual(
            {"bookmarks": {"test": {"updated": "2017-01-01T00:00:00Z"}}, "current_stream": "test"},
            self.state.to_dict())

    def test_get_entity_state(self):
        self.assertDictEqual({"updated": "2017-01-01T00:00:00Z"}, self.state.get_entity_state(self.entity))

        self.assertDictEqual({}, self.state.get_entity_state(self.entity_no_rep))

    @freezegun.freeze_time("2017-01-01")
    def test_get_version(self):
        self.assertIsNone(self.state.bookmarks["test"].get("version"))

        self.assertEqual(
            int(datetime.datetime(2017, 1, 1).timestamp()),
            self.state.get_version(self.entity))

        self.assertEqual(
            int(datetime.datetime(2017, 1, 1).timestamp()),
            self.state.bookmarks["test"].get("version"))

    def test_get_bookmark(self):
        self.assertEqual("2017-01-01T00:00:00Z", self.state.get_bookmark(self.entity))

        self.state.bookmarks = {}
        self.assertEqual("2016-01-01T00:00:00Z", self.state.get_bookmark(self.entity))

    def test_get_bookmark_no_replication_key(self):
        with self.assertRaises(Exception):
            self.state.get_bookmark(self.entity_no_rep)

    def test_set_bookmark(self):
        self.state.set_bookmark(self.entity, "2017-02-01T00:00:00Z")
        self.assertEqual("2017-02-01T00:00:00Z", self.state.bookmarks["test"]["updated"])

    def test_set_bookmark_not_more_recent(self):
        self.state.set_bookmark(self.entity, "2016-02-01T00:00:00Z")
        self.assertEqual("2017-01-01T00:00:00Z", self.state.bookmarks["test"]["updated"])

    def test_set_bookmark_no_replication_key(self):
        with self.assertRaises(Exception):
            self.state.set_bookmark(self.entity_no_rep, "2017-01-01T00:00:00Z")

    def test_get_file_ids(self):
        self.assertListEqual([], self.state.get_file_ids(self.entity))

        self.state.bookmarks["test"]["file_ids"] = ["1a", "2b"]
        self.assertListEqual(["1a", "2b"], self.state.get_file_ids(self.entity))

    def test_set_file_ids(self):
        self.state.set_file_ids(self.entity, ["1a", "2b"])
        self.assertListEqual(["1a", "2b"], self.state.bookmarks["test"]["file_ids"])
