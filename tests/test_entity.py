import unittest

from singer.schema import Schema

from tap_zuora import entity, state


class TestEntityFormatFunctions(unittest.TestCase):
    def test_format_zoql_datetime(self):
        self.assertEqual("2017-01-01 00:00:00", entity.format_zoql_datetime("2017-01-01T00:00:00Z"))

    def test_format_datetime(self):
        self.assertEqual("2017-01-01T00:00:00Z", entity.format_datetime("2017-01-01T00:00:00.000"))
        self.assertEqual("2017-01-01T08:00:00Z", entity.format_datetime("2017-01-01T00:00:00-08:00"))
        self.assertEqual("2017-01-01T00:00:00Z", entity.format_datetime("2017-01-01"))

    def test_format_value_empty_string_is_none(self):
        self.assertIsNone(entity.format_value("", "string"))
        self.assertIsNone(entity.format_value("", "integer"))
        self.assertIsNone(entity.format_value("", "number"))
        self.assertIsNone(entity.format_value("", "boolean"))
        self.assertIsNone(entity.format_value("", "date"))
        self.assertIsNone(entity.format_value("", "datetime"))
        self.assertIsNone(entity.format_value("", "thisisntavalidtype"))

    def test_format_value_integer(self):
        self.assertEqual(42, entity.format_value("42", "integer"))
        self.assertEqual(int, type(entity.format_value("42", "integer")))

        self.assertEqual(1701, entity.format_value(1701, "integer"))
        self.assertEqual(int, type(entity.format_value(1701, "integer")))

        self.assertEqual(1337, entity.format_value(1337.1337, "integer"))
        self.assertEqual(int, type(entity.format_value(1337.1337, "integer")))

        with self.assertRaises(ValueError):
            entity.format_value("thiswillfail", "integer")

    def test_format_value_number(self):
        self.assertEqual(42, entity.format_value("42", "number"))
        self.assertEqual(float, type(entity.format_value("42", "number")))

        self.assertEqual(17.01, entity.format_value(17.01, "number"))
        self.assertEqual(float, type(entity.format_value(17.01, "number")))

        with self.assertRaises(ValueError):
            entity.format_value("thiswillfail", "number")

    def test_format_value_datetime(self):
        self.assertEqual("2017-01-01T00:00:00Z", entity.format_value("2017-01-01T00:00:00.000", "datetime"))
        self.assertEqual("2017-01-01T08:00:00Z", entity.format_value("2017-01-01T00:00:00-08:00", "datetime"))
        self.assertEqual("2017-01-01T00:00:00Z", entity.format_value("2017-01-01", "date"))

    def test_format_value_boolean(self):
        self.assertTrue(entity.format_value(True, "boolean"))
        self.assertTrue(entity.format_value("True", "boolean"))
        self.assertTrue(entity.format_value("true", "boolean"))

        self.assertFalse(entity.format_value(False, "boolean"))
        self.assertFalse(entity.format_value("False", "boolean"))
        self.assertFalse(entity.format_value("false", "boolean"))

        self.assertFalse(entity.format_value("thisisntaboolean", "boolean"))

    def test_format_value_unknown(self):
        self.assertEqual("junk", entity.format_value("junk", "notarealtype"))


class TestEntity(unittest.TestCase):
    def setUp(self):
        self.schema = Schema.from_dict({
            "type": "object",
            "properties": {
                "id": {"type": "integer", "selected": True},
                "field": {"type": "string", "selected": True},
                "updated": {"type": "string", "format": "date-time", "selected": True},
                "junk": {"type": "string", "selected": False},
            }
        })
        self.entity = entity.Entity("test", self.schema, "updated", ["id"])

    def test_get_fields(self):
        self.assertListEqual(["field", "id", "updated"], self.entity.get_fields())

    def test_get_base_query(self):
        self.assertEqual("select field, id, updated from test", self.entity.get_base_query())

    def test_get_zoqlexport_no_start_date(self):
        self.assertEqual("select field, id, updated from test", self.entity.get_zoqlexport())

    def test_get_zoqlexport_with_start_date(self):
        self.assertEqual(
            "select field, id, updated from test where updated >= '2017-01-01 00:00:00' order by updated asc",
            self.entity.get_zoqlexport("2017-01-01T00:00:00Z"))

        self.entity.replication_key = None
        self.assertEqual(
            "select field, id, updated from test",
            self.entity.get_zoqlexport("2017-01-01T00:00:00Z"))

    def test_get_zoql_no_dates(self):
        self.assertEqual("select field, id, updated from test", self.entity.get_zoql())

    def test_get_zoql_one_date(self):
        self.assertEqual(
            "select field, id, updated from test",
            self.entity.get_zoql(start_date="2017-01-01T00:00:00Z"))

        self.assertEqual(
            "select field, id, updated from test",
            self.entity.get_zoql(end_date="2017-02-01T00:00:00Z"))

        self.assertEqual(
            "select field, id, updated from test "
            "where updated >= '2017-01-01 00:00:00' and updated < '2017-02-01 00:00:00'",
            self.entity.get_zoql(start_date="2017-01-01T00:00:00Z", end_date="2017-02-01T00:00:00Z"))

    def test_format_values(self):
        self.assertDictEqual(
            {
                "id": 123,
                "field": "data",
                "updated": "2017-01-02T00:00:00Z",
            },
            self.entity.format_values({
                "id": "123",
                "field": "data",
                "updated": "2017-01-02T00:00:00Z",
                "junk": "idonotwantthis",
            }))

    def test_update_bookmark(self):
        the_state = state.State({"bookmarks": {"test": {"updated": "2016-12-01T00:00:00Z"}}})
        record = self.entity.format_values({
            "id": "123",
            "field": "data",
            "updated": "2017-01-02T00:00:00Z",
            "junk": "idonotwantthis",
        })
        self.entity.update_bookmark(record, the_state)
        self.assertEqual(record["updated"], the_state.get_bookmark(self.entity))
