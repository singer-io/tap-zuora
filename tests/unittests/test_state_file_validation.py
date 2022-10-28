import unittest
from unittest import mock
from typing import Dict, List
from singer.catalog import Catalog, CatalogEntry
from tap_zuora import validate_state, convert_legacy_state


class Schema:
    selected = True


class CatalogProperties:
    """
    Sets the properties required for catalog object
    """

    def __init__(self, is_selected):
        self.replication_key = 'UpdatedOn'
        self.replication_method = 'INCREMENTAL'
        self.metadata = [{'breadcrumb': [],
                          'metadata': {'table-key-properties': ['Id'], 'forced-replication-method': 'INCREMENTAL',
                                       'valid-replication-keys': ['UpdatedOn'], 'inclusion': 'available',
                                       'selected': is_selected}}]
        self.schema = Schema


def mock_catalog_entry(streams: List[Dict]) -> List:
    """
    Mocks the catalog entry object
    """
    catalog_streams = []
    for stream in streams:
        properties = CatalogProperties(stream['is_selected'])
        catalog_streams.append(CatalogEntry(tap_stream_id=stream['name'], replication_key=properties.replication_key,
                                            schema=properties.schema, metadata=properties.metadata))
    return catalog_streams


def mock_catalog(stream_names: List[Dict]) -> Catalog:
    return Catalog(mock_catalog_entry(stream_names))


class TestConvertLegacyState(unittest.TestCase):
    """
    Tests Covert_legacy_state fn for converting the state file to legacy format
    """

    def test_with_one_stream_in_state_file(self):
        state_file = {"RevenueEventItem": {'UpdatedOn': '2022-10-01'}}
        resp = convert_legacy_state(mock_catalog([{"name": "RevenueEventItem", "is_selected": True}]), state_file)
        self.assertEqual(resp, {"bookmarks": {"RevenueEventItem": {"UpdatedOn": "2022-10-01"}}, "current_stream": None})

    def test_with_no_stream_in_state_file(self):
        state_file = {}  # Sets the state file to be empty
        self.assertEqual(convert_legacy_state(mock_catalog([{"name": "RevenueEventItem", "is_selected": True}]),
                                              state_file), {"bookmarks": {}, "current_stream": None})


@mock.patch('time.time')
class TestValidateState(unittest.TestCase):
    """
    Tests validate_state fn
    """

    def test_with_empty_state_file(self, mock_time):
        """
        Make sure the program creates a state_file with bookmark value as start_date for all selected streams
        with an empty state_file as an input
        """
        state_file = {}
        catalog_file = mock_catalog([{"name": "RevenueEventItem", "is_selected": True},
                                     {"name": "Account", "is_selected": True}])
        config_file = {"start_date": "2022-10-01T00:00:00Z"}
        mock_time.return_value = 1234567
        expected_response = {"bookmarks": {"RevenueEventItem": {"version": 1234567, "UpdatedOn": "2022-10-01T00:00:00Z"}
                             , "Account": {"version": 1234567, "UpdatedOn": "2022-10-01T00:00:00Z"}},
                             "current_stream": None}
        self.assertEqual(validate_state(config_file, catalog_file, state_file), expected_response)

    def test_current_stream_not_selected(self, mock_time):
        """
        Make sure the state file has current_stream set as None when current_stream from existing state file is not
        selected
        """
        state_file = {"current_stream": "Account", "bookmarks": {}}
        catalog_file = mock_catalog([{"name": "RevenueEventItem", "is_selected": True},
                                     {"name": "Account", "is_selected": False},
                                     {"name": "Subscription", "is_selected": True}])
        config_file = {"start_date": "2022-10-01T00:00:00Z"}
        mock_time.return_value = 1234567
        expected_response = {"bookmarks": {"RevenueEventItem": {"version": 1234567, "UpdatedOn": "2022-10-01T00:00:00Z"}
                             , "Subscription": {"version": 1234567, "UpdatedOn": "2022-10-01T00:00:00Z"}},
                             "current_stream": None}
        self.assertEqual(validate_state(config_file, catalog_file, state_file), expected_response)
