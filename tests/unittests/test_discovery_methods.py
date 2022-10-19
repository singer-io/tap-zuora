from tap_zuora import discover
import unittest
import requests
from utils import get_response
from unittest import mock
from tap_zuora.client import Client
import pathlib

FIELD_RESPONSE = {'Field1': {'type': 'string', 'required': False, 'supported': True},
                  'Id': {'type': 'integer', 'required': True, 'supported': True},
                  'UpdatedOn': {'type': 'string', 'required': True, 'supported': True},
                  'Stream3.Id': {'type': 'string', 'required': False, 'supported': True,
                                 'joined': True}}


class TestDiscoveryMethods(unittest.TestCase):

    def test_is_unsupported_field_rest(self):
        """
        Test to ensure whether a field is unsupported for a given stream for REST API Calls
        """
        stream = "InvoiceItem"
        field_name = "ExcludeItemBillingFromRevenueAccounting"
        self.assertEqual(discover.is_unsupported_field(stream, field_name, True), True)

    def test_is_unsupported_field_aqua(self):
        """
        Test to ensure whether a field is unsupported for a given stream for AQuA API Calls
        it should always be False for AQuA calls
        """
        stream = "Invoice"
        field = "SourceType"
        self.assertEqual(discover.is_unsupported_field(stream, field, False), False)

    @mock.patch('tap_zuora.client.Client.rest_request')
    @mock.patch('requests.Session.send')
    @mock.patch('requests.Request')
    def test_discovery_streams(self, mock_request, mock_send, mock_rest_request):
        """
        Test to ensure that we get right list of streams by parsing the XML content
        """
        mock_request.return_value = requests.Request()
        mock_send.return_value = get_response(200, json={'id': 1234})
        client_object = Client.from_config({'username': '', 'password': ''})
        p = pathlib.Path(__file__).with_name('sample_stream_data.xml')
        with p.open('r') as f:
            mock_rest_request.return_value = get_response(200, {}, False, f.read())
        expected_response = ['Stream1', 'Stream2', 'Stream3']
        print(discover.discover_stream_names(client_object))
        self.assertEqual(discover.discover_stream_names(client_object), expected_response)

    @mock.patch('tap_zuora.client.Client.rest_request')
    @mock.patch('requests.Session.send')
    @mock.patch('requests.Request')
    def test_get_field_dict(self, mock_request, mock_send, mock_rest_request):
        """
        Test to ensure that we get right list of fields for a given stream
        """
        mock_request.return_value = requests.Request()
        mock_send.return_value = get_response(200, json={'id': 1234})
        client_object = Client.from_config({'username': '', 'password': ''})
        p = pathlib.Path(__file__).with_name('sample_fields_data.xml')
        with p.open('r') as f:
            mock_rest_request.return_value = get_response(200, {}, False, f.read())
        self.assertEqual(discover.get_field_dict(client_object, 'Stream1'), FIELD_RESPONSE)

    @mock.patch('requests.Session.send')
    @mock.patch('requests.Request')
    @mock.patch('tap_zuora.discover.get_field_dict')
    def test_discover_stream_with_fields(self, mock_field_dict, mock_request, mock_send):
        """
        Test to ensure that we get correct catalog content for a given stream
        """
        mock_request.return_value = requests.Request()
        mock_send.return_value = get_response(200, json={'id': 1234})
        client_object = Client.from_config({'username': '', 'password': ''})
        mock_field_dict.return_value = FIELD_RESPONSE
        expected_response = {'tap_stream_id': 'Stream1', 'stream': 'Stream1', 'key_properties': ['Id'], 'schema':
            {'type': 'object', 'additionalProperties': False, 'properties': {'Field1': {'type': ['string', 'null']},
                                                                             'Id': {'type': ['integer', 'null']},
                                                                             'UpdatedOn': {'type': ['string', 'null']},
                                                                             'Stream3Id': {'type': ['string', 'null']},
                                                                             'Deleted': {'type': 'boolean'}}},
                             'metadata': [{'breadcrumb': (), 'metadata': {'table-key-properties': ['Id'],
                                                                          'forced-replication-method': 'INCREMENTAL',
                                                                          'valid-replication-keys': ['UpdatedOn'],
                                                                          'inclusion': 'available'}},
                                          {'breadcrumb': ('properties', 'Field1'),
                                           'metadata': {'inclusion': 'available'}},
                                          {'breadcrumb': ('properties', 'Id'), 'metadata': {'inclusion': 'automatic'}},
                                          {'breadcrumb': ('properties', 'UpdatedOn'),
                                           'metadata': {'inclusion': 'automatic'}},
                                          {'breadcrumb': ('properties', 'Stream3Id'),
                                           'metadata': {'tap-zuora.joined_object': 'Stream3',
                                                        'inclusion': 'available'}},
                                          {'breadcrumb': ('properties', 'Deleted'),
                                           'metadata': {'inclusion': 'available'}}], 'replication_key': 'UpdatedOn',
                             'replication_method': 'INCREMENTAL'}

        self.assertEqual(discover.discover_stream(client_object, 'Stream1'), expected_response)
