import os
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from functools import reduce
import unittest
from singer import utils
from datetime import timedelta, datetime

class Zuora(unittest.TestCase):

    def tap_name(self):
        return "tap-zuora"

    def name(self):
        return "tap_tester_zuora"

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_ZUORA_USERNAME'),
                                    os.getenv('TAP_ZUORA_PASSWORD')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_ZUORA_USERNAME, TAP_ZUORA_PASSWORD")

    def get_type(self):
        return "platform.zuora"

    def expected_check_streams(self):
        return {
            'Account'
        }

    def expected_sync_streams(self):
        return {
            'Account'
        }

    def expected_pks(self):
        return {
            'Account': {"Id"}
        }

    def get_credentials(self):
        return {'username': os.getenv('TAP_ZUORA_USERNAME'),
                'password': os.getenv('TAP_ZUORA_PASSWORD')
        }

    def get_properties(self):
        return {
            'start_date' : ('2017-01-05T00:00:00Z'
                            if self.api_type == "AQUA"
                            else datetime.strftime(utils.now() - timedelta(days=30), "%Y-%m-%dT00:00:00Z")),
            'partner_id' : os.getenv('TAP_ZUORA_PARTNER_ID'),
            'api_type' : self.api_type,
            'sandbox' : 'true'
        }

    def perform_field_selection(self, conn_id, catalog):
        schema = menagerie.select_catalog(conn_id, catalog)

        return {'key_properties' :     catalog.get('key_properties'),
                'schema' :             schema,
                'tap_stream_id':       catalog.get('tap_stream_id'),
                'replication_method' : catalog.get('replication_method'),
                'replication_key'    : catalog.get('replication_key')}

    def test_run(self):
        for api_type in ['AQUA', 'REST']:
            with self.subTest(api_type=api_type):
                self.run_for_api(api_type)

    def run_for_api(self, api_type):
        self.api_type = api_type
        conn_id = connections.ensure_connection(self)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            # BUG TDL-15828 - Remove non_selected_fields when discovery no longer returns them for REST
            non_selected_fields = ['SequenceSetId'] if api_type=='REST' else []
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_fields)

        # Clear State and run sync
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # We should receive at least 1 account greater than start_date and bookmark on it
        account_bm = menagerie.get_state(conn_id)["bookmarks"]["Account"]["UpdatedDate"]
        start_date = self.get_properties()["start_date"]
        self.assertTrue(account_bm > start_date)
