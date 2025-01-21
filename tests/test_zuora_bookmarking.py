from base import ZuoraBaseTest
from singer.utils import strptime_to_utc
from tap_tester import connections, menagerie, runner


class ZuoraBookmarking(ZuoraBaseTest):
    @staticmethod
    def name():
        return "tap_tester_zuora_bookmarking"

    def test_run(self):
        """Executing tap-tester scenarios for both types of zuora APIs AQUA and
        REST."""
        self.run_test("AQUA")
        self.run_test("REST")

    def run_test(self, api_type):
        """
        - Verify that for each stream you can do a sync which records
        bookmarks. That the bookmark is the maximum value sent to the target
        for the replication key.
        - A second sync respects the bookmark
        - All data of the second sync is >= the bookmark from the first sync.
        - The number of records in the 2nd sync is less then the first
        (This assumes that new data added to the stream is done at a rate slow enough that
        you haven't doubled the amount of data from the start date to the first
        sync between the first sync and second sync run in this test)
        - Verify that for full table stream, all data replicated in sync 1 is replicated
        again in sync 2.

        PREREQUISITE For EACH stream that is incrementally replicated
        there are multiple rows of data with different values for
        the replication key
        """
        self.zuora_api_type = api_type

        # Select only the expected streams tables
        expected_streams = {"Export"}
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        catalog_entries = [catalog for catalog in found_catalogs if catalog["tap_stream_id"] in expected_streams]

        # Disable all_fields selection as some fields are missing in the object
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, select_all_fields=False)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {"bookmarks": dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks, expected_streams)

        for stream, new_state in simulated_states.items():
            new_states["bookmarks"][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark_key_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:
                    # Collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                    simulated_bookmark_value = self.convert_state_to_utc(
                        new_states["bookmarks"][stream][replication_key]
                    )

                    simulated_bookmark_minus_lookback = simulated_bookmark_value

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Greater than or Equal to the first sync bookmark
                    # as data changes during test
                    self.assertGreaterEqual(second_bookmark_value, first_bookmark_value)

                    for record in first_sync_messages:
                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        self.assertLessEqual(
                            replication_key_value,
                            first_bookmark_value_utc,
                            msg="A record with a greater replication-key value was synced in first sync.",
                        )

                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        self.assertGreaterEqual(
                            strptime_to_utc(replication_key_value),
                            strptime_to_utc(simulated_bookmark_minus_lookback),
                            msg="Second sync records do not repeat the previous bookmark.",
                        )

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value_utc,
                            msg="A record with a greater replication-key value was synced in second sync.",
                        )

                    # Verify that you get less than or equal to data getting at 2nd time around
                    self.assertLessEqual(
                        second_sync_count,
                        first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified",
                    )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count,
                    0,
                    msg=f"We are not fully testing bookmarking for {stream}",
                )
