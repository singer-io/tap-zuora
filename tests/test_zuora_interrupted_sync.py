import copy
from datetime import datetime as dt
from datetime import timedelta

from base import ZuoraBaseTest
from singer import utils
from singer.utils import strptime_to_utc
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class ZuoraInterruptedSyncTest(ZuoraBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    def name(self):
        """Returns the name of the test case."""
        return "tap_tester_zuora_interrupted_test"

    def test_run(self):
        """Executing tap-tester scenarios for both types of zuora APIs AQUA and
        REST."""
        # Testing for only AQUA mode to reduce the execution time
        self.run_test("AQUA")

    def run_test(self, api_type):
        """
        Scenario: A sync job is interrupted. The state is saved with `current_stream`.
                  The next sync job kicks off and the tap picks back up on that `current_stream` stream.
        Expected State Structure:
            {
                "current_stream": "stream-name",
                "bookmarks": {
                    "stream-name-1": "bookmark-date"
                    "stream-name-2": "bookmark-date"
                }
            }
        Test Cases:
        - Verify an interrupted sync can resume based on the `current_stream` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark are
            replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """
        self.zuora_api_type = api_type
        self.start_date = dt.strftime(utils.now() - timedelta(days=10), "%Y-%m-%dT00:00:00Z")
        expected_streams = {"PaymentMethodTransactionLog"}

        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Catalog selection
        for catalog in found_catalogs:
            if catalog["stream_name"] not in expected_streams:
                continue

            annoted_schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            # De-select all fields
            non_selected_properties = annoted_schema.get("annotated-schema", {}).get("properties", {})
            non_selected_properties = non_selected_properties.keys()
            additional_md = []
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog,
                annoted_schema,
                additional_md=additional_md,
                non_selected_fields=non_selected_properties,
            )

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        LOGGER.info(f"first_sync_record_count = {first_sync_record_count}")

        completed_streams = {"RatePlan"}
        pending_streams = {"PaymentMethodTransactionLog"}
        interrupt_stream = "OrderAction"

        interrupted_sync_states = self.create_interrupt_sync_state(
            copy.deepcopy(first_sync_bookmarks),
            interrupt_stream,
            pending_streams,
            first_sync_records,
        )
        bookmark_state = interrupted_sync_states["bookmarks"]
        menagerie.set_state(conn_id, interrupted_sync_states)

        LOGGER.info("Interrupted Bookmark - %s", interrupted_sync_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)
        LOGGER.info("second_sync_record_count = %s ", second_sync_record_count)
        LOGGER.info("second_sync_bookmarks = %s", second_sync_bookmarks)

        # Run sync after interruption
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get("current_stream")

        # Checking resuming the sync resulted in a successfully saved state
        with self.subTest():
            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get("bookmarks"))

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_replication_method = self.expected_replication_method()[stream]
                replication_key = next(iter(self.expected_replication_keys()[stream]))

                # Gather results
                full_records = [message["data"] for message in first_sync_records[stream]["messages"]]
                interrupted_records = [message["data"] for message in second_sync_records[stream]["messages"]]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                LOGGER.info("first_bookmark_value =%s ", first_bookmark_value)
                LOGGER.info("second_bookmark_value =%s ", second_bookmark_value)

                if expected_replication_method == self.INCREMENTAL:
                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreaterEqual(
                            second_sync_count,
                            1,
                            msg="Incorrect bookmarking for {}, at least one or more record \
                                                should be replicated".format(
                                stream
                            ),
                        )

                    elif stream == interrupted_sync_states.get("current_stream", None):
                        # For interrupted stream second sync count should be greater than or equal to
                        # remaining records count

                        interrupted_bmk = strptime_to_utc(bookmark_state[stream][replication_key])

                        # Record count for all streams of interrupted sync match expectations
                        remaining_records = 0

                        for record in full_records:
                            rec_time = strptime_to_utc(record.get(replication_key))
                            if rec_time >= interrupted_bmk:
                                remaining_records += 1

                        self.assertGreaterEqual(len(interrupted_records), remaining_records)

                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(
                            second_sync_count,
                            first_sync_count,
                            msg="For pending sync streams - {}, second sync record count should \
                                                be more than or equal to first sync".format(
                                stream
                            ),
                        )

                    else:
                        raise Exception(
                            "Invalid state of stream {} in interrupted state, \
                                         please update appropriate state for the stream".format(
                                stream
                            )
                        )

                elif expected_replication_method == self.FULL_TABLE:
                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )
