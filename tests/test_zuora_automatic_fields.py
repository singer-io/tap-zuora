from base import ZuoraBaseTest
from tap_tester import connections, runner


class ZuoraAutomaticFields(ZuoraBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_zuora_automatic_fields_rest"

    def test_run(self):
        """Testing for only AQUA mode to reduce the execution time."""
        self.run_test("AQUA")

    def run_test(self, api_type):
        """Verify that for each stream you can get multiple pages of data when
        no fields are selected and only the automatic fields are replicated.

        PREREQUISITE For EACH stream add enough data that you surpass
        the limit of a single fetch of data.  For instance if you have a
        limit of 250 records ensure that 251 (or more) records have been
        posted for that stream.
        """
        self.zuora_api_type = api_type
        expected_streams = {"Export", "RatePlan"}

        # Instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id,
            test_catalogs_automatic_fields,
            select_all_fields=False,
        )

        # Run initial sync
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get("data").keys()) for row in data.get("messages", {})]
                primary_keys_list = [
                    tuple(message.get("data", {}).get(expected_pk) for expected_pk in expected_primary_keys)
                    for message in data.get("messages", [])
                    if message.get("action") == "upsert"
                ]
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify that all replicated records have unique primary key values.
                self.assertEqual(
                    len(primary_keys_list),
                    len(unique_primary_keys_list),
                    msg="Replicated record does not have unique primary key values.",
                )
