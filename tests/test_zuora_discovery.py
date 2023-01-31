from base import ZuoraBaseTest
from tap_tester import connections, menagerie
from tap_tester.logger import LOGGER


class DiscoveryTest(ZuoraBaseTest):
    """Test tap discovery mode and metadata/annotated-schema conforms to
    standards."""

    @staticmethod
    def name():
        return "tap_tester_zuora_discovery_rest"

    def test_run(self):
        """Executing tap-tester scenarios for both types of zuora APIs AQUA and
        REST."""
        self.run_test("AQUA")
        self.run_test("REST")

    def run_test(self, api_type):
        """Verify that discover creates the appropriate catalog, schema,
        metadata, etc.

        - Verify number of actual streams discovered match expected
        - Verify the stream names discovered were what we expect
        - Verify stream names follow naming convention   streams should only have
            lowercase alphas and underscores
        - Verify there is only 1 top level breadcrumb
        - Verify primary key(s) match expectations.
        - Verify replication key(s) match expectations.
        - Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL.
        - Verify the actual replication matches our expected
            replication method
        - Verify that primary, replication and foreign keys are given the inclusion of automatic (metadata
            and annotated schema)
        - Verify that all other fields have inclusion of available (metadata and schema)
        """
        self.zuora_api_type = api_type

        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self, original_properties=False)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # verify the tap only discovers the expected streams
        found_catalog_names = {catalog["tap_stream_id"] for catalog in found_catalogs} - self.streams_not_under_test
        self.assertSetEqual(streams_to_test, found_catalog_names)
        LOGGER.info("discovered schemas are OK")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs if catalog["stream_name"] == stream]))
                # based on previous tests this should always be found
                self.assertIsNotNone(catalog)

                # gather expectations
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_method = self.expected_replication_method()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                if stream in self.additional_automatic_field_in_streams:
                    expected_automatic_fields.add("TransactionDate")

                # gather results
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                metadata = schema_and_metadata["metadata"]
                schema = schema_and_metadata["annotated-schema"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_replication_keys = set(
                    stream_properties[0].get("metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_primary_keys = set(
                    stream_properties[0].get("metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_method = (
                    stream_properties[0].get("metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                )

                # verify there is only 1 top level breadcrumb
                self.assertTrue(
                    len(stream_properties) == 1,
                    msg=f"There is NOT only one top level breadcrumb for {stream}"
                    + f"\n stream_properties | {stream_properties}",
                )

                actual_fields = []
                for md_entry in metadata:
                    if md_entry["breadcrumb"] != []:
                        actual_fields.append(md_entry["breadcrumb"][1])

                # Verify there are no duplicate/conflicting metadata entries.
                self.assertEqual(
                    len(actual_fields),
                    len(set(actual_fields)),
                    msg="duplicates in the metadata entries retrieved : \
                                 {set([x for x in actual_fields if actual_fields.count(x) > 1])}",
                )

                # verify replication key(s)
                self.assertSetEqual(expected_replication_keys, actual_replication_keys)

                # verify primary key(s)
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # verify the actual replication matches our expected replication method
                self.assertEqual(expected_replication_method, actual_replication_method)

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if actual_replication_keys:
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL_TABLE, actual_replication_method)

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in annotated schema.
                actual_automatic_fields = {
                    key for key, value in schema["properties"].items() if value.get("inclusion") == "automatic"
                }

                self.assertEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in metadata.
                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                }

                self.assertEqual(
                    expected_automatic_fields,
                    actual_automatic_fields,
                    msg="expected {} automatic fields but got {}".format(
                        expected_automatic_fields, actual_automatic_fields
                    ),
                )

                self.assertTrue(
                    all(
                        {
                            (
                                item.get("metadata").get("inclusion") == "available"
                                or item.get("metadata").get("inclusion") == "unsupported"
                            )
                            for item in metadata
                            if item.get("breadcrumb", []) != []
                            and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                        }
                    ),
                    msg="Not all non key properties are set to available in metadata",
                )
