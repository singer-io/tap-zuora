from tap_tester import connections, runner, menagerie
from base import ZuoraBaseTest


# As we are not able to generate following fields by yotpo post apis, so removed it form expectation list.
KNOWN_MISSING_FIELDS = {
    'product_reviews': {
        'comment',
        'images_data'
    },
    'reviews': {
        'user_reference'
    }
}
class ZuoraAllFields(ZuoraBaseTest):
    """Ensure running the tap with all streams and fields selected results in the replication of all fields."""
     
    def name(self):
        return "tap_tester_zuora_all_fields_test"

    def test_run(self):
        #self.run_test("REST")
        self.run_test("AQUA")

    def run_test(self, api_type):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """
        self.zuora_api_type = api_type
        DOES_NOT_SUPPORT_DELETED_ORG = {'AccountingPeriod', 'ContactSnapshot', 'DiscountAppliedMetrics',
        'PaymentGatewayReconciliationEventLog', 'PaymentTransactionLog', 'PaymentMethodTransactionLog',
        'PaymentReconciliationJob', 'PaymentReconciliationLog', 'ProcessedUsage',
        'RefundTransactionLog', 'UpdaterBatch', 'UpdaterDetail'}

        DOES_NOT_SUPPORT_DELETED = {'BookingTransaction','SmartPreventionAudit','HpmCaptchaValidationResult','CalloutHistory','EmailHistory'}

        ZERO_RECORDS_SYNCED = {'AchNocEventLog','AccountingCode','AccountingPeriod','Amendment','BillingRun',
        'ChargeMetrics','ChargeMetricsRun','CommunicationProfile','DiscountAppliedMetrics','DiscountApplyDetail',
        'DiscountClass','PaymentGatewayReconciliationEventLog','PaymentReconciliationJob','PaymentReconciliationLog',
        'DiscountAppliedMetrics',  'DiscountApplyDetail', 'DiscountClass','Import','InvoiceAdjustment','InvoiceItemAdjustment',
        'InvoiceSplit','InvoiceSplitItem'}

        # Streams to verify all fields tests
        expected_streams = self.expected_streams() - DOES_NOT_SUPPORT_DELETED - ZERO_RECORDS_SYNCED #- 'JOURN'#self.expected_streams()

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # # Verify no unexpected streams were replicated
        # synced_stream_names = set(synced_records.keys())
        # self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(stream, set())

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                expected_all_keys = expected_all_keys - KNOWN_MISSING_FIELDS.get(stream, set())
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())
                # Verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)