import copy
import os
import unittest
from datetime import datetime, timedelta

import dateutil.parser
import pytz
import singer
from singer import utils
from tap_tester import connections, menagerie, runner
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({ **jira_config })
LOGGER = singer.get_logger()


class ZuoraBaseTest(unittest.TestCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    PRIMARY_KEYS = "table-key-properties"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT%H:%M:%S.000000Z"
    REPLICATION_KEYS = "valid-replication-keys"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    OBEYS_START_DATE = "obey-start-date"
    zuora_api_type = ""
    start_date = datetime.strftime(utils.now() - timedelta(days=3), "%Y-%m-%dT00:00:00Z")

    # Few streams have UpdatedAt and TransactionDate both the fields and both are automatic
    # but updatedAt is the only field used as replication key
    additional_automatic_field_in_streams = {
        "BookingTransaction",
        "JournalEntryDetailRefundInvoicePayment",
        "JournalEntryDetailPaymentApplication",
        "JournalEntryDetailCreditBalanceAdjustment",
        "JournalEntryDetailInvoiceItem",
        "JournalEntryDetailCreditMemoApplicationItem",
        "JournalEntryDetailCreditMemoItem",
        "JournalEntryDetailPaymentApplicationItem",
        "JournalEntryDetailRevenueEventItem",
        "JournalEntryDetailRefundApplication",
        "JournalEntryDetailRefundApplicationItem",
        "JournalEntryDetailDebitMemoItem",
        "JournalEntryDetailCreditTaxationItem",
        "JournalEntryDetailInvoicePayment",
        "JournalEntryDetailInvoiceAdjustment",
        "JournalEntryDetailTaxationItem",
        "JournalEntryDetailDebitTaxationItem",
        "JournalEntryDetailInvoiceItemAdjustment",
        "RevenueRecognitionEventsTransaction"
    }

    #BUG: https://jira.talendforge.org/browse/TDL-21812
    streams_not_under_test = {
        "JournalEntryDetailRealizedFxGainLoss",
        "JournalEntryDetailUnrealizedFxGainLoss",
        "SubscriptionStatusHistory",
    }

    def name(self):
        return "tap_tester_zuora"

    def tap_name(self):
        """The name of the tap."""
        return "tap-zuora"

    def setUp(self):
        """Checking required environment variables."""
        missing_envs = [x for x in [os.getenv("TAP_ZUORA_USERNAME"), os.getenv("TAP_ZUORA_PASSWORD")] if x is None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_ZUORA_USERNAME, TAP_ZUORA_PASSWORD")

    def get_type(self):
        """The expected url route ending."""
        return "platform.zuora"

    def get_credentials(self):
        """Authentication information for the test account."""
        return {
            "username": os.getenv("TAP_ZUORA_USERNAME"),
            "password": os.getenv("TAP_ZUORA_PASSWORD"),
        }

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date,
            "partner_id": os.getenv("TAP_ZUORA_PARTNER_ID"),
            "api_type": self.zuora_api_type,
            "sandbox": "true",
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return_value["api_type"] = self.zuora_api_type
        return return_value

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""
        default_full = {
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_METHOD: self.FULL_TABLE,
            self.OBEYS_START_DATE: False,
        }

        incremental_updated_on = {
            self.REPLICATION_KEYS: {"UpdatedOn"},
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True,
        }

        incremental_updated_date = {
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_KEYS: {"UpdatedDate"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True,
        }

        incremental_transaction_date = {
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_KEYS: {"TransactionDate"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True,
        }

        return {
            "AchNocEventLog": incremental_updated_on,
            "Account": incremental_updated_date,
            "AccountingCode": incremental_updated_date,
            "AccountingPeriod": incremental_updated_date,
            "Amendment": incremental_updated_date,
            "BillingRun": incremental_updated_date,
            "BookingTransaction": incremental_updated_date,
            "CommunicationProfile": incremental_updated_date,
            "Contact": incremental_updated_date,
            "ContactSnapshot": incremental_updated_date,
            "CreditBalanceAdjustment": incremental_updated_date,
            "DiscountAppliedMetrics": incremental_updated_date,
            "DiscountApplyDetail": incremental_updated_date,
            "DiscountClass": incremental_updated_date,
            "Export": incremental_updated_date,
            "PaymentGatewayReconciliationEventLog": incremental_updated_date,
            "PaymentReconciliationJob": incremental_updated_date,
            "PaymentReconciliationLog": incremental_updated_date,
            "SmartPreventionAudit": incremental_updated_date,
            "HpmCaptchaValidationResult": incremental_updated_date,
            "Import": incremental_updated_date,
            "Invoice": incremental_updated_date,
            "InvoiceAdjustment": incremental_updated_date,
            "InvoiceItem": incremental_updated_date,
            "InvoiceItemAdjustment": incremental_updated_date,
            "InvoicePayment": incremental_updated_date,
            "InvoiceSplit": incremental_updated_date,
            "InvoiceSplitItem": incremental_updated_date,
            "JournalEntry": incremental_updated_date,
            "JournalEntryDetailCreditBalanceAdjustment": incremental_updated_date,
            "JournalEntryDetailCreditMemoApplicationItem": incremental_updated_date,
            "JournalEntryDetailCreditMemoItem": incremental_updated_date,
            "JournalEntryDetailCreditTaxationItem": incremental_updated_date,
            "JournalEntryDetailDebitMemoItem": incremental_updated_date,
            "JournalEntryDetailDebitTaxationItem": incremental_updated_date,
            "JournalEntryDetailInvoiceAdjustment": incremental_updated_date,
            "JournalEntryDetailInvoiceItem": incremental_updated_date,
            "JournalEntryDetailInvoiceItemAdjustment": incremental_updated_date,
            "JournalEntryDetailInvoicePayment": incremental_updated_date,
            "JournalEntryDetailPaymentApplication": incremental_updated_date,
            "JournalEntryDetailPaymentApplicationItem": incremental_updated_date,
            "JournalEntryDetailRefundApplication": incremental_updated_date,
            "JournalEntryDetailRefundApplicationItem": incremental_updated_date,
            "JournalEntryDetailRefundInvoicePayment": incremental_updated_date,
            "JournalEntryDetailRevenueEventItem": incremental_updated_date,
            "JournalEntryDetailTaxationItem": incremental_updated_date,
            "JournalEntryItem": incremental_updated_date,
            "JournalRun": incremental_updated_date,
            "Order": incremental_updated_date,
            "OrderAction": incremental_updated_date,
            "OrderLineItem": incremental_updated_date,
            "Payment": incremental_updated_date,
            "PaymentMethod": incremental_updated_date,
            "UpdaterDetail": incremental_updated_date,
            "PaymentRun": incremental_updated_date,
            "ProcessedUsage": incremental_updated_date,
            "Product": incremental_updated_date,
            "ProductRatePlan": incremental_updated_date,
            "ProductRatePlanCharge": incremental_updated_date,
            "ProductRatePlanChargeTier": incremental_updated_date,
            "RatePlan": incremental_updated_date,
            "RatePlanCharge": incremental_updated_date,
            "RatePlanChargeTier": incremental_updated_date,
            "Refund": incremental_updated_date,
            "RefundInvoicePayment": incremental_updated_date,
            "RefundTransactionLog": incremental_transaction_date,
            "RevenueChargeSummaryItem": incremental_updated_date,
            "RevenueEventItem": incremental_updated_date,
            "RevenueEventItemCreditMemoItem": incremental_updated_date,
            "RevenueEventItemDebitMemoItem": incremental_updated_date,
            "RevenueEventItemInvoiceItem": incremental_updated_date,
            "RevenueEventItemInvoiceItemAdjustment": incremental_updated_date,
            "RevenueScheduleItem": incremental_updated_date,
            "RevenueScheduleItemCreditMemoItem": incremental_updated_date,
            "RevenueScheduleItemDebitMemoItem": incremental_updated_date,
            "RevenueScheduleItemInvoiceItem": incremental_updated_date,
            "RevenueScheduleItemInvoiceItemAdjustment": incremental_updated_date,
            "StoredCredentialProfile": incremental_updated_date,
            "Subscription": incremental_updated_date,
            "TaxationItem": incremental_updated_date,
            "UpdaterBatch": incremental_updated_date,
            "Usage": incremental_updated_date,
            "PaymentMethodTransactionLog": incremental_transaction_date,
            "PaymentTransactionLog": incremental_transaction_date,
            "CalloutHistory": default_full,
            "EmailHistory": default_full,
            "Fulfillment": incremental_updated_date,
            "FulfillmentItem": incremental_updated_date,
            "PaymentMethodToken": incremental_updated_date,
            "DeliveryAdjustment": incremental_updated_date,
            "SubscriptionChargeDeliverySchedule": incremental_updated_date,
            "PaymentMethodPriority": incremental_updated_date,
            "GatewayProfileData": incremental_updated_date,
            "BillingPreviewRunResult": incremental_updated_date,
            "RevenueRecognitionEventsTransaction": incremental_updated_date
        }

    def rest_only_streams(self):
        """A group of streams that is only discovered when the REST API is in
        use."""
        return {
            "AchNocEventLog",
            "Account",
            "AccountingCode",
            "AccountingPeriod",
            "Amendment",
            "BillingRun",
            "BookingTransaction",
            "CommunicationProfile",
            "Contact",
            "ContactSnapshot",
            "CreditBalanceAdjustment",
            "DiscountAppliedMetrics",
            "DiscountApplyDetail",
            "DiscountClass",
            "Export",
            "PaymentGatewayReconciliationEventLog",
            "PaymentReconciliationJob",
            "PaymentReconciliationLog",
            "Import",
            "Invoice",
            "InvoiceAdjustment",
            "InvoiceItem",
            "InvoiceItemAdjustment",
            "InvoicePayment",
            "InvoiceSplit",
            "InvoiceSplitItem",
            "JournalEntry",
            "JournalEntryItem",
            "JournalRun",
            "Order",
            "OrderAction",
            "OrderLineItem",
            "Payment",
            "PaymentMethod",
            "UpdaterDetail",
            "PaymentRun",
            "ProcessedUsage",
            "Product",
            "ProductRatePlan",
            "ProductRatePlanCharge",
            "ProductRatePlanChargeTier",
            "RatePlan",
            "RatePlanCharge",
            "RatePlanChargeTier",
            "Refund",
            "RefundInvoicePayment",
            "RefundTransactionLog",
            "RevenueChargeSummaryItem",
            "RevenueEventItem",
            "RevenueEventItemCreditMemoItem",
            "RevenueEventItemDebitMemoItem",
            "RevenueEventItemInvoiceItem",
            "RevenueEventItemInvoiceItemAdjustment",
            "RevenueScheduleItem",
            "RevenueScheduleItemCreditMemoItem",
            "RevenueScheduleItemDebitMemoItem",
            "RevenueScheduleItemInvoiceItem",
            "RevenueScheduleItemInvoiceItemAdjustment",
            "Subscription",
            "TaxationItem",
            "UpdaterBatch",
            "Usage",
            "PaymentTransactionLog",
            "PaymentMethodTransactionLog",
            "CalloutHistory",
            "EmailHistory",
            "Fulfillment",
            "FulfillmentItem",
            "BillingPreviewRunResult",
            "RevenueRecognitionEventsTransaction",
        }

    def expected_streams(self):
        """A set of expected stream names."""
        streams = set(self.expected_metadata().keys())

        if self.zuora_api_type == "REST":
            return self.rest_only_streams()
        return streams

    def expected_primary_keys(self):
        """Return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def expected_replication_keys(self):
        """Return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def expected_replication_method(self):
        """Return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """Run the tap in check mode and verify it succeeds. This should be ran
        prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(
            len(found_catalogs),
            0,
            msg=f"unable to locate schemas for connection {conn_id}",
        )

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """Run a sync job and make sure it exited properly.

        Return a dictionary with keys of streams synced and values of
        records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )
        self.assertGreater(
            sum(sync_record_count.values()),
            0,
            msg=f"failed to replicate any data: {sync_record_count}",
        )

        LOGGER.info("total replicated row count: %s", sum(sync_record_count.values()))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("tap_stream_id") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info("Validating selection on %s: %s", cat["stream_name"], selected)
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    LOGGER.info(
                        "\tValidating selection on %s.%s: %s",
                        cat["stream_name"],
                        field,
                        field_selected,
                    )
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["tap_stream_id"])

                if cat["stream_name"] in self.additional_automatic_field_in_streams:
                    expected_automatic_fields.add("TransactionDate")
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        """Function to fetch the fields with inclusion available or
        automatic."""
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            if field["metadata"].get("inclusion") is None and is_field_metadata:
                LOGGER.info("Error %s has no inclusion key in metadata", field)
                continue
            inclusion_automatic_or_selected = (
                field["metadata"]["selected"] is True or field["metadata"]["inclusion"] == "automatic"
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams."""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_properties)

    def parse_date(self, date_value):
        """Pass in string-formatted-datetime, parse the value, and return it as
        an unformatted datetime object."""
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d",
        }
        for date_format in date_formats:
            try:
                date_stripped = datetime.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(f"Tests do not account for dates of this format: {date_value}")

    def timedelta_formatted(self, dtime, dt_format, days=0):
        """Checking the datetime format is as per the expectation Adding the
        lookback window days in the date given as an argument."""
        try:
            date_stripped = datetime.strptime(dtime, dt_format)
            return_date = date_stripped + timedelta(days=days)
            return datetime.strftime(return_date, dt_format)

        except ValueError:
            return Exception(f"Datetime object is not of the format: {dt_format}")

    def convert_state_to_utc(self, date_str):
        """Convert a saved bookmark value of the form
        '2020-08-25T13:17:36-07:00' to a string formatted utc datetime, in
        order to compare aginast json formatted datetime values."""
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state, expected_streams):
        """Look at the bookmarks from a previous sync and set a new bookmark
        value that is 1 day prior.

        This ensures the subsequent sync will replicate at least 1
        record but, fewer records than the previous sync.
        """
        stream_to_calculated_state = copy.deepcopy(current_state)
        timedelta_by_stream = {
            stream: [1, 0, 0] for stream in expected_streams  # {stream_name: [days, hours, minutes], ...}
        }
        timedelta_by_stream["Account"] = [0, 0, 2]

        for stream, bookmark in stream_to_calculated_state["bookmarks"].items():
            days, hours, minutes = timedelta_by_stream[stream]
            repl_key = list(self.expected_replication_keys()[stream])
            state = bookmark[repl_key[0]]

            # Convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)
            # Convert back to string and format
            calculated_state = datetime.strftime(calculated_state_as_datetime, "%Y-%m-%dT%H:%M:%S.000000Z")
            stream_to_calculated_state[stream] = calculated_state
            bookmark[repl_key[0]] = ""
            bookmark[repl_key[0]] = calculated_state

        return stream_to_calculated_state["bookmarks"]

    def is_incremental(self, stream):
        """Checking if the given stream is incremental or not."""
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL

    def create_interrupt_sync_state(self, state, interrupt_stream, pending_streams, sync_records):
        """This function will create a new interrupt sync bookmark state."""
        expected_replication_keys = self.expected_replication_keys()
        interrupted_sync_states = copy.deepcopy(state)
        bookmark_state = interrupted_sync_states["bookmarks"]
        # Set the interrupt stream as currently syncing
        interrupted_sync_states["current_stream"] = interrupt_stream

        # For pending streams, update the bookmark_value to start-date
        for stream in pending_streams:
            # Only incremental streams should have the bookmark value
            if self.is_incremental(stream):
                bookmark_state.pop(stream, None)

        if self.is_incremental(interrupt_stream):
            replication_key = next(iter(expected_replication_keys[interrupt_stream]))

            # Update state for chats stream and set the bookmark to a date earlier
            interrupted_stream_bookmark = bookmark_state.get(interrupt_stream, {})
            interrupted_stream_bookmark.pop("offset", None)
            interrupted_stream_rec = []
            for record in sync_records.get(interrupt_stream).get("messages"):
                if record.get("action") == "upsert":
                    rec = record.get("data")
                    interrupted_stream_rec.append(rec)

            # Set a deferred bookmark value for both the bookmarks of chat stream
            rec_index = len(interrupted_stream_rec) // 2 if len(interrupted_stream_rec) > 1 else 0
            interrupted_stream_bookmark[replication_key] = interrupted_stream_rec[rec_index][replication_key]

            bookmark_state[interrupt_stream] = interrupted_stream_bookmark
            interrupted_sync_states["bookmarks"] = bookmark_state
        return interrupted_sync_states
