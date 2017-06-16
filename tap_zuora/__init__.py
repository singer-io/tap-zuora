"""
singer.io Zuora tap
"""

import copy
import csv
import datetime
import io
import json
import re
import sys
import time

from xml.etree import ElementTree

import requests
import pendulum

import singer
import singer.metrics as metrics
import singer.utils


BASE_URL = "https://rest.zuora.com/v1"
BASE_SANDBOX_URL = "https://rest.apisandbox.zuora.com/v1"
LATEST_WSDL_VERSION = "84.0"
REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
REPLICATION_KEYS = ["UpdatedDate", "TransactionDate", "UpdatedOn"]
REQUIRED_KEYS = ["Id"] + REPLICATION_KEYS

MAX_EXPORT_TRIES = 3        # number of times to rety failed export before
                            # ExportFailedException
MAX_EXPORT_POLLS = 10       # number of times to poll job for completion
                            # before ExportTimedOutException
EXPORT_SLEEP_INTERVAL = 30  # sleep time between export status checks in
                            # seconds
EXPORT_DAY_RANGE = 30       # number of days to export at once

LOGGER = singer.get_logger()

NEVER_AVAILABLE_ENTITY = 'never_available_entity'
ADVANCED_AR_ENTITY = 'advanced_ar_entity'
ADVANCED_AR_DEPRECATED_ENTITY = 'advanced_ar_deprecated_entity'
CREDIT_BALANCE_ENTITY = 'credit_balance_entity'
CUSTOM_EXCHANGE_RATES_ENTITY = 'custom_exchange_rates_entity'
INVOICE_ITEM_ENTITY = 'invoice_item_entity'
ZUORA_REVENUE_ENTITY = 'zuora_revenue_entity'

def entity_type(entity):
    "Type of entity"
    return {
        # These entities aren't documented on
        # https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability
        # but the discover endpoint returns them as available. Don't ever try to get
        # these.
        "ContactSnapshot": NEVER_AVAILABLE_ENTITY,

        # These entities are only available if the Advanced AR Settlement feature is
        # enabled in Zuora.
        "ApplicationGroup": ADVANCED_AR_ENTITY,
        "CreditMemo": ADVANCED_AR_ENTITY,
        "CreditMemoApplication": ADVANCED_AR_ENTITY,
        "CreditMemoApplicationItem": ADVANCED_AR_ENTITY,
        "CreditMemoItem": ADVANCED_AR_ENTITY,
        "CreditMemoPart": ADVANCED_AR_ENTITY,
        "CreditMemoPartItem": ADVANCED_AR_ENTITY,
        "CreditTaxationItem": ADVANCED_AR_ENTITY,
        "DebitMemo": ADVANCED_AR_ENTITY,
        "DebitMemoItem": ADVANCED_AR_ENTITY,
        "DebitTaxationItem": ADVANCED_AR_ENTITY,
        "PaymentApplication": ADVANCED_AR_ENTITY,
        "PaymentPart": ADVANCED_AR_ENTITY,
        "RefundApplication": ADVANCED_AR_ENTITY,
        "RefundPart": ADVANCED_AR_ENTITY,
        "RevenueEventItemCreditMemoItem": ADVANCED_AR_ENTITY,
        "RevenueEventItemDebitMemoItem": ADVANCED_AR_ENTITY,
        "RevenueScheduleItemCreditMemoItem": ADVANCED_AR_ENTITY,
        "RevenueScheduleItemDebitMemoItem": ADVANCED_AR_ENTITY,

        # If the Advanced AR Settlement feature in Zuora is enabled, these tables are
        # deprecated.
        "CreditBalanceAdjustment": ADVANCED_AR_DEPRECATED_ENTITY,
        "InvoiceAdjustment": ADVANCED_AR_DEPRECATED_ENTITY,
        "InvoiceItemAdjustment": ADVANCED_AR_DEPRECATED_ENTITY,
        "InvoicePayment": ADVANCED_AR_DEPRECATED_ENTITY,
        "RefundInvoicePayment": ADVANCED_AR_DEPRECATED_ENTITY,

        # "CreditBalanceAdjustment": CREDIT_BALANCE_ENTITY,

        "CustomExchangeRate": CUSTOM_EXCHANGE_RATES_ENTITY,

        "PaymentApplicationItem": INVOICE_ITEM_ENTITY,
        "PaymentPartItem": INVOICE_ITEM_ENTITY,
        "RefundApplicationItem": INVOICE_ITEM_ENTITY,
        "RefundPartItem": INVOICE_ITEM_ENTITY,

        "RevenueChargeSummaryItem": ZUORA_REVENUE_ENTITY,
        "RevenueEventItem": ZUORA_REVENUE_ENTITY,
        # "RevenueEventItemCreditMemoItem": ZUORA_REVENUE_ENTITY,
        # "RevenueEventItemDebitMemoItem": ZUORA_REVENUE_ENTITY,
        "RevenueEventItemInvoiceItem": ZUORA_REVENUE_ENTITY,
        "RevenueEventItemInvoiceItemAdjustment": ZUORA_REVENUE_ENTITY,
        "RevenueScheduleItem": ZUORA_REVENUE_ENTITY,
        # "RevenueScheduleItemCreditMemoItem": ZUORA_REVENUE_ENTITY,
        # "RevenueScheduleItemDebitMemoItem": ZUORA_REVENUE_ENTITY,
        "RevenueScheduleItemInvoiceItem": ZUORA_REVENUE_ENTITY,
        "RevenueScheduleItemInvoiceItemAdjustment": ZUORA_REVENUE_ENTITY,
    }.get(entity, 'default')

TYPE_MAP = {
    "picklist": "string",
    "text": "string",
    "boolean": "boolean",
    "integer": "integer",
    "decimal": "number",
    "date": "date",
    "datetime": "datetime",
}


class ApiException(Exception):
    """
    Thrown when the API behaves unexpectedly.
    """

    def __init__(self, resp):
        super(ApiException, self).__init__("Bad API response")
        self.status_code = resp.status_code
        self.content = resp.content


class ExportTimedOutException(Exception):
    "Thrown when Zuora has not finished the export within MAX_EXPORT_POLLS"
    pass


class ExportFailedException(Exception):
    "Thrown when Zuora has reported the export as Failed."
    pass


def parse_field_element(field_element):
    """Extract name, field_type, required, and contexts from a
field_element."""
    name = field_element.find('name').text
    field_type = TYPE_MAP.get(field_element.find('type').text, None)
    required = name in REQUIRED_KEYS or field_element.find('required').text.lower() == "true"
    contexts = [t.text for t in field_element.find('contexts').getchildren()]
    return name, field_type, required, contexts


def format_zuora_datetime(datetime_str):
    "format datetime_str as the corresponding time in UTC"
    datetime_dt = pendulum.parse(datetime_str).in_timezone('utc')
    return singer.utils.strftime(datetime_dt)


def format_value(value, field_type):
    "parse value according to field_type"
    if value == "":
        return None

    if field_type == "integer":
        return int(value)

    if field_type == "number":
        return float(value)

    if field_type in ["date", "datetime"]:
        return format_zuora_datetime(value)

    if field_type == "boolean":
        return value.lower() == "true"

    return value


def parse_line(line):
    "Decode line as utf-8 and parse it as csv"
    reader = csv.reader(io.StringIO(line.decode('utf-8')))
    return next(reader)


def convert_header(header):
    "Strip leading *. off of header column name"
    _, header = header.split(".", 1)
    return header


def parse_header_line(line):
    "Strip leading *. off of each column name and return as list"
    return [convert_header(h) for h in parse_line(line)]


class ZuoraState:
    "Data bag for zuora state"

    def __init__(self, initial_state):
        self.current_entity = initial_state.get("current_entity")
        self.state = initial_state.get("bookmarks", {})

    def get_state(self, entity_name):
        "Return state of entity_name"
        return self.state.get(entity_name)

    def set_state(self, entity_name, when):
        "Set state for entity_name"
        if isinstance(when, datetime.datetime):
            when = singer.utils.strftime(when)

        if self.get_state(entity_name) is None or when > self.get_state(entity_name):
            self.state[entity_name] = when
            self.stream_state()

    def stream_state(self):
        "Write state to output stream"
        state = {
            "current_entity": self.current_entity,
            "bookmarks": self.state,
        }
        singer.write_state(state)


class ZuoraEntity:
    "Class representing Entitities returned by Zuora's describe API"

    def __init__(self, client, name, annotated_schema=None):
        self.client = client
        self.name = name
        self.annotated_schema = annotated_schema
        self._schema = None
        self._definition = None

    def _get_definition(self):
        """This will define the entity."""
        xml_str = self.client.get("/describe/{}".format(self.name)).content
        etree = ElementTree.fromstring(xml_str)

        field_dict = {}
        for field_element in etree.find('fields').getchildren():
            name, field_type, required, contexts = parse_field_element(field_element)
            if field_type is None:
                LOGGER.debug("%s.%s has an unsupported data type", self.name, name)
            elif "export" not in contexts:
                LOGGER.debug("%s.%s not available", self.name, name)
            else:
                field_dict[name] = {"type": field_type, "required": required}

        return field_dict

    @property
    def definition(self):
        "Cache and return or simply return definition for self from API"
        if not self._definition:
            self._definition = self._get_definition()

        return self._definition

    def _get_schema(self):
        """This will generate the schema for discovery mode."""
        properties = {}
        for field_name, field_dict in self.definition.items():
            field_properties = {}
            field_properties["selected"] = True

            if field_dict["type"] in ["date", "datetime"]:
                field_properties["type"] = "string"
                field_properties["format"] = "date-time"
            else:
                field_properties["type"] = field_dict["type"]

            # Zuora's API currently lies about these. If this list grows
            # any more it's probably time to figure out a different
            # solution

            # pylint: disable=too-many-boolean-expressions
            if not field_dict["required"] \
               or (self.name == 'Export' and field_name == 'Size') \
               or (self.name == 'Import' and field_name == 'TotalCount') \
               or (self.name == 'Import' and field_name == 'ResultResourceUrl') \
               or (self.name == 'InvoiceItem' and field_name == 'UOM') \
               or (self.name == 'Payment' and field_name == 'GatewayResponse') \
               or (self.name == 'Payment' and field_name == 'GatewayResponseCode') \
               or (self.name == 'RatePlanCharge' and field_name == 'UOM'):
                field_properties["type"] = [field_properties["type"], "null"]

            if field_name in REQUIRED_KEYS:
                field_properties['inclusion'] = "automatic"
            else:
                field_properties['inclusion'] = "available"

            properties[field_name] = field_properties

        return {
            "type": "object",
            "properties": properties,
        }

    @property
    def schema(self):
        "Cache and return or simply return schema for self from API"
        if not self._schema:
            self._schema = self._get_schema()

        return self._schema

    @property
    def update_field(self):
        "Returns the first key in self that is a REPLICATION_KEYS"
        for key in REPLICATION_KEYS:
            if key in self.definition:
                return key

    def get_field_query(self):
        "Return an query for self.annotated_schema"
        if not self.annotated_schema:
            return "*"

        fields = [k for k, v
                  in self.annotated_schema["properties"].items()
                  if v.get("selected", False)]
        return ", ".join(fields)

    def get_start_date(self):
        "Return the current bookmark for self or the default start_date"
        state = self.client.state.get_state(self.name)
        if state:
            return state

        return self.client.config['start_date']

    def get_where_clause(self, start_date=None, end_date=None):
        "Return an appropriate where clause for self, start_date, and end_date"
        if self.update_field and start_date and end_date:
            return ("where {update_field} >= '{start_date}' "
                    "and {update_field} < '{end_date}'").format(
                        update_field=self.update_field,
                        start_date=start_date,
                        end_date=end_date,
                    )

        return ""

    def get_zoql(self, start_date=None, end_date=None):
        "Generate a ZOQL query for self"
        return "select {fields} from {entity} {where}".format(
            fields=self.get_field_query(),
            entity=self.name,
            where=self.get_where_clause(start_date, end_date),
        )

    def get_query_data(self, start_date=None, end_date=None):
        "Get body for ZOQL query request"
        return {
            "Format": "csv",
            "Query": self.get_zoql(start_date, end_date),
        }

    def get_export(self, start_date=None, end_date=None, retry=0):
        "Create an export"
        with metrics.http_request_timer("export_create"):
            data = self.client.post("/object/export",
                                    json=self.get_query_data(
                                        start_date, end_date)).json()

        export_id = data["Id"]

        poll = 0
        failed = False
        file_id = None
        # Time a single attempt of the export, separately from the timing
        # for the whole export process, which includes possibly retrying
        # the job multiple times and downloading the CSV.
        with metrics.job_timer('export_attempt'):
            while poll < MAX_EXPORT_POLLS and not file_id and not failed:
                with metrics.http_request_timer("export_poll"):
                    poll_data = self.client.get("/object/export/{}".format(export_id)).json()

                    if poll_data['Status'] == "Completed":
                        file_id = poll_data['FileId']

                    elif poll_data['Status'] == "Failed":
                        failed = True

                    else:
                        time.sleep(EXPORT_SLEEP_INTERVAL)

                poll += 1

        # If the export timed out, we want to retry until we hit max retries
        if not file_id:
            if retry < MAX_EXPORT_TRIES:
                LOGGER.error("Export timed out. Retrying")
                return self.get_export(start_date, end_date, retry + 1)
            else:
                LOGGER.error("Export timed out %s times. Aborting", MAX_EXPORT_TRIES)
                raise ExportTimedOutException()

        # If the export failed, we want to retry until we hit max retries
        if failed:
            LOGGER.error("Export failed: %s", MAX_EXPORT_TRIES)
            raise ExportFailedException()

        # It's completed! Stream down the CSV
        return self.client.get("/files/{}".format(file_id), stream=True)

    def format_values(self, row):
        "Format every value for the row"
        data = {}
        for key, value in row.items():
            if key not in self.definition:
                # This shouldn't get hit since we're specifying fields, but jic
                continue

            data[key] = format_value(value, self.definition[key]["type"])

        return data

    def _gen_records(self, start_date=None, end_date=None):
        with metrics.job_timer('export'):
            lines = self.get_export(start_date, end_date).iter_lines()

        header_line = next(lines)

        for line in lines:
            data = parse_line(line)
            row = dict(zip(parse_header_line(header_line), data))
            row = self.format_values(row)
            yield row

    def gen_records(self):
        "Yield records from an export for self"
        if self.update_field:
            while self.get_start_date() < self.client.now_str:
                end_date = self.client.get_end_date(self.get_start_date())
                for row in self._gen_records(self.get_start_date(), end_date):
                    yield row

                self.client.state.set_state(self.name, end_date)

        else:
            for row in self._gen_records():
                yield row

    def sync(self):
        "Write records for self to stream"
        singer.write_schema(self.name, self.schema, ["Id"])
        with metrics.record_counter(self.name) as counter:
            for record in self.gen_records():
                singer.write_record(self.name, record)
                counter.increment()


def _scrub_headers(headers):
    scrubbed_headers = copy.deepcopy(headers)
    scrubbed_headers['apiAccessKeyId'] = re.sub(r'(.{3}).+(.{3})',
                                                r'\1***\2',
                                                scrubbed_headers['apiAccessKeyId'])
    scrubbed_headers['apiSecretAccessKey'] = re.sub(r'(.{3}).+(.{3})',
                                                    r'\1***\2',
                                                    scrubbed_headers['apiSecretAccessKey'])
    return scrubbed_headers


class ZuoraClient:
    "Encapsulate talking to Zuora"
    def __init__(self, state, properties, config):
        self.state = ZuoraState(state)
        # The properties arg is renamed to annotated_schemas for clarity
        self.annotated_schemas = properties
        self.config = config
        if 'features' not in self.config:
            self.config['features'] = {}

        # internal
        self._session = requests.Session()
        self._now_datetime = datetime.datetime.utcnow()

    @classmethod
    def from_args(cls, args):
        "Factory for ZuoraClient"
        return cls(args.state, args.properties, args.config)

    @property
    def base_url(self):
        "Sandbox or Real URL for self"
        if self.config.get('sandbox', False):
            return BASE_SANDBOX_URL

        return BASE_URL

    @property
    def now_str(self):
        "Returns start of run as string"
        return singer.utils.strftime(self._now_datetime)

    def request(self, method, url, **kwargs):
        "Make an api request"
        stream = kwargs.pop('stream', False)
        headers = {
            # Zuora's API requires these names (apiAccessKeyId and
            # apiSecretAccessKey), but the actual values are in fact
            # username and password.
            'apiAccessKeyId': self.config['username'],
            'apiSecretAccessKey': self.config['password'],
            'x-zuora-wsdl-version': LATEST_WSDL_VERSION,
            'Content-Type': 'application/json',
        }
        url = self.base_url + url
        req = requests.Request(method, url, headers=headers, **kwargs).prepare()
        if "json" in kwargs:
            LOGGER.info("%s: %s - %s", method, req.url, kwargs["json"])
        else:
            LOGGER.info("%s: %s", method, req.url)

        resp = self._session.send(req, stream=stream)
        if resp.status_code != 200:
            LOGGER.info("Non-200 from API: %s, %s, %s, %s, %s, %s",
                        method,
                        url,
                        _scrub_headers(headers),
                        kwargs,
                        resp.status_code,
                        resp.content)
            raise ApiException(resp)

        return resp

    def get(self, url, **kwargs):
        "Make a get request against the API"
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        "Make a post request against the API"
        return self.request('POST', url, **kwargs)

    def _has_advanced_ar_access(self):
        return self.config['features'].get("advanced_ar", False)

    def _has_credit_balance_access(self):
        return not self.config['features'].get("credit_balance", False)

    def _has_custom_exchange_rate_access(self):
        return self.config['features'].get("custom_exchange_rates", False)

    def _has_invoice_item_access(self):
        return self.config['features'].get("invoice_item", False)

    def _has_zuora_revenue_access(self):
        return self.config['features'].get("zuora_revenue", False)

    def entity_available(self, entity):
        "Configure entity's availability"
        return {
            NEVER_AVAILABLE_ENTITY: (lambda: False),
            ADVANCED_AR_ENTITY: (lambda: not self._has_advanced_ar_access()),
            ADVANCED_AR_DEPRECATED_ENTITY: self._has_advanced_ar_access,
            CREDIT_BALANCE_ENTITY: self._has_credit_balance_access,
            CUSTOM_EXCHANGE_RATES_ENTITY: self._has_custom_exchange_rate_access,
        }.get(entity_type(entity), lambda: True)()

    def get_available_entities(self):
        "Get all available entities"
        xml_str = self.get("/describe").content
        etree = ElementTree.fromstring(xml_str)
        entity_names = (t.text for t in etree.findall('./object/name')
                        if self.entity_available(t.text))

        entities = []
        selected_streams = {}

        if self.annotated_schemas and 'streams' in self.annotated_schemas:
            for s in self.annotated_schemas['streams']:
                if s['schema'].get('selected', False):
                    selected_streams[s['stream']] = s['schema']

        for entity_name in entity_names:
            if self.annotated_schemas and "streams" in self.annotated_schemas:
                if entity_name not in selected_streams:
                    continue

                annotated_schema = selected_streams[entity_name]
            else:
                annotated_schema = None

            entities.append(ZuoraEntity(self, entity_name, annotated_schema))

        return entities

    def get_end_date(self, start):
        "Get the proper end date for self"
        if isinstance(start, str):
            start = singer.utils.strptime(start)

        end_datetime = start + datetime.timedelta(days=EXPORT_DAY_RANGE)
        if end_datetime >= self._now_datetime:
            return self._now_datetime

        return singer.utils.strftime(end_datetime)

    def do_check(self):
        "Do a connection check"
        LOGGER.info("RUNNING IN CHECK MODE")
        entity = ZuoraEntity(self, "Subscription")
        entity.sync()

    def do_discover(self):
        "Do schema discovery"
        LOGGER.info("RUNNING IN DISCOVER MODE")
        streams = []
        for entity in self.get_available_entities():
            schema = entity.schema
            schema["selected"] = True
            streams.append({'stream': entity.name,
                            'tap_stream_id': entity.name,
                            'schema': schema})

        json.dump({"streams": streams}, sys.stdout, indent=4)

    def do_sync(self):
        "Do a data sync"
        LOGGER.info("RUNNING IN SYNC MODE")

        # Loop through all of the available entities
        started = False
        for entity in self.get_available_entities():
            if not started:
                if self.state.current_entity and entity.name != self.state.current_entity:
                    continue
                else:
                    started = True

            # Mark that the current entity is being processed and stream the
            # state so that it can be resumed in case of error.
            self.state.current_entity = entity.name
            self.state.stream_state()

            entity.sync()

            # Unset the current entity as it is now finished. Stream the state
            # over to mark that there is no current entity.
            self.state.current_entity = None
            self.state.stream_state()


def main():
    "Zuora's main entry point"
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = ZuoraClient.from_args(args)

    if args.discover:
        client.do_discover()
    elif args.properties:
        client.do_sync()
    else:
        client.do_check()


if __name__ == "__main__":
    main()
