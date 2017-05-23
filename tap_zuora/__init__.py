#!/usr/bin/env python3

import csv
import datetime
import io
import json
import requests
import sys
import time
from xml.etree import ElementTree

import pendulum

import singer
import singer.stats
import singer.utils


BASE_URL = "https://rest.zuora.com/v1"
BASE_SANDBOX_URL = "https://rest.apisandbox.zuora.com/v1"
LATEST_WSDL_VERSION = "84.0"
REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
REPLICATION_KEYS = ["UpdatedDate", "TransactionDate", "UpdatedOn"]
REQUIRED_KEYS = ["Id"] + REPLICATION_KEYS

MAX_EXPORT_TRIES = 3        # number of times to rety failed export before ExportFailedException
MAX_EXPORT_POLLS = 10       # number of times to poll job for completion before ExportTimedOutException
EXPORT_SLEEP_INTERVAL = 30  # sleep time between export status checks in seconds
EXPORT_DAY_RANGE = 30       # number of days to export at once

LOGGER = singer.get_logger()

# These entities aren't documented on
# https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability
# but the discover endpoint returns them as available. Don't ever try to get
# these.
NEVER_AVAILABLE_ENTITIES = [
    "ContactSnapshot",
]

# These entities are only available if the Advanced AR Settlement feature is
# enabled in Zuora.
ADVANCED_AR_ENTITIES = [
    "ApplicationGroup",
    "CreditMemo",
    "CreditMemoApplication",
    "CreditMemoApplicationItem",
    "CreditMemoItem",
    "CreditMemoPart",
    "CreditMemoPartItem",
    "CreditTaxationItem",
    "DebitMemo",
    "DebitMemoItem",
    "DebitTaxationItem",
    "PaymentApplication",
    "PaymentPart",
    "RefundApplication",
    "RefundPart",
    "RevenueEventItemCreditMemoItem",
    "RevenueEventItemDebitMemoItem",
    "RevenueScheduleItemCreditMemoItem",
    "RevenueScheduleItemDebitMemoItem",
]

# If the Advanced AR Settlement feature in Zuora is enabled, these tables are
# deprecated.
ADVANCED_AR_ENTITIES_DEPRECATED = [
    "CreditBalanceAdjustment",
    "InvoiceAdjustment",
    "InvoiceItemAdjustment",
    "InvoicePayment",
    "RefundInvoicePayment",
]

CREDIT_BALANCE_ENTITIES = [
    "CreditBalanceAdjustment",
]

CUSTOM_EXCHANGE_RATES_ENTITIES = [
    "CustomExchangeRate",
]

INVOICE_ITEM_ENTITIES = [
    "PaymentApplicationItem",
    "PaymentPartItem",
    "RefundApplicationItem",
    "RefundPartItem",
]

ZUORA_REVENUE_ENTITIES = [
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
]

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
    def __init__(self, resp):
        self.status_code = resp.status_code
        self.content = resp.content


class ExportTimedOutException(Exception):
    pass


class ExportFailedException(Exception):
    pass


def parse_field_element(field_element):
    name = field_element.find('name').text
    type = TYPE_MAP.get(field_element.find('type').text, None)
    required = name in REQUIRED_KEYS or field_element.find('required').text.lower() == "true"
    contexts = [t.text for t in field_element.find('contexts').getchildren()]
    return name, type, required, contexts


def format_zuora_datetime(datetime):
    datetime = pendulum.parse(datetime)
    datetime = datetime.in_timezone('utc')
    return singer.utils.strftime(datetime._datetime)


def format_value(value, type):
    if value == "":
        return None

    if type == "integer":
        return int(value)
    elif type == "number":
        return float(value)
    elif type in ["date", "datetime"]:
        return format_zuora_datetime(value)
    elif type == "boolean":
        return value.lower() == "true"
    else:
        return value


def convert_header(header):
    _, header = header.split(".", 1)
    return header


def parse_line(line):
    reader = csv.reader(io.StringIO(line.decode('utf-8')))
    return next(reader)


def parse_header_line(line):
    return [convert_header(h) for h in parse_line(line)]


class ZuoraState:
    def __init__(self, initial_state):
        self.current_entity = initial_state.get("current_entity")
        self.state = initial_state.get("bookmarks", {})

    def get_state(self, entity_name):
        return self.state.get(entity_name)

    def set_state(self, entity_name, when):
        if isinstance(when, datetime.datetime):
            when = singer.utils.strftime(when)

        if self.get_state(entity_name) is None or when > self.get_state(entity_name):
            self.state[entity_name] = when
            self.stream_state()

    def stream_state(self):
        state = {
            "current_entity": self.current_entity,
            "bookmarks": self.state,
        }
        singer.write_state(state)


class ZuoraEntity:
    def __init__(self, client, name, annotated_schema=None):
        self.client = client
        self.name = name
        self.annotated_schema = None
        self._schema = None
        self._definition = None

    def _get_definition(self):
        """This will define the entity."""
        xml_str = self.client.get("/describe/{}".format(self.name)).content
        et = ElementTree.fromstring(xml_str)

        field_dict = {}
        for field_element in et.find('fields').getchildren():
            name, type, required, contexts = parse_field_element(field_element)
            if type is None:
                LOGGER.debug("{}.{} has an unsupported data type".format(self.name, name))
            elif "export" not in contexts:
                LOGGER.debug("{}.{} not available".format(self.name, name))
            else:
                field_dict[name] = {"type": type, "required": required}

        return field_dict

    @property
    def definition(self):
        if not self._definition:
            self._definition = self._get_definition()

        return self._definition

    def _get_schema(self):
        """This will generate the schema for discovery mode."""
        properties = {}
        for field_name, field_dict in self.definition.items():
            d = {}
            d["selected"] = True

            if field_dict["type"] in ["date", "datetime"]:
                d["type"] = "string"
                d["format"] = "date-time"
            else:
                d["type"] = field_dict["type"]

            if not field_dict["required"]:
                d["type"] = [d["type"], "null"]

            if field_name in REQUIRED_KEYS:
                d['inclusion'] = "automatic"
            else:
                d['inclusion'] = "available"

            properties[field_name] = d

        return {
            "type": "object",
            "properties": properties,
        }

    @property
    def schema(self):
        if not self._schema:
            self._schema = self._get_schema()

        return self._schema

    @property
    def update_field(self):
        for key in REPLICATION_KEYS:
            if key in self.definition:
                return key
        else:
            return None

    def get_field_query(self):
        if not self.annotated_schema:
            return "*"
        else:
            fields = [k for k, v in self.annotated_schema["properties"].items() if v.get("selected", False)]
            return ", ".join(fields)

    def get_start_date(self):
        state = self.client.state.get_state(self.name)
        if state:
            return state
        else:
            return self.client.start_date

    def get_where_clause(self, start_date=None, end_date=None):
        if self.update_field and start_date and end_date:
            return "where {update_field} >= '{start_date}' and {update_field} < '{end_date}'".format(
                update_field=self.update_field,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            return ""

    def get_zoql(self, start_date=None, end_date=None):
        return "select {fields} from {entity} {where}".format(
            fields=self.get_field_query(),
            entity=self.name,
            where=self.get_where_clause(start_date, end_date),
        )

    def get_query_data(self, start_date=None, end_date=None):
        return {
            "Format": "csv",
            "Query": self.get_zoql(start_date, end_date),
        }

    def get_export(self, start_date=None, end_date=None, retry=0):
        with singer.stats.Timer(source="export_create") as stats:
            data = self.client.post("/object/export", json=self.get_query_data(start_date, end_date)).json()

        export_id = data["Id"]

        poll = 0
        failed = False
        file_id = None
        while poll < MAX_EXPORT_POLLS and not file_id and not failed:
            with singer.stats.Timer(source="export_poll") as stats:
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
                return self.get_export(start, end, retry + 1)
            else:
                LOGGER.error("Export timed out {} times. Aborting".format(MAX_EXPORT_TRIES))
                raise ExportTimedOutException()

        # If the export failed, we want to retry until we hit max retries
        if failed:
            LOGGER.error("Export failed".format(MAX_EXPORT_TRIES))
            raise ExportFailedException()

        # It's completed! Stream down the CSV
        return self.client.get("/files/{}".format(file_id), stream=True)

    def format_values(self, row):
        data = {}
        for k, v in row.items():
            if k not in self.definition:
                # This shouldn't get hit since we're spcifying fields, but jic
                continue

            data[k] = format_value(v, self.definition[k]["type"])

        return data

    def _gen_records(self, start_date=None, end_date=None):
        with singer.stats.Timer(source=self.name) as stats:
            lines = self.get_export(start_date, end_date).iter_lines()

            header_line = next(lines)
            stats.byte_count = len(header_line) + 2
            stats.record_count = 0

            for line in lines:
                stats.byte_count += len(line) + 2
                stats.record_count += 1
                data = parse_line(line)
                row = dict(zip(headers, data))
                row = self.format_values(row)
                yield row

    def gen_records(self):
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
        with singer.stats.Counter(source=self.name) as stats:
            for record in self.gen_records():
                singer.write_record(self.name, record)
                stats.add(record_count=1)


class ZuoraClient:
    def __init__(self, state, annotated_schemas, start_date, api_key, api_secret, sandbox=False, **features):
        self.start_date = start_date
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        self.features = features
        self.session = requests.Session()
        self.state = ZuoraState(state)
        self.annotated_schemas = annotated_schemas

        self.now_datetime = datetime.datetime.utcnow()
        self.now_str = singer.utils.strftime(self.now_datetime)

    @classmethod
    def from_args(cls, args):
        return cls(args.state, args.properties, **args.config)

    @property
    def base_url(self):
        if self.sandbox:
            return BASE_SANDBOX_URL
        else:
            return BASE_URL

    def request(self, method, url, **kwargs):
        stream = kwargs.pop('stream', False)
        headers = {
            'apiAccessKeyId': self.api_key,
            'apiSecretAccessKey': self.api_secret,
            'x-zuora-wsdl-version': LATEST_WSDL_VERSION,
            'Content-Type': 'application/json',
        }
        url = self.base_url + url
        req = requests.Request(method, url, headers=headers, **kwargs).prepare()
        if "json" in kwargs:
            LOGGER.info("{}: {} - {}".format(method, req.url, kwargs["json"]))
        else:
            LOGGER.info("{}: {}".format(method, req.url))

        resp = self.session.send(req, stream=stream)
        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def entity_available(self, entity):
        if entity in NEVER_AVAILABLE_ENTITIES:
            return False

        if entity in ADVANCED_AR_ENTITIES and not self.features.get("advanced_ar", False):
            return False

        if entity in ADVANCED_AR_ENTITIES_DEPRECATED and self.features.get("advanced_ar", False):
            return False

        if entity in CREDIT_BALANCE_ENTITIES and not self.features.get("credit_balance", False):
            return False

        if entity in CUSTOM_EXCHANGE_RATES_ENTITIES and not self.features.get("custom_exchange_rates", False):
            return False

        if entity in INVOICE_ITEM_ENTITIES and not self.features.get("invoice_item", False):
            return False

        if entity in ZUORA_REVENUE_ENTITIES and not self.features.get("zuora_revenue", False):
            return False

        return True

    def get_available_entities(self):
        xml_str = self.get("/describe").content
        et = ElementTree.fromstring(xml_str)
        entity_names = (t.text for t in et.findall('./object/name') if self.entity_available(t.text))

        entities = []
        for entity_name in entity_names:
            if self.annotated_schemas and "streams" in self.annotated_schemas:
                if entity_name not in self.annotated_schemas["streams"]:
                    continue

                if not self.annotated_schemas["streams"][entity_name].get("selected", False):
                    continue

                annotated_schema = self.annotated_schemas["streams"]
            else:
                annotated_schema = None

            entities.append(ZuoraEntity(self, entity_name, annotated_schema))

        return entities

    def get_end_date(self, start):
        if isinstance(start, str):
            start = singer.utils.strptime(start)

        end_datetime = start + datetime.timedelta(days=EXPORT_DAY_RANGE)
        if end_datetime >= self.now_datetime:
            return self.now_datetime
        else:
            return singer.utils.strftime(end_datetime)

    def do_check(self):
        LOGGER.info("RUNNING IN CHECK MODE")
        entity = ZuoraEntity(self, "Subscription")
        entity.sync()

    def do_discover(self):
        LOGGER.info("RUNNING IN DISCOVER MODE")
        streams = {}
        for entity in self.get_available_entities():
            streams[entity.name] = entity.schema
            streams[entity.name]["selected"] = True

        json.dump({"streams": streams}, sys.stdout, indent=4)

    def do_sync(self):
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
