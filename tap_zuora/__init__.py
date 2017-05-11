#!/usr/bin/env python3

import csv
import datetime
import io
import json
import sys
import time
from xml.etree import ElementTree

import requests
import singer
import singer.stats

from singer import utils


BASE_URL = "https://rest.apisandbox.zuora.com/v1"
REQUIRED_CONFIG_KEYS = ['start_date', 'api_key', 'api_secret']
CONFIG = {}
STATE = {}
SCHEMAS = {}
PROPERTIES = {}

LOGGER = singer.get_logger()
SESSION = requests.Session()

MAX_EXPORT_TRIES = 3        # number of times to rety failed export before ExportFailedException
MAX_EXPORT_POLLS = 10       # number of times to poll job for completion before ExportTimedOutException
EXPORT_SLEEP_INTERVAL = 30  # sleep time between export status checks
EXPORT_DAY_RANGE = 30       # number of days to export at once


class NoSuchDataSourceException(Exception):
    pass


class ExportTimedOutException(Exception):
    pass


class ExportFailedException(Exception):
    pass


def print_element(element):
    print(ElementTree.tostring(element).decode('utf-8'))


def request(method, url, **kwargs):
    stream = kwargs.pop('stream', False)
    headers = kwargs.pop('headers', {})
    headers['apiAccessKeyId'] = CONFIG['api_key']
    headers['apiSecretAccessKey'] = CONFIG['api_secret']
    headers['Content-Type'] = 'application/json'

    req = requests.Request(method, url, headers=headers, **kwargs).prepare()
    LOGGER.info("{}: {}".format(method, req.url))
    resp = SESSION.send(req, stream=stream)

    return resp


def get(url, **kwargs):
    return request('GET', url, **kwargs)


def post(url, **kwargs):
    return request('POST', url, **kwargs)


def update_field_for_entity(entity):
    if "UpdatedDate" in SCHEMAS[entity]:
        return "UpdatedDate"
    elif "TransactionDate" in SCHEMAS[entity]:
        return "TransactionDate"
    else:
        return None


def start_date_for_entity(entity):
    if entity in STATE:
        return STATE[entity]
    else:
        return CONFIG["start_date"]


def end_datetime_from_start_datetime(start_datetime):
    end_datetime = start_datetime + datetime.timedelta(days=EXPORT_DAY_RANGE)
    if end_datetime >= CONFIG['now_datetime']:
        end_datetime = CONFIG['now_datetime']

    return end_datetime


def get_where_clause(entity, start_datetime, end_datetime):
    update_field = update_field_for_entity(entity)
    if update_field is not None:
        end_date_str = utils.strftime(end_datetime)
        start_date_str = utils.strftime(start_datetime)
        return " where {update_field} >= '{start_date}' and {update_field} < '{end_date}'".format(
            update_field=update_field,
            start_date=start_date_str,
            end_date=end_date_str,
        )
    else:
        return ""


def get_export(entity, start_datetime, end_datetime, fields=None, retry=0):
    # TODO - if zuora ever fixes their API, we can query subfields until then just get them all
    # if fields:
    #     field_list = ", ".join(fields)
    # else:
    #     field_list = "*"
    field_list = "*"

    zoql = "select {field_list} from {entity}{where}".format(
        field_list=field_list,
        entity=entity,
        where=get_where_clause(entity, start_datetime, end_datetime),
    )

    data = {
        "Format": "csv",
        "Query": zoql,
    }

    with singer.stats.Timer(source="export_create") as stats:
        LOGGER.info("QUERY: {}".format(zoql))
        resp = post("{}/object/export".format(BASE_URL), json=data)

        if resp.status_code != 200:
            try:
                data = resp.json()
            except:
                raise Exception("API returned an error. status={} body={}"
                                .format(resp.status_code, resp.content))

            if 'Errors' in data:
                err = data['Errors'][0].get('Message', '')
                if "noSuchDataSource" in err:
                    raise NoSuchDataSourceException(entity)
                else:
                    raise Exception("API returned an error. status={} body={}"
                                    .format(resp.status_code, resp.content))

    export_id = resp.json()["Id"]

    # Wait a second for export to be ready
    time.sleep(1)

    # Try to download export until we exhaust retries
    for i in range(MAX_EXPORT_POLLS):
        with singer.stats.Timer(source="export_poll") as stats:
            resp = get("{}/object/export/{}".format(BASE_URL, export_id))
            d = resp.json()
            if d['Status'] == "Completed":
                file_id = d['FileId']
                break
            elif d['Status'] == "Failed":
                retry += 1
                if retry == MAX_EXPORT_TRIES:
                    raise ExportFailedException()
                else:
                    return get_export(entity, start_datetime, end_datetime, fields, retry + 1)
            else:
                LOGGER.info("Export not complete, sleeping %s seconds", EXPORT_SLEEP_INTERVAL)

        time.sleep(EXPORT_SLEEP_INTERVAL)

    else: # if for loop is exhausted without a success
        retry += 1
        if retry == MAX_EXPORT_TRIES:
            raise ExportTimedOutException()
        else:
            return get_export(entity, start_datetime, end_datetime, fields, retry + 1)

    r = get("{}/files/{}".format(BASE_URL, file_id), stream=True)
    if r.encoding is None:
        r.encoding = 'utf-8'

    return r


def format_value(value, type_):
    if value == "":
        return None

    if type_ == "integer":
        return int(value)
    elif type_ == "float":
        return float(value)
    elif type_ == "datetime":
        if "+" in value:
            value, _ = value.split("+")
            return value
        else:
            return value
    elif type_ == "date":
        return value + "T00:00:00Z"
    elif type_ == "boolean":
        return value.lower() == "true"
    else:
        return value


def format_values(entity, row):
    entity_schema = SCHEMAS[entity]
    rtn = {}
    for key, value in row.items():
        if key not in entity_schema:
            continue

        type_ = entity_schema[key]["type"]
        rtn[key] = format_value(value, type_)

    return rtn


def convert_header(key):
    _, key = key.split(".", 1)
    return key


def parse_header_line(line):
    reader = csv.reader(io.StringIO(line.decode('utf-8')))
    headers = next(reader)
    return [convert_header(h) for h in headers]


def filter_fields(row, fields):
    if fields:
        return {k: v for k, v in row.items() if k in fields}
    else:
        return row


def _gen_records(entity, start_datetime, end_datetime, fields=None):
    with singer.stats.Timer(source=entity) as stats:
        # These are None by default, so they need to be initialized to 0
        stats.record_count = 0
        stats.byte_count = 0

        # This returns a streaming response, so we need to use iter_lines()
        lines = get_export(entity, start_datetime, end_datetime, fields).iter_lines()

        # Parse the headers and track the number of bytes sent
        header_line = next(lines)
        stats.byte_count += len(header_line) + 2 # count newlines
        headers = parse_header_line(header_line)

        # Iterate through the lines and yield them
        for line in lines:
            stats.record_count += 1
            stats.byte_count += len(line) + 2 # count newlines
            reader = csv.reader(io.StringIO(line.decode('utf-8')))
            row = dict(zip(headers, next(reader)))
            row = format_values(entity, row)
            row = filter_fields(row, fields)
            yield row

        # If there's an end datetime we store that in the state and stream the state
        if end_datetime:
            STATE[entity] = utils.strftime(end_datetime)
            singer.write_state(STATE)


def gen_records(entity, fields=None):
    update_field = update_field_for_entity(entity)

    if update_field is not None:
        start_datetime = utils.strptime(start_date_for_entity(entity))
        while start_datetime < CONFIG['now_datetime']:
            end_datetime = end_datetime_from_start_datetime(start_datetime)
            for row in _gen_records(entity, start_datetime, end_datetime, fields):
                yield row

            start_datetime = end_datetime

    else:
        for row in _gen_records(entity, None, None, fields):
            yield row


def get_entities():
    xml_str = get("{}/describe".format(BASE_URL)).content
    et = ElementTree.fromstring(xml_str)
    return [t.text for t in et.findall('./object/name')]


TYPE_MAP = {
    "picklist": "string",
    "text": "string",
    "boolean": "boolean",
    "integer": "integer",
    "decimal": "float",
    "date": "date",
    "datetime": "datetime",
}


def get_field_schema(field_element):
    name = field_element.find('name').text
    type_ = TYPE_MAP.get(field_element.find('type').text, None)
    required = name == "id" or field_element.find('required').text.lower() == "true"
    contexts = [e.text for e in field_element.find('contexts').getchildren()]
    return name, type_, required, contexts


def _get_schema(entity):
    xml_str = get("{}/describe/{}".format(BASE_URL, entity)).content
    et = ElementTree.fromstring(xml_str)
    return et


def get_schema(entity):
    et = _get_schema(entity)
    fields = et.find('fields').getchildren()

    field_dict = {}
    for field in fields:
        name, type_, required, contexts = get_field_schema(field)
        if type_ is None:
            LOGGER.info("{}.{} has an unsupported field type".format(entity, name))
        elif "export" not in contexts:
            LOGGER.debug("{}.{} not available through exports".format(entity, name))
        else:
            field_dict[name] = {"type": type_, "required": required}

    return field_dict


def get_json_schema(type_, required, inclusion):
    rtn = {
        "inclusion": inclusion,
    }

    if type_ in ["date", "datetime"]:
        t = "string"
        rtn["format"] = "date-time"

    else:
        t = type_

    if not required:
        t = [t, "null"]

    rtn["type"] = t
    return rtn


def discover_field_schema(field_element):
    name, type_, required, contexts = get_field_schema(field_element)

    if "export" not in contexts:
        return None, None

    if name in ["Id", "UpdatedDate", "TransactionDate"]:
        inclusion = "automatic"
    else:
        inclusion = "available"

    return name, get_json_schema(type_, required, inclusion)


def discover_schema(entity):
    et = _get_schema(entity)
    fields = et.find('fields').getchildren()

    properties = {}
    for field_entity in fields:
        name, schema = discover_field_schema(field_entity)
        if not name:
            continue

        properties[name] = schema

    schema = {
        "type": "object",
        "properties": properties,
    }

    return schema


def discover_schemas():
    entities = get_entities()
    schemas = {}
    for entity in entities:
        schemas[entity] = discover_schema(entity)

    return schemas


def sync_entity(entity, fields=None):
    LOGGER.info("SYNC: {}".format(entity))
    SCHEMAS[entity] = get_schema(entity)
    try:
        with singer.stats.Counter(source=entity) as stats:
            for record in gen_records(entity, fields):
                singer.write_record(entity, record)
                stats.add(record_count=1)

    except NoSuchDataSourceException:
        # The "discover" endpoint listed this entity as available
        # but the API reported that the data source does not exist
        # Skip this entity and move to the next
        LOGGER.info("{} not available".format(entity))

    except ExportFailedException:
        # We've tried to get this export multiple times now and each time it's
        # failed. Move on to the next one but log the error
        LOGGER.error("{} export exceeded max retries".format(entity))

    except ExportTimedOutException:
        # One of the exports for this endpoint timed out
        # Move onto the next one but log the error occured
        LOGGER.error("{} export timed out".format(entity))


def do_discover():
    schemas = discover_schemas()
    for stream, schema in schemas.items():
        schema["selected"] = True
        for field, field_schema in schema["properties"].items():
            field_schema["selected"] = True

    json.dump({"streams": schemas}, sys.stdout, indent=4)


def do_sync():
    for entity in get_entities():
        entity_properties = PROPERTIES["streams"].get(entity, {})
        field_properties = entity_properties.get("properties", {})

        if entity_properties.get("selected", False):
            fields = [k for k, v in field_properties.items() if v.get("selected", False)]
            sync_entity(entity, fields)
        else:
            LOGGER.info("{} is not selected".format(entity))


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    STATE.update(args.state)

    # Cache now for use later
    CONFIG['now_datetime'] = datetime.datetime.utcnow()

    if args.discover:
        # When running in discover mode we just report the schemas from the API
        LOGGER.info("RUNNING IN DISCOVER MODE")
        do_discover()

    elif args.properties:
        # When properties is set, we have all we need to run the tap
        # Use properties to determine the tables and fields to sync
        LOGGER.info("RUNNING IN SYNC MODE")
        PROPERTIES.update(args.properties)
        do_sync()

    else:
        # If no properties are set we must assume we're in check mode
        # Check mode just syncs the Subscription endpoint and will short-circuit
        # after a single record is emitted
        LOGGER.info("RUNNING IN CHECK MODE")
        sync_entity("Subscription")
