#!/usr/bin/env python3

import csv
import datetime
import io
import time
from xml.etree import ElementTree

import requests
import singer

from tap_zuora import utils

BASE_URL = "https://rest.apisandbox.zuora.com/v1"
REQUIRED_CONFIG_KEYS = ['start_date', 'api_key', 'api_secret']
CONFIG = {}
STATE = {}
SCHEMAS = {}

LOGGER = singer.get_logger()
SESSION = requests.Session()


def print_element(element):
    print(ElementTree.tostring(element).decode('utf-8'))


def request(method, url, **kwargs):
    headers = kwargs.pop('headers', {})
    headers['apiAccessKeyId'] = CONFIG['api_key']
    headers['apiSecretAccessKey'] = CONFIG['api_secret']
    headers['Content-Type'] = 'application/json'

    req = requests.Request(method, url, headers=headers, **kwargs).prepare()
    LOGGER.info("{} {}".format(method, req.url))
    resp = SESSION.send(req)

    return resp


def get(url, **kwargs):
    return request('GET', url, **kwargs)


def post(url, **kwargs):
    return request('POST', url, **kwargs)


def update_field_for_entity(entity):
    if "updateddate" in SCHEMAS[entity]["fields"]:
        return "UpdatedDate"
    elif "transactiondate" in SCHEMAS[entity]["fields"]:
        return "TransactionDate"
    else:
        return None


def start_date_for_entity(entity):
    if entity in STATE:
        return STATE[entity]
    else:
        return CONFIG["start_date"]

def get_where_clause(entity):
    update_field = update_field_for_entity(entity)
    if update_field is not None:
        start_date = start_date_for_entity(entity)
        end_date = utils.strftime(utils.strptime(start_date) + datetime.timedelta(days=1))
        return ' where {update_field} >= "{start_date}" and {update_field} < "{end_date}"'.format(
            update_field=update_field,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        return ""

def get_export(entity, fields=None):
    if fields:
        field_list = ", ".join(fields)
    else:
        field_list = "*"

    # need the where clause here
    zoql = "select {field_list} from {entity}{where}".format(
        field_list=field_list,
        entity=entity,
        where=get_where_clause(entity),
    )

    data = {
        "Format": "csv",
        "Query": zoql,
    }

    LOGGER.info("Query: {}".format(zoql))
    resp = post("{}/object/export".format(BASE_URL), json=data)

    if resp.status_code != 200:
        print(resp.status_code)
        print(resp.content)
        raise Exception("API got mad")

    export_id = resp.json()["Id"]

    for i in range(10):
        resp = get("{}/object/export/{}".format(BASE_URL, export_id))
        d = resp.json()
        if d['Status'] == "Completed":
            file_id = d['FileId']
            break
        time.sleep(30)
    else:
        raise Exception("Export didn't complete")

    resp = get("{}/files/{}".format(BASE_URL, file_id))
    c = resp.content.decode('utf-8')
    f = io.StringIO(c)
    return csv.DictReader(f)


def convert_key(key):
    _, key = key.split(".", 1)
    return key.lower()


def convert_keys(row):
    return {convert_key(k): v for k, v in row.items()}


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
    entity_schema = SCHEMAS[entity]['fields']

    rtn = {}
    for key, value in row.items():
        if key not in entity_schema:
            continue

        type_ = entity_schema[key]["type"]
        rtn[key] = format_value(value, type_)

    return rtn


def gen_records(entity, fields=None):
    reader = get_export(entity, fields)
    for row in reader:
        row = convert_keys(row)
        row = format_values(entity, row)
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
    # print_element(field_element)
    name = field_element.find('name').text.lower()
    type_ = TYPE_MAP.get(field_element.find('type').text, None)
    required = name == "id" or field_element.find('required').text.lower() == "true"
    return name, type_, required


def get_schema(entity):
    # print(entity)
    xml_str = get("{}/describe/{}".format(BASE_URL, entity)).content
    et = ElementTree.fromstring(xml_str)
    fields = et.find('fields').getchildren()

    field_dict = {}
    for field in fields:
        name, type_, required = get_field_schema(field)
        if type_ is None:
            # TODO: log something
            pass
        else:
            field_dict[name] = {"type": type_, "required": required}

    return field_dict


def get_schemas():
    entities = get_entities()
    schemas = {}
    for entity in entities:
        schemas[entity] = {
            "fields": get_schema(entity),
        }

    return schemas


def get_json_schema(schema):
    print(schema)
    rtn = {}

    if schema["type"] in ["date", "datetime"]:
        type_ = "string"
        rtn["format"] = "date-time"
    else:
        type_ = schema["type"]

    if not schema["required"]:
        type_ = [type_, "null"]

    rtn["type"] = type_
    return rtn


def schema_to_json_schema(schema):
    properties = {k: get_json_schema(v) for k, v in schema.items()}
    return {
        "type": "object",
        "properties": properties,
    }


def sync_entity(entity):
    # TODO- Need to time-gate

    singer.write_schema(entity, schema_to_json_schema(SCHEMAS[entity]["fields"]), ["id"])
    for record in gen_records(entity):
        singer.write_record(entity, record)

    # TODO- Update state


def do_sync():
    SCHEMAS.update(get_schemas())
    for entity in SCHEMAS.keys():
        sync_entity(entity)


def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == '__main__':
    main()
