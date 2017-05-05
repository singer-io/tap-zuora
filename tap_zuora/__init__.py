#!/usr/bin/env python3

import csv
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


def get_export(entity, fields=None):
    if fields:
        field_list = ", ".join(fields)
    else:
        field_list = "*"

    zoql = "select {field_list} from {entity}".format(
        field_list=field_list,
        entity=entity)

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
        # nothing to do - we already have the correct dt format
        return value
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

        type_ = entity_schema[key]
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
    "date": "datetime",
    "datetime": "datetime",
}


def get_field_schema(field_element):
    # print_element(field_element)
    name = field_element.find('name').text.lower()
    type_ = TYPE_MAP.get(field_element.find('type').text, None)
    return name, type_


def get_schema(entity):
    # print(entity)
    xml_str = get("{}/describe/{}".format(BASE_URL, entity)).content
    et = ElementTree.fromstring(xml_str)
    fields = et.find('fields').getchildren()

    field_dict = {}
    for field in fields:
        name, type_ = get_field_schema(field)
        if type_ is None:
            # TODO: log something
            pass
        else:
            field_dict[name] = type_

    return field_dict


def get_schemas():
    entities = get_entities()
    schemas = {}
    for entity in entities:
        schemas[entity] = {
            "fields": get_schema(entity),
        }

    return schemas


def do_sync():
    SCHEMAS.update(get_schemas())


def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == '__main__':
    main()
