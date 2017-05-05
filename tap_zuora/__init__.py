#!/usr/bin/env python3

import time

import requests
import singer

from tap_zuora import utils


BASE_URL = "https://rest.apisandbox.zuora.com/v1"
REQUIRED_CONFIG_KEYS = ['start_date', 'api_key', 'api_secret']
CONFIG = {}
STATE = {}

LOGGER = singer.get_logger()
SESSION = requests.Session()


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

    # make sure we got a 200

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
    return resp.content


def do_sync():
    pass


def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == '__main__':
    main()
