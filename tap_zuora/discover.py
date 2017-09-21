from collections import namedtuple

from xml.etree import ElementTree

import singer
from singer.schema import Schema
from singer.catalog import (
    Catalog,
    CatalogEntry,
)

from tap_zuora.entity import Entity
from tap_zuora.state import State
from tap_zuora.streamer import get_export_payload

LOGGER = singer.get_logger()

TYPE_MAP = {
    "picklist": "string",
    "text": "string",
    "boolean": "boolean",
    "integer": "integer",
    "decimal": "number",
    "date": "date",
    "datetime": "datetime",
}

REPLICATION_KEYS = [
    "UpdatedDate",
    "TransactionDate",
    "UpdatedOn",
]

REQUIRED_KEYS = ["Id"] + REPLICATION_KEYS

CAN_BE_NULL_FIELD_PATHS = set([
    "Export.Size",
    "Import.TotalCount",
    "Import.ResultResourceUrl",
    "InvoiceItem.UOM",
    "Payment.GatewayResponse",
    "Payment.GatewayResponseCode",
    "RatePlanCharge.UOM",
])

SYNTAX_ERROR = "There is a syntax error in one of the queries in the AQuA input"
NO_DELETED_SUPPORT = ("Objects included in the queries do not support the querying of deleted "
                      "records. Remove Deleted section in the JSON request and retry the request")
FieldInfo = namedtuple('FieldInfo', ['name', 'type', 'required', 'contexts'])


def entity_available_and_deleted(client, entity_name):
    query = "select * from {} limit 1".format(entity_name)
    export_payload = get_export_payload(entity_name, "discover", query)
    resp = client.aqua_request("POST", "apps/api/batch-query/", json=export_payload).json()
    if "message" in resp:
        if resp["message"] == SYNTAX_ERROR:
            LOGGER.info("%s not available", entity_name)
            return False, False
        elif resp["message"] == NO_DELETED_SUPPORT:
            LOGGER.info("%s available, does not support deleted entries", entity_name)
            return True, False
        else:
            raise Exception("Something went wrong testing {}: {}".format(entity_name, resp["message"]))

    LOGGER.info("%s available", entity_name)
    return True, True


def get_entity_names(client):
    xml_str = client.rest_request("GET", "v1/describe").content
    etree = ElementTree.fromstring(xml_str)
    return [t.text for t in etree.findall('./object/name')]


def discover_available_entities_and_deleted(client):
    available_entities = []

    entity_names = get_entity_names(client)
    for entity_name in entity_names:
        available, deleted = entity_available_and_deleted(client, entity_name)
        if available:
            available_entities.append((entity_name, deleted))

    return available_entities


def parse_field_element(field_element):
    name = field_element.find('name').text
    return FieldInfo(
        name=name,
        type=TYPE_MAP.get(field_element.find('type').text, None),
        required=name in REQUIRED_KEYS or field_element.find('required').text.lower() == "true",
        contexts=[t.text for t in field_element.find('contexts').getchildren()],
    )


def discover_entity_definition(client, entity_name):
    xml_str = client.rest_request("GET", "v1/describe/{}".format(entity_name)).content
    etree = ElementTree.fromstring(xml_str)

    field_dict = {}
    for field_element in etree.find("fields").getchildren():
        field_info = parse_field_element(field_element)
        if field_info.type is None:
            LOGGER.debug("%s.%s has an unsupported data type", entity_name, field_info.name)
        elif "export" not in field_info.contexts:
            LOGGER.debug("%s.%s not available", entity_name, field_info.name)
        else:
            field_dict[field_info.name] = {
                "type": field_info.type,
                "required": field_info.required,
            }

    return field_dict


def convert_definition_to_schema(entity_name, definition):
    properties = {}
    for name, props in definition.items():
        field_properties = {"selected": True}

        if props["type"] in ["date", "datetime"]:
            field_properties["type"] = "string"
            field_properties["format"] = "date-time"
        else:
            field_properties["type"] = props["type"]

        path = "{}.{}".format(entity_name, name)
        if not props["required"] or path in CAN_BE_NULL_FIELD_PATHS:
            field_properties["type"] = [field_properties["type"], "null"]

        if name in REQUIRED_KEYS:
            field_properties["inclusion"] = "automatic"
        else:
            field_properties["inclusion"] = "available"

        properties[name] = field_properties

    return {
        "type": "object",
        "properties": properties,
    }


def get_replication_key(definition):
    for key in REPLICATION_KEYS:
        if key in definition:
            return key


def discover_entities(client, force_rest=False):
    catalog = Catalog([])

    for name, deleted in discover_available_entities_and_deleted(client):
        definition = discover_entity_definition(client, name)
        schema = convert_definition_to_schema(name, definition)
        if deleted and not force_rest:
            schema["properties"]["Deleted"] = {"type": "boolean"}

        catalog_entry = CatalogEntry(
            tap_stream_id=name,
            stream=name,
            key_properties=["Id"],
            schema=Schema.from_dict(schema),
            replication_key=get_replication_key(definition),
        )

        catalog.streams.append(catalog_entry)

    return catalog
