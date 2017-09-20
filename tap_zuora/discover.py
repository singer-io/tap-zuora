from xml.etree import ElementTree

import singer
from singer import catalog

from tap_zuora.streamer import RestStreamer

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


FieldInfo = namedtuple('FieldInfo', ['name', 'type', 'required', 'contexts'])


def entity_available(client, entity_name):
    streamer = RestStreamer(None, client, None)
    query = "select Id from {}".format(entity_name)

    # TODO: should blow up here somehow
    job_id = streamer.post_job(query)

    return True


def get_entity_names(client):
    xml_str = client.rest_request("GET", "v1/describe").content
    etree = ElementTree.fromstring(xml_str)
    return [t.text for t in etree.findall('./object/name')]


def discover_available_entities(client):
    return [e for e in get_entity_names(client) if entity_available(client, e)]


def parse_field_element(field_element):
    return FieldInfo(
        name=field_element.find('name').text,
        type=TYPE_MAP.get(field_element.find('type').text, None),
        required=name in REQUIRED_KEYS or field_element.find('required').text.lower() == "true",
        contexts=[t.text for t in field_element.find('contexts').getchildren()],
    )


def discover_entity_definition(client, entity_name):
    xml_str = client.rest_request("GET", "describe/{}".format(entity_name)).content
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
        field_properties["selected"] = True

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


def discover_entities(client):
    catalog = Catalog()

    for name in discover_available_entities(client):
        definition = discover_entity_definition(client, name)

        catalog_entry = CatalogEntry(
            tap_stream_id=name,
            stream=name,
            key_properties=["Id"],
            schema=convert_definition_to_schema(name, definition),
            replication_key=get_replication_key(definition),
        )

        catalog.streams.append(catalog_entry)

    return catalog
