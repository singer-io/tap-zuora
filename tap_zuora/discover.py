from xml.etree import ElementTree
import singer


from singer import metadata
from tap_zuora import apis
from tap_zuora.client import ApiException


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


LOGGER = singer.get_logger()


def parse_field_element(field_element):
    name = field_element.find("name").text
    field_type = TYPE_MAP.get(field_element.find("type").text, None)
    required = field_element.find("required").text.lower() == "true" or name in REQUIRED_KEYS
    contexts = [t.text for t in field_element.find("contexts").getchildren()]
    return {
        "name": name,
        "type": field_type,
        "required": required,
        "contexts": contexts,
    }


def get_field_dict(client, stream_name):
    endpoint = "v1/describe/{}".format(stream_name)
    xml_str = client.rest_request("GET", endpoint).content
    etree = ElementTree.fromstring(xml_str)

    field_dict = {}
    for field_element in etree.find("fields").getchildren():
        field_info = parse_field_element(field_element)
        supported = True

        if field_info["type"] is None:
            LOGGER.info("%s.%s has an unsupported data type", stream_name, field_info["name"])
            supported = False
        elif "export" not in field_info["contexts"]:
            LOGGER.info("%s.%s not available for export", stream_name, field_info["name"])
            continue

        field_dict[field_info["name"]] = {
            "type": field_info["type"],
            "required": field_info["required"],
            "supported": supported
        }

    for related_object in etree.find("related-objects").getchildren():
        related_object_name = related_object.find("name").text + ".Id"
        field_dict[related_object_name] = {
            "type": "string",
            "required": False,
            "supported": True,
            "joined": True
        }

    return field_dict


def get_replication_key(properties):
    for key in REPLICATION_KEYS:
        if key in properties:
            return key
    return None

def discover_stream_names(client):
    xml_str = client.rest_request("GET", "v1/describe").content
    etree = ElementTree.fromstring(xml_str)
    return [t.text for t in etree.findall("./object/name")]


def discover_stream(client, stream_name, force_rest): # pylint: disable=too-many-branches
    try:
        field_dict = get_field_dict(client, stream_name)
    except ApiException:
        return None

    properties = {}
    mdata = metadata.new()

    for field_name, props in field_dict.items():
        field_properties = {}

        if props.get("joined", False):
            split_field_name = field_name.split(".")
            field_name = field_name.replace(".","")
            mdata=metadata.write(mdata, ('properties', field_name), 'tap-zuora.joined_object', split_field_name[0])

        if props["type"] in ["date", "datetime"]:
            field_properties["type"] = "string"
            field_properties["format"] = "date-time"
        else:
            field_properties["type"] = props["type"]

        if props["supported"]:
            field_properties["type"] = [field_properties["type"], "null"]

        if field_name in REQUIRED_KEYS:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        elif props["supported"]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'unsupported')

        properties[field_name] = field_properties

    # Zuora sends back more entities than are actually available. We need to
    # run a sample export to test if the stream is available. If we are using
    # AQuA, we also need to see if we can use the Deleted property for that
    # stream.
    if force_rest:
        status = apis.Rest.stream_status(client, stream_name)
    else:
        status = apis.Aqua.stream_status(client, stream_name)

    # If the entity is unavailable, we need to return None
    if status == "unavailable":
        return None
    elif status == "available_with_deleted":
        properties["Deleted"] = {"type": "boolean"}
        mdata = metadata.write(mdata, ('properties', 'Deleted'), 'inclusion', 'available')

    stream = {
        "tap_stream_id": stream_name,
        "stream": stream_name,
        "key_properties": ["Id"],
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
        'metadata': metadata.to_list(mdata)
    }

    replication_key = get_replication_key(properties)
    if replication_key:
        stream["replication_key"] = replication_key
        stream["replication_method"] = "INCREMENTAL"
    else:
        stream["replication_method"] = "FULL_TABLE"

    return stream


def discover_streams(client, force_rest):
    streams = []
    failed_stream_names = []
    for stream_name in discover_stream_names(client):
        stream = discover_stream(client, stream_name, force_rest)
        if stream:
            streams.append(stream)
        else:
            failed_stream_names.append(stream_name)

    if failed_stream_names:
        LOGGER.info('Failed to discover following streams: %s', failed_stream_names)
    return streams
