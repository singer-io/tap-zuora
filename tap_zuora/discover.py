from typing import Dict, KeysView, List, Union
from xml.etree import ElementTree

import singer
from singer import metadata

from tap_zuora import apis
from tap_zuora.client import Client
from tap_zuora.exceptions import ApiException

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

UNSUPPORTED_FIELDS_FOR_REST = {
    "Account": ["SequenceSetId"],
    "Amendment": [
        "BookingDate",
        "EffectivePolicy",
        "NewRatePlanId",
        "RemovedRatePlanId",
        "SubType",
    ],
    "BillingRun": ["BillingRunType", "NumberOfCreditMemos", "PostedDate"],
    "Export": ["Encoding"],
    "Invoice": ["PaymentTerm", "SourceType", "TaxMessage", "TaxStatus", "TemplateId"],
    "InvoiceItem": ["Balance", "ExcludeItemBillingFromRevenueAccounting"],
    "InvoiceItemAdjustment": ["ExcludeItemBillingFromRevenueAccounting"],
    "PaymentMethod": ["StoredCredentialProfileId"],
    "ProductRatePlanCharge": [
        "ExcludeItemBillingFromRevenueAccounting",
        "ExcludeItemBookingFromRevenueAccounting",
    ],
    "RatePlanCharge": [
        "AmendedByOrderOn",
        "CreditOption",
        "DrawdownRate",
        "DrawdownUom",
        "ExcludeItemBillingFromRevenueAccounting",
        "ExcludeItemBookingFromRevenueAccounting",
        "IsPrepaid",
        "OriginalOrderDate",
        "PaymentTermSnapshot",
        "PrepaidOperationType",
        "PrepaidQuantity",
        "PrepaidTotalQuantity",
        "PrepaidUom",
        "ValidityPeriodType",
    ],
    "Subscription": ["IsLatestVersion", "LastBookingDate", "PaymentTerm", "Revision"],
    "TaxationItem": ["Balance", "CreditAmount", "PaymentAmount"],
    "Usage": ["ImportId"],
}

REQUIRED_KEYS = ["Id"] + REPLICATION_KEYS

LOGGER = singer.get_logger()


def parse_field_element(field_element):
    name = field_element.find("name").text
    field_type = TYPE_MAP.get(field_element.find("type").text, None)
    required = field_element.find("required").text.lower() == "true" or name in REQUIRED_KEYS
    contexts = [t.text for t in list(field_element.find("contexts"))]
    return {
        "name": name,
        "type": field_type,
        "required": required,
        "contexts": contexts,
    }


def get_field_dict(client: Client, stream_name: str) -> Dict:
    endpoint = f"v1/describe/{stream_name}"
    xml_str = client.rest_request("GET", endpoint).content
    etree = ElementTree.fromstring(xml_str)

    field_dict = {}
    for field_element in list(etree.find("fields")):
        field_info = parse_field_element(field_element)
        supported = True

        # Make the field unsupported if type is None
        if field_info["type"] is None:
            LOGGER.info(f"{stream_name}.{field_info['name']} has an unsupported data type")
            supported = False

        # Skip the stream from discovery if the required field is not exportable
        if "export" not in field_info["contexts"] and field_info["name"] in REQUIRED_KEYS:
            LOGGER.info(
                f"Skipping stream {stream_name} since required field {field_info['name']}" f" not available for export"
            )
            field_dict = {}
            break

        # Skip the non-required field if is not exportable
        if "export" not in field_info["contexts"]:
            LOGGER.info(f"{stream_name}.{field_info['name']} is not available for export")
            continue

        field_dict[field_info["name"]] = {
            "type": field_info["type"],
            "required": field_info["required"],
            "supported": supported,
        }

    for related_object in list(etree.find("related-objects")):
        related_object_name = related_object.find("name").text + ".Id"
        field_dict[related_object_name] = {
            "type": "string",
            "required": False,
            "supported": True,
            "joined": True,
        }

    return field_dict


def get_replication_key(properties: KeysView) -> Union[str, None]:
    return next((key for key in REPLICATION_KEYS if key in properties), None)


def discover_stream_names(client: Client):
    xml_str = client.rest_request("GET", "v1/describe").content
    etree = ElementTree.fromstring(xml_str)
    return [t.text for t in etree.findall("./object/name")]


def is_unsupported_field(stream_name: str, field_name: str, is_rest: bool) -> bool:
    """Checks whether a given field for a given stream is supported, applicable
    only for REST api calls."""
    unsupported_fields = UNSUPPORTED_FIELDS_FOR_REST.get(stream_name, [])
    return bool(unsupported_fields and is_rest and field_name in unsupported_fields)


def discover_stream(client: Client, stream_name: str) -> Union[Dict, None]:
    try:
        field_dict = get_field_dict(client, stream_name)
    except ApiException:
        return None

    if not field_dict:
        return None

    properties = {}

    replication_key = get_replication_key(field_dict.keys())
    replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"

    # adds empty breadcrumb for selecting stream in catalog file
    mdata = metadata.get_standard_metadata(
        key_properties=["Id"],
        valid_replication_keys=[replication_key] if replication_key else None,
        replication_method=replication_method,
    )
    mdata = metadata.write(metadata.to_map(mdata), (), "inclusion", "available")

    for field_name, props in field_dict.items():
        field_properties = {}

        if props.get("joined", False):
            split_field_name = field_name.split(".")
            field_name = field_name.replace(".", "")
            mdata = metadata.write(
                mdata,
                ("properties", field_name),
                "tap-zuora.joined_object",
                split_field_name[0],
            )

        if props["type"] in ["date", "datetime"]:
            field_properties["type"] = "string"
            field_properties["format"] = "date-time"
        else:
            field_properties["type"] = props["type"]

        if props["supported"]:
            field_properties["type"] = [field_properties["type"], "null"]

        if field_name in REQUIRED_KEYS:
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")
        elif props["supported"] and not is_unsupported_field(stream_name, field_name, client.is_rest):
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "available")
        else:
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "unsupported")

        properties[field_name] = field_properties

    # Zuora sends back more entities than are actually available. We need to
    # run a sample export to test if the stream is available. If we are using
    # AQuA, we also need to see if we can use the Deleted property for that
    # stream.
    if client.is_rest:
        status = apis.Rest.stream_status(client, stream_name)
    else:
        status = apis.Aqua.stream_status(client, stream_name)

    # If the entity is unavailable, we need to return None
    if status == "unavailable":
        LOGGER.info(f"Stream {stream_name} is unavailable to export")
        return None
    elif status == "available_with_deleted":
        properties["Deleted"] = {"type": "boolean"}
        mdata = metadata.write(mdata, ("properties", "Deleted"), "inclusion", "available")

    return {
        "tap_stream_id": stream_name,
        "stream": stream_name,
        "key_properties": ["Id"],
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
        "metadata": metadata.to_list(mdata),
        "replication_key": replication_key,
        "replication_method": replication_method,
    }


def discover_streams(client: Client) -> List:
    """Performs discovery for each stream."""
    streams = []
    failed_stream_names = []
    for stream_name in discover_stream_names(client):

        if stream := discover_stream(client, stream_name):
            streams.append(stream)
        else:
            failed_stream_names.append(stream_name)
    if failed_stream_names:
        LOGGER.info(f"Failed to discover following streams: {failed_stream_names}")
    return streams
