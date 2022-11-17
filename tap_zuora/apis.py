from typing import Dict, List, Union

import pendulum
import singer
from singer import metadata

from tap_zuora.client import Client
from tap_zuora.exceptions import ApiException
from tap_zuora.utils import make_aqua_payload

MAX_EXPORT_DAYS = 30
SYNTAX_ERROR = "There is a syntax error in one of the queries in the AQuA input"
NO_DELETED_SUPPORT = (
    "Objects included in the queries do not support the querying of deleted "
    "records. Remove Deleted section in the JSON request and retry the request"
)

LOGGER = singer.get_logger()


def selected_fields(stream: Dict) -> List:
    mdata = metadata.to_map(stream["metadata"])
    fields = [
        f
        for f, s in stream["schema"]["properties"].items()
        if (
            metadata.get(mdata, ("properties", f), "selected")
            or metadata.get(mdata, ("properties", f), "inclusion") == "automatic"
        )
        and metadata.get(mdata, ("properties", f), "inclusion") != "unsupported"
    ]

    # Remove Deleted from the query if its selected
    if "Deleted" in fields:
        fields.remove("Deleted")
    return fields


def joined_fields(fields: List, stream: Dict) -> List:
    mdata = metadata.to_map(stream["metadata"])
    joined_fields_list = []
    for field_name in fields:
        if joined_obj := metadata.get(mdata, ("properties", field_name), "tap-zuora.joined_object"):
            joined_fields_list.append(f"{joined_obj}." + field_name.replace(joined_obj, ""))

        else:
            joined_fields_list.append(field_name)
    return joined_fields_list


def format_datetime_zoql(datetime_str: str, date_format: str):
    return pendulum.parse(datetime_str, tz=pendulum.timezone("UTC")).strftime(date_format)


class ExportFailed(Exception):
    pass


class ExportTimedOut(ExportFailed):
    def __init__(self, timeout: int, unit: str):
        super().__init__(f"Export failed (TimedOut): The job took longer than {timeout} {unit}")


class Aqua:
    ZOQL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    # Specifying incrementalTime requires this format, but ZOQL requires the 'T'
    PARAMETER_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Zuora's documentation describes some objects which are not supported for deleted
    # See https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
    # and https://github.com/singer-io/tap-zuora/pull/8 for more info.
    DOES_NOT_SUPPORT_DELETED = [
        "AccountingPeriod",
        "ContactSnapshot",
        "DiscountAppliedMetrics",
        "PaymentGatewayReconciliationEventLog",
        "PaymentTransactionLog",
        "PaymentMethodTransactionLog",
        "PaymentReconciliationJob",
        "PaymentReconciliationLog",
        "ProcessedUsage",
        "RefundTransactionLog",
        "UpdaterBatch",
        "UpdaterDetail",
        "BookingTransaction",
        "CalloutHistory",
        "SmartPreventionAudit",
        "HpmCaptchaValidationResult",
        "EmailHistory",
    ]

    @staticmethod
    def deleted_records_available(stream: Dict) -> Union[str, bool]:
        if stream["tap_stream_id"] in Aqua.DOES_NOT_SUPPORT_DELETED:
            LOGGER.info(
                f"Deleted fields are not supported for stream - {stream['tap_stream_id']}."
                f" Not selecting deleted records."
            )
            return False

        mdata = metadata.to_map(stream["metadata"])
        return "Deleted" in stream["schema"]["properties"] and metadata.get(
            mdata, ("properties", "Deleted"), "selected"
        )

    @staticmethod
    def get_query(stream: Dict) -> str:
        selected_field_names = selected_fields(stream)
        dotted_field_names = joined_fields(selected_field_names, stream)
        fields = ", ".join(dotted_field_names)
        query = f'select {fields} from {stream["tap_stream_id"]}'
        if replication_key := stream.get("replication_key"):
            query += f" order by {replication_key} asc"

        LOGGER.info(f"Executing query: {query}")
        return query

    @staticmethod
    def get_payload(state: Dict, stream: Dict, partner_id: str) -> Dict:
        stream_name = stream["tap_stream_id"]
        version = state["bookmarks"][stream["tap_stream_id"]].get("version")
        project = f"{stream_name}_{version}"
        query = Aqua.get_query(stream)
        deleted = Aqua.deleted_records_available(stream)
        payload = make_aqua_payload(project, query, partner_id, deleted)

        if stream.get("replication_key"):
            # Incremental time must be in Pacific time
            # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/e_Post_Query_with_Retrieval_Time#Request_Parameters
            start_date = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
            inc_pen = pendulum.parse(start_date)
            inc_pen = inc_pen.astimezone(pendulum.timezone("US/Pacific"))
            payload["incrementalTime"] = inc_pen.strftime(Aqua.PARAMETER_DATE_FORMAT)

        return payload

    @staticmethod
    def create_job(client: Client, state: Dict, stream: Dict) -> str:
        endpoint = "v1/batch-query/"
        # This _always_ submits with an incremental_time which I think
        # means that we're never executing a full export which means we
        # can't establish a baseline to report deletes on.
        # https://stitchdata.atlassian.net/browse/SRCE-322
        payload = Aqua.get_payload(state, stream, client.partner_id)
        # Log to show whether the aqua request should trigger a full or
        # incremental response based on
        # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
        payload_content = {k: v for k, v in payload.items() if k in {"partner", "project", "incrementalTime"}}
        LOGGER.info(f"Submitting aqua request with {payload_content}")
        resp = client.aqua_request("POST", endpoint, json=payload).json()
        # Log to show whether the aqua response is in full or incremental
        # mode based on
        # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
        if "batches" in resp:
            LOGGER.info(f"Received aqua response with batch fulls={[x.get('full', None) for x in resp['batches']]}")
        else:
            LOGGER.info("Received aqua response with no batches")
        if "message" in resp:
            raise ExportFailed(resp["message"])

        return resp["id"]

    @staticmethod
    def stream_status(client: Client, stream_name: str) -> str:
        """Check if the provided Zuora object (stream_name) can be queried via
        AQuA API by issuing a small export job of 1 row. This job must be
        cleaned up after submission to limit concurrent jobs during discovery.

        The response from submitting the job indicates whether or not
        the object is available.
        """
        endpoint = "v1/batch-query/"
        query = f"select * from {stream_name} limit 1"
        payload = make_aqua_payload("discover", query, client.partner_id)
        resp = client.aqua_request("POST", endpoint, json=payload).json()

        # Cancel this job to keep concurrency low.
        client.aqua_request("DELETE", f"v1/batch-query/jobs/{resp['id']}")
        if "message" in resp:
            if resp["message"] == SYNTAX_ERROR:
                return "unavailable"
            elif resp["message"] == NO_DELETED_SUPPORT:
                return "available"
            else:
                raise Exception(f'Error probing {stream_name}: {resp["message"]}')

        return "available_with_deleted"

    # Must match call signature of other APIs
    @staticmethod
    def job_ready(client: Client, job_id: str) -> bool:
        endpoint = f"v1/batch-query/jobs/{job_id}"
        data = client.aqua_request("GET", endpoint).json()
        if data["status"] == "completed":
            return True
        elif data["status"] == "failed":
            raise ExportFailed(data["batches"][0]["message"])
        else:
            return False

    # Must match call signature of other APIs
    @staticmethod
    def get_file_ids(client: Client, job_id: str) -> List:
        endpoint = f"v1/batch-query/jobs/{job_id}"
        data = client.aqua_request("GET", endpoint).json()
        if "segments" in data["batches"][0]:
            return data["batches"][0]["segments"]
        return [data["batches"][0]["fileId"]]

    # Must match call signature of other APIs
    @staticmethod
    def stream_file(client: Client, file_id: str):
        endpoint = f"v1/file/{file_id}"
        return client.aqua_request("GET", endpoint, stream=True).iter_lines()


class Rest:
    ZOQL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    @staticmethod
    def make_payload(query: str) -> Dict:
        return {
            "Format": "csv",
            "Query": query,
        }

    @staticmethod
    def get_query(stream: Dict, start_date: Union[str, None], end_date: Union[str, None]) -> str:
        selected_field_names = selected_fields(stream)
        dotted_field_names = joined_fields(selected_field_names, stream)
        fields = ", ".join(dotted_field_names)
        query = f'select {fields} from {stream["tap_stream_id"]}'

        if stream.get("replication_key") and start_date and end_date:
            start_date = format_datetime_zoql(start_date, Rest.ZOQL_DATE_FORMAT)
            end_date = format_datetime_zoql(end_date, Rest.ZOQL_DATE_FORMAT)
            query += f""" where {stream["replication_key"]} >= '{start_date}'"""
            query += f""" and {stream["replication_key"]} < '{end_date}'"""

        LOGGER.info(f"Executing query: {query}")
        return query

    @staticmethod
    def get_payload(stream: Dict, start_date: Union[str, None], end_date: Union[str, None]) -> Dict:
        query = Rest.get_query(stream, start_date, end_date)
        return Rest.make_payload(query)

    @staticmethod
    def create_job(
        client: Client,
        stream: Dict,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
    ) -> str:
        endpoint = "v1/object/export"
        payload = Rest.get_payload(stream, start_date, end_date)
        resp = client.rest_request("POST", endpoint, json=payload).json()
        return resp["Id"]

    # Must match call signature of other APIs
    @staticmethod
    def job_ready(client: Client, job_id: str) -> bool:
        endpoint = f"v1/object/export/{job_id}"
        data = client.rest_request("GET", endpoint).json()
        if data["Status"] == "Completed":
            return True
        elif data["Status"] in ["Cancelled", "Failed"]:
            raise ExportFailed(data["StatusReason"])
        else:
            return False

    # Must match call signature of other APIs
    @staticmethod
    def get_file_ids(client: Client, job_id: str) -> List:
        endpoint = f"v1/object/export/{job_id}"
        data = client.rest_request("GET", endpoint).json()
        return [data["FileId"]]

    # Must match call signature of other APIs
    @staticmethod
    def stream_file(client: Client, file_id: str):
        endpoint = f"v1/files/{file_id}"
        return client.rest_request("GET", endpoint, stream=True).iter_lines()

    @staticmethod
    def stream_status(client: Client, stream_name: str) -> str:
        endpoint = "v1/object/export"
        query = f"select * from {stream_name} limit 1"
        payload = {"Query": query, "Format": "csv"}

        try:
            resp = client.rest_request("POST", endpoint, json=payload).json()
        except ApiException:
            LOGGER.info(f"Error probing status for stream {stream_name}, assuming unavailable")
            return "unavailable"

        return "available" if resp["Success"] else "unavailable"
