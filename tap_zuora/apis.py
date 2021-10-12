import pendulum
import singer
from singer import metadata


MAX_EXPORT_DAYS = 30
SYNTAX_ERROR = "There is a syntax error in one of the queries in the AQuA input"
NO_DELETED_SUPPORT = ("Objects included in the queries do not support the querying of deleted "
                      "records. Remove Deleted section in the JSON request and retry the request")

LOGGER = singer.get_logger()

def selected_fields(stream):
    mdata = metadata.to_map(stream['metadata'])
    fields = [f for f, s in stream["schema"]["properties"].items()
              if metadata.get(mdata, ('properties', f), 'selected')
              or metadata.get(mdata, ('properties', f), 'inclusion') == 'automatic']

    # Remove Deleted from the query if its selected
    if 'Deleted' in fields:
        fields.remove('Deleted')
    return fields

def joined_fields(fields, stream):
    mdata = metadata.to_map(stream['metadata'])
    joined_fields_list = []
    for field_name in fields:
        joined_obj = metadata.get(mdata, ('properties', field_name), 'tap-zuora.joined_object')
        if joined_obj:
            joined_fields_list.append(joined_obj + '.' + field_name.replace(joined_obj, ""))
        else:
            joined_fields_list.append(field_name)
    return joined_fields_list

def format_datetime_zoql(datetime_str, date_format):
    return pendulum.parse(datetime_str, tz=pendulum.timezone("UTC")).strftime(date_format)


class ExportFailed(Exception):
    pass

class ExportTimedOut(ExportFailed):
    def __init__(self, timeout, unit):
        super().__init__("Export failed (TimedOut): The job took longer than {} {}".format(timeout, unit))

class Aqua:
    ZOQL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    # Specifying incrementalTime requires this format, but ZOQL requires the 'T'
    PARAMETER_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Zuora's documentation describes some objects which are not supported for deleted
    # See https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
    # and https://github.com/singer-io/tap-zuora/pull/8 for more info.
    DOES_NOT_SUPPORT_DELETED = [
        'AccountingPeriod',
        'ContactSnapshot',
        'DiscountAppliedMetrics',
        'PaymentGatewayReconciliationEventLog',
        'PaymentTransactionLog',
        'PaymentMethodTransactionLog',
        'PaymentReconciliationJob',
        'PaymentReconciliationLog',
        'ProcessedUsage',
        'RefundTransactionLog',
        'UpdaterBatch',
        'UpdaterDetail'
    ]

    @staticmethod
    def make_payload(stream_name, project, query, partner_id, deleted=False):
        # NB - 4/5/19 - Were told by zuora support to use the same value
        # for both project and name to imply an incremental export
        rtn = {
            "name": project,
            "partner": partner_id,
            "project": project,
            "format": "csv",
            "version": "1.2",
            "encrypted": "none",
            "useQueryLabels": "true",
            "dateTimeUtc": "true",
            "queries": [
                {
                    "name": project,
                    "query": query,
                    "type": "zoqlexport",
                },
            ],
        }

        if deleted:
            rtn["queries"][0]["deleted"] = {"column": "Deleted", "format": "Boolean"}

        return rtn

    @staticmethod
    def deleted_records_available(stream):
        if stream['tap_stream_id'] in Aqua.DOES_NOT_SUPPORT_DELETED:
            LOGGER.info("Deleted fields are not supported for stream - %s. Not selecting deleted records.", stream['tap_stream_id'])
            return False

        mdata = metadata.to_map(stream['metadata'])
        return "Deleted" in stream["schema"]["properties"] and metadata.get(mdata, ('properties', 'Deleted'), 'selected')

    @staticmethod
    def get_query(state, stream):
        selected_field_names = selected_fields(stream)
        dotted_field_names = joined_fields(selected_field_names, stream)
        fields = ", ".join(dotted_field_names)
        query = "select {} from {}".format(fields, stream["tap_stream_id"])
        if stream.get("replication_key"):
            replication_key = stream["replication_key"]
            query += " order by {} asc".format(replication_key)

        LOGGER.info("Executing query: %s", query)
        return query

    @staticmethod
    def get_payload(state, stream, partner_id):
        stream_name = stream["tap_stream_id"]
        version = state["bookmarks"][stream["tap_stream_id"]]["version"]
        project = "{}_{}".format(stream_name, version)
        query = Aqua.get_query(state, stream)
        deleted = Aqua.deleted_records_available(stream)
        payload = Aqua.make_payload(stream_name, project, query, partner_id, deleted)

        if stream.get("replication_key"):
            # Incremental time must be in Pacific time
            # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/e_Post_Query_with_Retrieval_Time#Request_Parameters
            start_date = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
            inc_pen = pendulum.parse(start_date)
            inc_pen = inc_pen.astimezone(pendulum.timezone("US/Pacific"))
            payload["incrementalTime"] = inc_pen.strftime(Aqua.PARAMETER_DATE_FORMAT)

        return payload

    @staticmethod
    def create_job(client, state, stream):
        endpoint = "v1/batch-query/"
        # This _always_ submits with an incremental_time which I think
        # means that we're never executing a full export which means we
        # can't establish a baseline to report deletes on.
        # https://stitchdata.atlassian.net/browse/SRCE-322
        payload = Aqua.get_payload(state, stream, client.partner_id)
        # Log to show whether the aqua request should trigger a full or
        # incremental response based on
        # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
        LOGGER.info("Submitting aqua request with `%s`",
                    {k: v for k, v in payload.items()
                     if k in {'partner', 'project', 'incrementalTime'}})
        resp = client.aqua_request("POST", endpoint, json=payload).json()
        # Log to show whether the aqua response is in full or incremental
        # mode based on
        # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/a_Export_Deleted_Data
        if 'batches' in resp:
            LOGGER.info("Received aqua response with batch fulls=%s",
                        [x.get('full', None) for x in resp['batches']])
        else:
            LOGGER.info("Received aqua response with no batches")
        if "message" in resp:
            raise ExportFailed(resp["message"])

        return resp["id"]

    @staticmethod
    def stream_status(client, stream_name):
        """
        Check if the provided Zuora object (stream_name) can be queried via
        AQuA API by issuing a small export job of 1 row. This job must be
        cleaned up after submission to limit concurrent jobs during
        discovery.

        The response from submitting the job indicates whether or not the
        object is available.
        """
        endpoint = "v1/batch-query/"
        query = "select * from {} limit 1".format(stream_name)
        payload = Aqua.make_payload(stream_name, "discover", query, client.partner_id)
        resp = client.aqua_request("POST", endpoint, json=payload).json()

        # Cancel this job to keep concurrency low.
        client.aqua_request("DELETE", "v1/batch-query/jobs/{}".format(resp['id']))
        if "message" in resp:
            if resp["message"] == SYNTAX_ERROR:
                return "unavailable"
            elif resp["message"] == NO_DELETED_SUPPORT:
                return "available"
            else:
                raise Exception("Error probing {}: {}".format(stream_name, resp["message"]))

        return "available_with_deleted"

    # Must match call signature of other APIs
    @staticmethod
    def job_ready(client, job_id):
        endpoint = "v1/batch-query/jobs/{}".format(job_id)
        data = client.aqua_request("GET", endpoint).json()
        if data["status"] == "completed":
            return True
        elif data["status"] == "failed":
            raise ExportFailed(data["batches"][0]["message"])
        else:
            return False

    # Must match call signature of other APIs
    @staticmethod
    def get_file_ids(client, job_id):
        endpoint = "v1/batch-query/jobs/{}".format(job_id)
        data = client.aqua_request("GET", endpoint).json()
        if "segments" in data["batches"][0]:
            return data["batches"][0]["segments"]
        return [data["batches"][0]["fileId"]]

    # Must match call signature of other APIs
    @staticmethod
    def stream_file(client, file_id):
        endpoint = "v1/file/{}".format(file_id)
        return client.aqua_request("GET", endpoint, stream=True).iter_lines()


class Rest:
    ZOQL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    @staticmethod
    def make_payload(query):
        return {
            "Format": "csv",
            "Query": query,
        }

    @staticmethod
    def get_query(stream, start_date, end_date):
        selected_field_names = selected_fields(stream)
        dotted_field_names = joined_fields(selected_field_names, stream)
        fields = ", ".join(dotted_field_names)
        query = "select {} from {}".format(fields, stream["tap_stream_id"])

        if stream.get("replication_key") and start_date and end_date:
            start_date = format_datetime_zoql(start_date, Rest.ZOQL_DATE_FORMAT)
            end_date = format_datetime_zoql(end_date, Rest.ZOQL_DATE_FORMAT)
            query += " where {} >= '{}'".format(stream["replication_key"], start_date)
            query += " and {} < '{}'".format(stream["replication_key"], end_date)

        LOGGER.info("Executing query: %s", query)
        return query

    @staticmethod
    def get_payload(stream, start_date, end_date):
        query = Rest.get_query(stream, start_date, end_date)
        return Rest.make_payload(query)

    @staticmethod
    def create_job(client, stream, start_date=None, end_date=None):
        endpoint = "v1/object/export"
        payload = Rest.get_payload(stream, start_date, end_date)
        resp = client.rest_request("POST", endpoint, json=payload).json()
        return resp["Id"]

    # Must match call signature of other APIs
    @staticmethod
    def job_ready(client, job_id):
        endpoint = "v1/object/export/{}".format(job_id)
        data = client.rest_request("GET", endpoint).json()
        if data["Status"] == "Completed":
            return True
        elif data["Status"] in ["Cancelled", "Failed"]:
            raise ExportFailed(data["StatusReason"])
        else:
            return False

    # Must match call signature of other APIs
    @staticmethod
    def get_file_ids(client, job_id):
        endpoint = "v1/object/export/{}".format(job_id)
        data = client.rest_request("GET", endpoint).json()
        return [data["FileId"]]

    # Must match call signature of other APIs
    @staticmethod
    def stream_file(client, file_id):
        endpoint = "v1/files/{}".format(file_id)
        return client.rest_request("GET", endpoint, stream=True).iter_lines()

    @staticmethod
    def stream_status(client, stream_name):
        endpoint = "v1/object/export"
        query = "select * from {} limit 1".format(stream_name)
        payload = {
            "Query": query,
            "Format": "csv"
        }
        resp = client.rest_request("POST", endpoint, json=payload).json()

        if resp["Success"]:
            return "available"

        # Should we raise an "Error probing" exception here?
        return "unavailable"
