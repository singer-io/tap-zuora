import pendulum
from singer import metadata


MAX_EXPORT_DAYS = 30
ZOQL_FORMAT = "%Y-%m-%d %H:%M:%S"
PARTNER_ID = "salesforce"
SYNTAX_ERROR = "There is a syntax error in one of the queries in the AQuA input"
NO_DELETED_SUPPORT = ("Objects included in the queries do not support the querying of deleted "
                      "records. Remove Deleted section in the JSON request and retry the request")


def selected_fields(stream):
    mdata = metadata.to_map(stream['metadata'])
    return [f for f, s in stream["schema"]["properties"].items()
            if metadata.get(mdata, ('properties', f), 'selected')
            or metadata.get(mdata, ('properties', f), 'inclusion') == 'automatic']


def format_datetime_zoql(datetime_str):
    return pendulum.parse(datetime_str, tz=pendulum.timezone("UTC")).strftime(ZOQL_FORMAT)


class ExportFailed(Exception):
    pass


class Aqua:
    @staticmethod
    def make_payload(stream_name, project, query, deleted=False):
        rtn = {
            "name": stream_name,
            "partner": PARTNER_ID,
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
    def get_query(state, stream):
        fields = ", ".join(selected_fields(stream))
        query = "select {} from {}".format(fields, stream["tap_stream_id"])
        if stream.get("replication_key"):
            bookmark = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
            start_date = format_datetime_zoql(bookmark)
            query += " where {} >= '{}'".format(stream["replication_key"], start_date)
            query += " order by {} asc".format(stream["replication_key"])

        return query

    @staticmethod
    def get_payload(state, stream):
        stream_name = stream["tap_stream_id"]
        version = state["bookmarks"][stream["tap_stream_id"]]["version"]
        project = "{}_{}".format(stream_name, version)
        query = Aqua.get_query(state, stream)
        deleted = "Deleted" in stream["schema"]["properties"]
        payload = Aqua.make_payload(stream_name, project, query, deleted)

        if stream.get("replication_key"):
            # Incremental time must be in Pacific time
            # https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/B_Submit_Query/e_Post_Query_with_Retrieval_Time#Request_Parameters
            start_date = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
            inc_pen = pendulum.parse(start_date)
            inc_pen = inc_pen.astimezone(pendulum.timezone("America/Los_Angeles"))
            payload["incrementalTime"] = inc_pen.strftime(ZOQL_FORMAT)

        return payload

    @staticmethod
    def create_job(client, state, stream):
        endpoint = "apps/api/batch-query/"
        payload = Aqua.get_payload(state, stream)
        resp = client.aqua_request("POST", endpoint, json=payload).json()
        if "message" in resp:
            raise ExportFailed(resp["message"])

        return resp["id"]

    @staticmethod
    def stream_status(client, stream_name):
        endpoint = "apps/api/batch-query/"
        query = "select * from {} limit 1".format(stream_name)
        payload = Aqua.make_payload(stream_name, "discover", query)
        resp = client.aqua_request("POST", endpoint, json=payload).json()
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
        endpoint = "apps/api/batch-query/jobs/{}".format(job_id)
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
        endpoint = "apps/api/batch-query/jobs/{}".format(job_id)
        data = client.aqua_request("GET", endpoint).json()
        if "segments" in data["batches"][0]:
            return data["batches"][0]["segments"]
        else:
            return [data["batches"][0]["fileId"]]

    # Must match call signature of other APIs
    @staticmethod
    def stream_file(client, file_id):
        endpoint = "apps/api/file/{}".format(file_id)
        return client.aqua_request("GET", endpoint, stream=True).iter_lines()


class Rest:
    @staticmethod
    def make_payload(query):
        return {
            "Format": "csv",
            "Query": query,
        }

    @staticmethod
    def get_query(stream, start_date, end_date):
        fields = ", ".join(selected_fields(stream))
        query = "select {} from {}".format(fields, stream["tap_stream_id"])

        if stream.get("replication_key") and start_date and end_date:
            start_date = format_datetime_zoql(start_date)
            end_date = format_datetime_zoql(end_date)
            query += " where {} >= '{}'".format(stream["replication_key"], start_date)
            query += " and {} < '{}'".format(stream["replication_key"], end_date)

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
