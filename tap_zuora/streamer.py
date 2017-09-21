import csv
import datetime
import io

import dateutil
import singer
import singer.utils


PARTNER_ID = "salesforce"
MAX_EXPORT_DAYS = 30
MAX_EXPORT_POLLS = 60
EXPORT_SLEEP_INTERVAL = 30

LOGGER = singer.get_logger()


def parse_line(line):
    reader = csv.reader(io.StringIO(line.decode('utf-8')))
    return next(reader)


def convert_header(header):
    _, header = header.split(".", 1)
    return header


def parse_header_line(line):
    return [convert_header(h) for h in parse_line(line)]


class ExportFailed(Exception):
    pass


class Streamer:
    def __init__(self, entity, client, state):
        self.entity = entity
        self.client = client
        self.state = state

    def gen_records(self):
        raise NotImplemented("Must be implemented in subclass")


class AquaStreamer(Streamer):
    def gen_records(self):
        file_ids = self.state.get_file_ids(self.entity)
        if not file_ids:
            start_date = self.state.get_bookmark(self.entity)
            query = self.entity.get_zoqlexport(start_date)
            job_id = self.post_job(query)
            file_ids = self.poll_job(job_id)
            self.state.set_file_ids(self.entity, file_ids)

        while len(file_ids) > 0:
            file_id = file_ids.pop(0)
            lines = self.get_file(file_id).iter_lines()
            header = parse_header_line(next(lines))
            for line in lines:
                data = parse_line(line)
                row = dict(zip(header, data))
                row = self.entity.format_values(row)
                yield row

            self.state.set_file_ids(self.entity, file_ids)

    def post_job(self, query):
        project = "{}_{}".format(self.entity.name, self.state.get_version(self.entity))
        export_data = {
            "name": self.entity.name,
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
                    "deleted": {
                        "column": "deleted",
                        "format": "Boolean",
                    },
                },
            ],
        }

        if self.entity.replication_key:
            # incremental time has to be YYYY-mm-dd HH:MM:SS in Pacific time
            # so we need to parse the datestring, make it UTC, then convert
            start_date = self.state.get_bookmark(self.entity)
            start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
            start_dt = start_dt.replace(tzinfo=dateutil.tz.gettz("UTC"))
            incremental_dt = start_dt.astimezone(dateutil.tz.gettz("America/Los_Angeles"))
            export_data["incrementalTime"] =  incremental_dt.strftime("%Y-%m-%d %H:%M:%S")

        resp = self.client.aqua_request("POST", "apps/api/batch-query/", json=export_data).json()
        if "message" in resp:
            raise SyntaxError(resp["message"])

        return resp['id']

    def poll_job(self, job_id):
        poll = 0
        while poll < MAX_EXPORT_POLLS:
            resp = self.client.aqua_request("GET", "apps/api/batch-query/jobs/{}".format(job_id)).json()

            if resp['status'] == 'completed':
                LOGGER.info("Export completed")
                batch = resp['batches'][0]
                if 'segments' in batch:
                    return batch['segments']
                else:
                    return [batch['fileId']]

            elif resp['status'] == 'failed':
                raise ExportFailed(batch['message'])

            time.sleep(EXPORT_SLEEP_INTERVAL)
            poll += 1

        raise ExportFailed("Timed out")

    def get_file(self, file_id):
        return self.client.aqua_request("GET", "apps/api/file/{}".format(file_id), stream=True)


class RestStreamer(Streamer):
    def _gen_records(self, start_date=None, end_date=None):
        query = self.entity.get_zoql(start_date, end_date)
        job_id = self.post_job(query)
        file_id = self.poll_job(job_id)
        lines = self.get_file(file_id).iter_lines()

        header = parse_header_line(next(lines))
        for line in lines:
            data = parse_line(line)
            row = dict(zip(header, data))
            row = self.entity.format_values(row)
            yield row

    def gen_records(self):
        if self.entity.replication_key:
            now = datetime.datetime.utcnow()
            start_date = self.state.get_bookmark(self.entity)
            start_dt = singer.utils.strptime(start_date)
            while start_dt < now:
                end_dt = start_dt + datetime.timedelta(days=MAX_EXPORT_DAYS)
                end_date = singer.utils.strftime(end_dt)
                for record in self._gen_records(start_date, end_date):
                    yield record

        else:
            for record in self._gen_records():
                yield record

    def post_job(self, query):
        export_query = {
            "Format": "csv",
            "Query": query,
        }
        return self.client.rest_request("POST", "object/export", json=export_query).json()["Id"]

    def poll_job(self, job_id):
        poll = 0
        while poll < MAX_EXPORT_POLLS:
            resp = self.client.rest_headers("GET", "object/export/{}".format(job_id)).json()
            if resp["Status"] == "Completed":
                return resp["FileId"]
            elif resp["Status"] == "Failed":
                raise ExportFailed("Export failed")

            time.sleep(EXPORT_SLEEP_INTERVAL)
            poll += 1

        raise ExportFailed("Timed out")

    def get_file(self, file_id):
        return self.client.rest_request("GET", "files/{}".format(file_id), stream=True)
