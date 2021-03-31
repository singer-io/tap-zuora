import csv
import io
import time

import pendulum
import singer

from singer import transform
from tap_zuora import apis
from tap_zuora.client import ApiException

PARTNER_ID = "salesforce"
DEFAULT_POLL_INTERVAL = 60
DEFAULT_JOB_TIMEOUT = 5400
MAX_EXPORT_DAYS = 30

LOGGER = singer.get_logger()


def parse_csv_line(line):
    reader = csv.reader(io.StringIO(line.decode('utf-8').replace('\0', '')))
    return next(reader)


def convert_header(header, stream):
    dotted_field = header.split(".")
    if stream == dotted_field[0]:
        return dotted_field[1]

    return header.replace(".", "")


def parse_header_line(line, stream):
    return [convert_header(h, stream) for h in parse_csv_line(line)]


def poll_job_until_done(job_id, client, api):
    timeout_time = pendulum.utcnow().add(seconds=DEFAULT_JOB_TIMEOUT)
    while pendulum.utcnow() < timeout_time:
        if api.job_ready(client, job_id):
            return api.get_file_ids(client, job_id)

        time.sleep(DEFAULT_POLL_INTERVAL)

    raise apis.ExportTimedOut(DEFAULT_JOB_TIMEOUT // 60, "minutes")

def clear_file_ids(state, stream):
    state["bookmarks"][stream["tap_stream_id"]].pop("file_ids", None)
    singer.write_state(state)
    return state


def clear_stateful_session(state, stream):
    state["bookmarks"][stream["tap_stream_id"]]["version"] = int(time.time())
    singer.write_state(state)
    return state

def sync_file_ids(file_ids, client, state, stream, api, counter): # pylint: disable=too-many-branches
    if stream.get("replication_key"):
        start_date = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
    else:
        start_date = None

    while file_ids:
        file_id = file_ids.pop(0)
        # Tracking variable to see whether we saw a deleted record
        # anywhere in this batch file. Needs to reset after processing
        # each file.
        saw_deleted = False
        try:
            lines = api.stream_file(client, file_id)
        except ApiException as ex:
            # If the file has been deleted, write state with "file_ids" removed and re-raise.
            # Don't advance the bookmark until all files in the window have been synced.
            if ex.resp.status_code == 404:
                clear_file_ids(state, stream)
                raise Exception(("File ID {} has been deleted, making the sync window invalid. "
                                 "Removing partially exported files from state and will resume "
                                 "from bookmark on the next extraction.")
                                .format(file_id)) from ex
            raise
        header = parse_header_line(next(lines), stream["tap_stream_id"])
        extraction_time = singer.utils.now()
        for line in lines:
            if not line:
                continue

            parsed_line = parse_csv_line(line)
            if len(header) != len(parsed_line):
                state = clear_file_ids(state, stream)
                state = clear_stateful_session(state, stream)
                raise Exception(("Detected that File ID {} is non-rectangular. Found row "
                                 "with {} entries, expected {} entries from header line. "
                                 "Will resume from bookmark with new AQuA session on next extraction.")
                                .format(file_id, len(parsed_line), len(header)))

            row = dict(zip(header, parsed_line))
            record = transform(row, stream['schema'])
            # safe get because not all records will have 'Deleted'
            if record.get('Deleted', False):
                # We should emit that we saw a deleted record
                saw_deleted = True
            if stream.get("replication_key"):
                bookmark = record.get(stream["replication_key"])
                if not bookmark:
                    # There's a chance we get back a bad record here, and we don't want to null the bookmark
                    continue

                if bookmark and bookmark < start_date:
                    continue

                singer.write_record(stream["tap_stream_id"], record, time_extracted=extraction_time)
                state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]] = bookmark
                singer.write_state(state)
            else:
                singer.write_record(stream["tap_stream_id"], record, time_extracted=extraction_time)

            counter.increment()

        if saw_deleted:
            # https://stitchdata.atlassian.net/browse/SRCE-322
            LOGGER.info("Saw a deleted record in %s", file_id)

        state["bookmarks"][stream["tap_stream_id"]]["file_ids"] = file_ids
        singer.write_state(state)

    state["bookmarks"][stream["tap_stream_id"]]["file_ids"] = None
    singer.write_state(state)
    return counter

def handle_aqua_timeout(ex, stream, state):
    if stream.get("replication_key"):
        LOGGER.info("Export timed out, reducing query window and writing state.")
        window_bookmark = state["bookmarks"][stream["tap_stream_id"]].get("current_window_end")
        previous_window_end = pendulum.parse(window_bookmark) if window_bookmark else pendulum.utcnow()
        window_start = pendulum.parse(state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]])
        if previous_window_end == window_start:
            raise apis.ExportFailed("Export too large for smallest possible query window. " +
                                    "Cannot subdivide any further. ({}: {})"
                                    .format(stream["replication_key"], window_start)) from ex

        half_day_range = (previous_window_end - window_start) // 2
        current_window_end = previous_window_end - half_day_range
        state["bookmarks"][stream["tap_stream_id"]]["current_window_end"] = current_window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        singer.write_state(state)

def sync_aqua_stream(client, state, stream, counter):
    timed_out = False
    try:
        file_ids = state["bookmarks"][stream["tap_stream_id"]].get("file_ids")
        if not file_ids:
            job_id = apis.Aqua.create_job(client, state, stream)
            file_ids = poll_job_until_done(job_id, client, apis.Aqua)
            state["bookmarks"][stream["tap_stream_id"]]["file_ids"] = file_ids
            singer.write_state(state)

        window_end = state["bookmarks"][stream["tap_stream_id"]].pop("current_window_end", None)
        if window_end:
            # Save the window_end as the latest bookmark in case the window was empty
            state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]] = window_end
        return sync_file_ids(file_ids, client, state, stream, apis.Aqua, counter)
    except apis.ExportTimedOut as ex:
        handle_aqua_timeout(ex, stream, state)
        timed_out = True

    if timed_out:
        LOGGER.info("Retrying timed out sync job...")
        return sync_aqua_stream(client, state, stream, counter)

def handle_rest_timeout(ex, stream, state, current_window, start_pen):
    if stream.get("replication_key"):
        LOGGER.info("Export timed out, reducing query window and writing state.")
        new_window = current_window // 2
        if new_window == 0:
            raise apis.ExportFailed("Export too large for smallest possible query window. " +
                                    "Cannot subdivide any further. ({}: {})"
                                    .format(stream["replication_key"], start_pen)) from ex
        state["bookmarks"][stream["tap_stream_id"]]["window_length"] = new_window
        singer.write_state(state)
        return new_window
    # NB: Pylint caught this, since no return existed. Returning `None` to not change
    #     the existing return value, but it may not make sense for usage, or never
    #     get hit (defensive coding might not be necessary above)
    return None

def iterate_rest_query_window(client, state, stream, counter,
                              start_pen, sync_started, window_length):
    try:
        timed_out = False
        while start_pen < sync_started:
            end_pen = start_pen.add(seconds=window_length)
            if end_pen > sync_started:
                end_pen = sync_started

            start_date = start_pen.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end_pen.strftime("%Y-%m-%d %H:%M:%S")
            job_id = apis.Rest.create_job(client, stream, start_date, end_date)
            file_ids = poll_job_until_done(job_id, client, apis.Rest)
            counter = sync_file_ids(file_ids, client, state, stream, apis.Rest, counter)
            start_pen = end_pen
            window_length = MAX_EXPORT_DAYS * 86400
            state["bookmarks"][stream["tap_stream_id"]].pop("window_length", None)
            state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]] = end_date
            singer.write_state(state)
    except apis.ExportTimedOut as ex:
        window_length = handle_rest_timeout(ex,
                                            stream,
                                            state,
                                            window_length,
                                            start_pen)
        timed_out = True

    if timed_out:
        LOGGER.info("Retrying timed out sync job...")
        return iterate_rest_query_window(client, state, stream, counter,
                                         start_pen, sync_started, window_length)
    return counter

def sync_rest_stream(client, state, stream, counter):
    file_ids = state["bookmarks"][stream["tap_stream_id"]].get("file_ids")
    if file_ids:
        counter = sync_file_ids(file_ids, client, state, stream, apis.Rest, counter)

    if stream.get("replication_key"):
        bookmark_window_length = state["bookmarks"][stream["tap_stream_id"]].pop("window_length", None)
        window_length_in_seconds = bookmark_window_length or MAX_EXPORT_DAYS * 86400
        sync_started = pendulum.utcnow()
        start_date = state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]]
        start_pen = pendulum.parse(start_date)
        counter = iterate_rest_query_window(client, state, stream, counter,
                                            start_pen, sync_started, window_length_in_seconds)
    else:
        job_id = apis.Rest.create_job(client, stream)
        file_ids = poll_job_until_done(job_id, client, apis.Rest)
        counter = sync_file_ids(file_ids, client, state, stream, apis.Rest, counter)

    return counter

def sync_stream(client, state, stream, force_rest=False):
    with singer.metrics.record_counter(stream["tap_stream_id"]) as counter:
        if force_rest:
            counter = sync_rest_stream(client, state, stream, counter)
        else:
            counter = sync_aqua_stream(client, state, stream, counter)

    return counter
