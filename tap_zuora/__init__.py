import json
import sys
import time

import singer

from singer import metadata
from tap_zuora.client import Client
from tap_zuora.discover import discover_streams
from tap_zuora.sync import sync_stream


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "api_type",
    "username",
    "password",
]


LOGGER = singer.get_logger()

def convert_legacy_state(catalog, state):
    new_state = {"bookmarks": {}, "current_stream": state.get("current_stream")}
    for stream in catalog["streams"]:
        if stream.get("selected") and stream.get("replication_key") and stream["tap_stream_id"] in state:
            new_state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]] = state[stream["tap_stream_id"]]

    return new_state

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def validate_state(config, catalog, state):
    if "bookmarks" not in state:
        if state.keys():
            LOGGER.info("Legacy state detected")
            state = convert_legacy_state(catalog, state)
        else:
            LOGGER.info("No bookmarks found")
            state["bookmarks"] = {}

    if "current_stream" not in state:
        LOGGER.info("Current stream not found")
        state["current_stream"] = None

    for stream in catalog.streams:
        if not stream_is_selected(metadata.to_map(stream.metadata)):
            if state["current_stream"] == stream.tap_stream_id:
                state["current_stream"] = None
            continue

        if stream.tap_stream_id not in state["bookmarks"]:
            LOGGER.info("Initializing state for %s", stream.tap_stream_id)
            version = int(time.time())
            state["bookmarks"][stream.tap_stream_id] = {"version": version}

        if not stream.replication_key:
            continue

        if stream.replication_key not in state["bookmarks"][stream.tap_stream_id]:
            LOGGER.info("Setting start date for %s to %s", stream.tap_stream_id, config["start_date"])
            state["bookmarks"][stream.tap_stream_id][stream.replication_key] = config["start_date"]

    return state


def do_discover(client, force_rest=False):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client, force_rest)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def do_sync(client, catalog, state, force_rest=False):
    starting_stream = state.get("current_stream")
    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        if not stream_is_selected(metadata.to_map(stream.metadata)):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info("%s: Resuming", stream_name)
                starting_stream = None
            else:
                LOGGER.info("%s: Skipping - already synced", stream_name)
                continue
        else:
            LOGGER.info("%s: Starting", stream_name)

        state["current_stream"] = stream_name
        singer.write_state(state)
        singer.write_schema(stream_name, stream.schema.to_dict(), stream.key_properties)
        counter = sync_stream(client, state, stream.to_dict(), force_rest)

        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client.from_config(args.config)
    force_rest = args.config.get("api_type") == "REST"

    # Using the AQuA API requires a Zuora Partner ID
    if not force_rest:
        partner_id = args.config.get("partner_id")
        if not partner_id:
            raise Exception("Config is missing required `partner_id` key when using the AQuA API")

    if args.discover:
        do_discover(client, force_rest)
    elif args.catalog:
        state = validate_state(args.config, args.catalog, args.state)
        do_sync(client, args.catalog, state, force_rest)
