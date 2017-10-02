import json
import sys
import time

import singer

from tap_zuora.client import Client
from tap_zuora.state import State
from tap_zuora.discover import discover_streams
from tap_zuora.sync import sync_stream


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "username",
    "password",
]


LOGGER = singer.get_logger()


def validate_state(config, catalog, state):
    if "current_stream" not in state:
        LOGGER.info("Current stream not found")
        state["current_stream"] = None

    if "bookmarks" not in state:
        LOGGER.info("No current bookmarks found")
        state["bookmarks"] = {}

    for stream in catalog["streams"]:
        if not stream.get("selected"):
            continue

        if stream["tap_stream_id"] not in state["bookmarks"]:
            LOGGER.info("Initializing state for %s", stream["tap_stream_id"])
            version = int(time.time())
            state["bookmarks"][stream["tap_stream_id"]] = {"version": version}

        if not stream.get("replication_key"):
            continue

        if stream["replication_key"] not in state["bookmarks"][stream["tap_stream_id"]]:
            LOGGER.info("Setting start date for %s to %s", stream["tap_stream_id"], config["start_date"])
            state["bookmarks"][stream["tap_stream_id"]][stream["replication_key"]] = config["start_date"]

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

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        if not stream.get("selected"):
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
        singer.write_schema(stream_name, stream["schema"], stream["key_properties"])
        counter = sync_stream(client, state, stream, force_rest)

        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")


def main(config, catalog, state, discover=False):
    client = Client.from_config(config)
    force_rest = config.get("force_rest", False)
    if discover:
        do_discover(client, force_rest)
    elif catalog:
        state = validate_state(config, catalog, state)
        if isinstance(catalog, singer.catalog.Catalog):
            catalog = catalog.to_dict()
        do_sync(client, state, catalog, force_rest)
    else:
        raise Exception("Must have catalog if syncing")

if __name__ == "__main__":
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    try:
        main(args.config, args.properties, args.state, args.discover)
    except Exception as e:
        LOGGER.fatal(e)
        raise e
