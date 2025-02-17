import json
import sys
import time

import singer
from singer import Catalog

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


def convert_legacy_state(catalog: Catalog, state: dict) -> dict:
    """creates a legacy state file this method gets invoked when there is no
    bookmarks key in non-empty state file."""
    new_state = {"bookmarks": {}, "current_stream": state.get("current_stream")}
    for stream in catalog.streams:
        if stream.is_selected() and stream.replication_key and stream.tap_stream_id in state:
            new_state["bookmarks"][stream.tap_stream_id] = state[stream.tap_stream_id]

    return new_state


def validate_state(config: dict, catalog: Catalog, state: dict) -> dict:
    """Validates the state file Sets the bookmark value for each selected
    stream as start_date if bookmark value is None or not available Sets the
    current_stream to None if the current_stream is not selected."""
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
        if not stream.is_selected():
            if state["current_stream"] == stream.tap_stream_id:
                state["current_stream"] = None
            continue

        if stream.tap_stream_id not in state["bookmarks"]:
            LOGGER.info(f"Initializing state for {stream.tap_stream_id}")
            singer.write_bookmark(state, stream.tap_stream_id, "version", int(time.time()))

        if not stream.replication_key:
            continue

        if (
            stream.replication_key not in state["bookmarks"][stream.tap_stream_id]
            or state["bookmarks"][stream.tap_stream_id][stream.replication_key] is None
        ):
            LOGGER.info(
                f"Setting start date for " f"{stream.tap_stream_id} to " f'{config["start_date"]}',
            )
            singer.write_bookmark(
                state,
                stream.tap_stream_id,
                stream.replication_key,
                config["start_date"],
            )

    return state


def do_discover(client: Client):
    """starts the Discover process."""
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def do_sync(client: Client, catalog: Catalog, state: dict):
    """Starts the sync process for all the selected streams."""
    starting_stream = state.get("current_stream")
    if starting_stream:
        LOGGER.info(f"Resuming sync from {starting_stream}")
    else:
        LOGGER.info("Starting sync")

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        if not stream.is_selected():
            LOGGER.info(f"{stream_name}: Skipping - not selected")
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info(f"{stream_name}: Resuming")
                starting_stream = None
            else:
                LOGGER.info(f"{stream_name}: Skipping - already synced")
                continue
        else:
            LOGGER.info(f"{stream_name}: Starting")

        state["current_stream"] = stream_name
        singer.write_state(state)
        singer.write_schema(stream_name, stream.schema.to_dict(), stream.key_properties)
        counter = sync_stream(client, state, stream.to_dict())

        LOGGER.info(f"{stream_name}: Completed sync ({counter.value} rows)")

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client.from_config(args.config)

    # Using the AQuA API requires a Zuora Partner ID
    if not client.is_rest and not client.partner_id:
        raise Exception("Config is missing required `partner_id` key when using the AQuA API")

    if args.discover:
        do_discover(client)
    elif args.catalog:
        LOGGER.info(f'This connection is currently using {"REST " if client.is_rest else "AQuA "}API')
        state = validate_state(args.config, args.catalog, args.state)
        do_sync(client, args.catalog, state)


if __name__ == "__main__":
    main()
