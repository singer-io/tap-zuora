from collections import namedtuple

from tap_zuora.client import Client
from tap_zuora.discover import discover_entities
from tap_zuora.entity import Entity
from tap_zuora.state import State
from tap_zuora.streamer import (
    AquaStreamer,
    RestStreamer,
)

import singer
from singer import catalog


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "username",
    "password",
]

LOGGER = singer.get_logger()


def do_discover(client, force_rest=False):
    LOGGER.info("Starting discover")
    catalog = discover_entities(client)
    catalog.dump()
    LOGGER.info("Finished discover")


def do_sync(client, state, catalog, force_rest=False):
    LOGGER.info("Starting sync")

    if force_rest:
        LOGGER.info("Using REST API")
        Streamer = RestStreamer
    else:
        LOGGER.info("Using AQuA API")
        Streamer = AquaStreamer

    started = False
    for catalog_entry in catalog.streams:
        if not started and state.current_stream and catalog_entry.tap_stream_id != state.current_stream:
            continue
        else:
            started = True

        if not catalog_entry.selected:
            LOGGER.info("%s not selected. Skipping.", catalog_entry.tap_stream_id)
            continue
        else:
            LOGGER.info("Syncing %s", entity.name)

        entity = Entity(
            name=catalog_entry.tap_stream_id,
            schema=catalog_entry.schema,
            replication_key=catalog_entry.replication_key,
            key_properties=catalog_entry.key_properties,
        )
        streamer = Streamer(entity, client, state)

        state.current_stream = entity.name
        singer.write_state(state.to_dict())

        singer.write_schema(entity.name, entity.schema.to_dict(), entity.key_properties)
        with metrics.record_counter(entity.name) as counter:
            for record in streamer.gen_records():
                singer.write_record(entity.name, record)
                entity.update_bookmark(record, state)
                singer.write_state(state.to_dict())
                counter.increment()

        LOGGER.info("Finished syncing %s %s's", counter.value, entity.name)
        state.current_stream = None
        singer.write_state(state.to_dict())

    LOGGER.info("Finished sync")
    singer.write_state(state.to_dict())


def main(config, state_dict, properties=None, discover=False):
    client = Client(
        username=config["username"],
        password=config["password"],
        sandbox=config.get("sandbox", False),
        euro=config.get("european", False),
    )

    force_rest = config.get("force_rest", False)

    if discover:
        do_discover(client, force_rest)
    elif properties:
        state = State(state_dict, config["start_date"])
        catalog = catalog.Catalog(annotated_schemas)
        do_sync(client, state, catalog, force_rest)
    else:
        raise Exception("Must have properties or run discovery")


if __name__ == "__main__":
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    main(args.config, args.state, args.properties, args.discover)
