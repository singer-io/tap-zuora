# tap-zuora

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Installation

See the getting-started guide:

https://github.com/singer-io/getting-started

## Usage

This section dives into basic usage of `tap-zuora` by walking through extracting
data from the api.

### Create the configuration file

Create a config file containing the zuora credentials, e.g.:

```json
{
  "api_type": "REST",
  "username": "abcd",
  "password": "1234",
  "sandbox": "true",
  "european": "false",
  "start_date": "2022-10-10T00:00:00Z"
}
```

### Discovery mode

The tap can be invoked in discovery mode to find the available zuora objects.

```bash
$ tap-zuora --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

### Field selection

In sync mode, `tap-zuora` consumes the catalog and looks for streams that have been
marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-zuora --config config.json --discover > catalog.json
```

Then edit `catalog.json` to make selections. The stream's metadata entry (associated
with `"breadcrumb": []`) gets a top-level `selected` flag, as does its columns' metadata
entries.

```diff
[
  {
    "breadcrumb": [],
    "metadata": {
      "valid-replication-keys": [
        "UpdatedOn"
      ],
      "table-key-properties": [
        "Id"
      ],
      "forced-replication-method": "INCREMENTAL",
+      "selected": "true"
    }
  },
]
```
Optionally, also create a state.json file. current_stream is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.
```json
{
  "current_stream": "Account",
  "bookmarks": {
    "AchNocEventLog": {"UpdatedOn": "2022-10-15T00:00:00Z"},
    "PaymentMethodTransactionLog": {"TransactionDate": "2022-10-15T00:00:00Z"}
  }
}
```
### Sync mode

With a `catalog.json` that describes field and table selections, the tap can be invoked in sync mode:

```bash
$ tap-zuora --config config.json --catalog catalog.json --state state.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

---

Copyright &copy; 2017 Stitch
