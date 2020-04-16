# pipelinewise-tap-zuora

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-zuora.svg)](https://badge.fury.io/py/pipelinewise-tap-zuora)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-zuora.svg)](https://pypi.org/project/pipelinewise-tap-zuora/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from the [Zuora API](https://www.zuora.com/) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

# instructions

Make a virtualenv and install this tap:

```
pip install -e .
```

Write config into new file config.json:

```
# config.json content:
{
    "username": "<username>",
    "password": "<password>",
    "api_type": "AQUA", # or REST
    "start_date": "2020-04-01",
    "sandbox": "false",
    "european": "true",
    "partner_id": "<partner_id>"
}
```

Run discovery mode in order to obtain the schema with everything that can be exported:

```
tap-zuora -c config.json --discover > catalog.json
```

Save the modified file as e.g. `catalog_aqua_selection.json` to use when running the sync in the next step.

Run the tap:

```
tap-zuora -c config.json --catalog catalog_aqua_selection.json
```

---

Based on Stitch documentation
