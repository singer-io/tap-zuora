# tap-zuora
Tap for Zuora

# running

```
pip install -e .
```

Write config into new file config.json:

```
# config.json content:
{
    "username": "<username>",
    "password": "<password>",
    "api_type": "REST", # or AQUA
    "start_date": "2020-04-01",
    "sandbox": "false",
    "european": "true",
    "partner_id": "<partner_id>"
}
```

Run discovery mode

```
tap-zuora -c config.json --discover > catalog.json
```

The resulting catalog of the rest api and of the aqua api are included in the repo as `catalog_aqua.json` or `catalog_rest.json`

Copy or edit the catalog json file and mark some streams and fields as being selected:

- Add this entry in the metadata list of a stream to select it

```
{
    "breadcrumb": [],
    "metadata": {
        "selected": "true"
    }
},
```

- Add this to other entries in the metadata of a stream (those with breadcrums):

```
"selected": "true"
```

Run the tap:

```
tap-zuora -c config.json --catalog catalog_aqua_selection.json
```

---

Copyright &copy; 2017 Stitch
