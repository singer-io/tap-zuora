# tap-zuora
Tap for Zuora

# instructions

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

Run discovery mode in order to view the schema

```
tap-zuora -c config.json --discover > catalog.json
```

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
