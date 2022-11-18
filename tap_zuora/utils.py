from typing import Dict, Optional


def make_aqua_payload(project: str, query: str, partner_id: str, deleted: Optional[bool] = False) -> Dict:
    # NB - 4/5/19 - Were told by zuora support to use the same value
    # for both project and name to imply an incremental export
    rtn = {
        "name": project,
        "partner": partner_id,
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
            },
        ],
    }

    if deleted:
        rtn["queries"][0]["deleted"] = {"column": "Deleted", "format": "Boolean"}

    return rtn
