"""dlt REST API pipeline: load Pydantic Logfire spans/records into DuckDB.

Uses Logfire's Query API (POST /v2/query, SQL against the `records` table).
Docs: https://pydantic.dev/docs/logfire/manage/query-api/
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
from dotenv import load_dotenv
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

load_dotenv()

LOGFIRE_BASE_URL = "https://logfire-eu.pydantic.dev"
QUERY = "SELECT * FROM records ORDER BY start_timestamp DESC LIMIT 1000"


@dlt.source(name="logfire")
def logfire_source(access_token: str) -> Any:
    min_timestamp = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    config: RESTAPIConfig = {
        "client": {
            "base_url": LOGFIRE_BASE_URL,
            "auth": {"type": "bearer", "token": access_token},
            "headers": {"Accept": "application/json"},
        },
        "resources": [
            {
                "name": "records",
                "endpoint": {
                    "path": "v2/query",
                    "method": "POST",
                    "json": {"sql": QUERY, "min_timestamp": min_timestamp},
                    "data_selector": "data",
                },
            },
        ],
    }
    yield from rest_api_resources(config)


def load_logfire_records() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="logfire_pipeline",
        destination="duckdb",
        dataset_name="logfire_data",
        dev_mode=True,  # fresh dataset on every run during dev
    )

    access_token = os.environ["LOGFIRE_READ_TOKEN"]
    load_info = pipeline.run(
        logfire_source(access_token=access_token), write_disposition="replace"
    )
    print(load_info)


if __name__ == "__main__":
    load_logfire_records()
