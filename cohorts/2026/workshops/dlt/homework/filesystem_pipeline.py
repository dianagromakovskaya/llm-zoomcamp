"""dlt filesystem pipeline: load local Claude Code session logs (.jsonl) into DuckDB."""

import hashlib
import json

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl


def _ensure_uuid(record: dict) -> dict:
    """Meta log lines (mode, permission-mode, ...) have no uuid; derive a
    stable one from the record content so merge dedup has a NOT NULL key."""
    if not record.get("uuid"):
        record["uuid"] = hashlib.sha1(
            json.dumps(record, sort_keys=True, default=str).encode()
        ).hexdigest()
    return record


def load_claude_logs() -> None:
    """Load raw Claude Code session log records into DuckDB.

    bucket_url is read from .dlt/config.toml under [sources.filesystem].
    file_glob is set inline so it lives next to the code that depends on it.
    """
    pipeline = dlt.pipeline(
        pipeline_name="claude_logs_pipeline",
        destination="duckdb",
        dataset_name="claude_logs",
    )

    reader = (
        filesystem(
            file_glob="**/*.jsonl",
            incremental=dlt.sources.incremental("modification_date"),
        )
        | read_jsonl()
    ).with_name("claude_logs")
    reader.add_map(_ensure_uuid)
    reader.apply_hints(primary_key="uuid")

    load_info = pipeline.run(reader, write_disposition="merge")
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


if __name__ == "__main__":
    load_claude_logs()
