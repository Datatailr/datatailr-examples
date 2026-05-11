"""Tasks for the EIA Weekly Petroleum Status Report ingestion workflow.

The workflow has three stages:

    download_report  ->  parse_and_store  ->  summarize

Each task runs in its own container; data flows through return values.
The full pipeline is idempotent: re-running for an already-stored report
date is a no-op.
"""

from __future__ import annotations

from datatailr import task
from datatailr.logging import DatatailrLogger

from weekly_oil_reports.common.parser import (
    parse_header,
    parse_report,
    parsed_blob_key,
    raw_blob_key,
)

logger = DatatailrLogger(__name__).get_logger()

EIA_TABLE9_URL = "https://ir.eia.gov/wpsr/table9.csv"


@task()
def download_report(url: str = EIA_TABLE9_URL) -> str:
    """Fetch the latest Weekly Petroleum Status Report (Table 9) as text."""
    import requests

    logger.info("Downloading EIA Table 9 from %s", url)
    resp = requests.get(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    text = resp.text
    logger.info("Downloaded %d bytes", len(text))
    return text


@task()
def parse_and_store(csv_text: str) -> dict:
    """Parse the CSV, then persist both raw and tidy versions to blob storage.

    Idempotent on the report's most-recent week-ending date: if the parsed
    blob already exists, the raw blob is still refreshed (cheap) and parse
    is skipped.
    """
    import io

    from datatailr import Blob

    from weekly_oil_reports.common.parser import PARSED_PREFIX

    header = parse_header(csv_text)
    report_date = header.report_date
    raw_key = raw_blob_key(report_date)
    parsed_key = parsed_blob_key(report_date)

    blob = Blob()

    blob.put(raw_key, csv_text.encode("utf-8"))
    logger.info("Stored raw report at blob://%s", raw_key)

    # Note: blob.exists() prints CLI help in some SDK versions, so we check
    # via blob.ls() on the parsed/ prefix and match by basename instead.
    parsed_basename = parsed_key.rsplit("/", 1)[-1]
    existing = blob.ls(PARSED_PREFIX + "/") or []
    already_parsed = any(
        (e["name"] if isinstance(e, dict) else str(e)).rsplit("/", 1)[-1] == parsed_basename
        for e in existing
    )
    if already_parsed:
        logger.info("Parsed report blob://%s already exists, skipping parse", parsed_key)
        return {
            "report_date": report_date.isoformat(),
            "raw_key": raw_key,
            "parsed_key": parsed_key,
            "row_count": 0,
            "action": "skipped_already_parsed",
        }

    df = parse_report(csv_text)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    blob.put(parsed_key, buf.getvalue())
    logger.info(
        "Stored parsed report at blob://%s (%d rows)", parsed_key, len(df)
    )

    return {
        "report_date": report_date.isoformat(),
        "raw_key": raw_key,
        "parsed_key": parsed_key,
        "row_count": int(len(df)),
        "action": "parsed_and_stored",
    }


@task()
def summarize(result: dict) -> dict:
    """Log a one-line summary so the workflow run page is easy to read."""
    logger.info(
        "EIA Table 9 ingestion: action=%s report_date=%s rows=%d "
        "raw_key=%s parsed_key=%s",
        result["action"],
        result["report_date"],
        result["row_count"],
        result["raw_key"],
        result["parsed_key"],
    )
    return result
