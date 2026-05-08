"""Workflow tasks for the vendor-email AI inbox.

The pipeline is intentionally simple:

    fetch_new_emails  ->  summarize_with_ai  ->  publish_to_blob

Each stage is a separate ``@task`` so the platform shows the DAG cleanly
and resources can be tuned independently.

The dashboard reads the resulting blobs at:

    trading_dashboard/inbox/index.json     # most-recent-first list
    trading_dashboard/inbox/<id>.json      # one file per email + summary
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

from datatailr import task

from trading_dashboard.email_workflow.generator import generate_batch
from trading_dashboard.email_workflow.summarizer import summarize_emails


BLOB_PREFIX = os.environ.get("INBOX_BLOB_PREFIX", "trading_dashboard/inbox").strip("/")
INDEX_KEY = f"{BLOB_PREFIX}/index.json"
INDEX_MAX_ITEMS = int(os.environ.get("INBOX_INDEX_MAX", "200"))
BATCH_SIZE = int(os.environ.get("INBOX_BATCH_SIZE", "5"))


log = logging.getLogger("inbox")
log.setLevel(logging.INFO)


@task()
def fetch_new_emails() -> list[dict[str, Any]]:
    """Pull the next batch of emails from the (simulated) vendor inbox."""
    run_id = os.environ.get("DATATAILR_BATCH_RUN_ID") or dt.datetime.utcnow().isoformat()
    emails = generate_batch(batch_size=BATCH_SIZE, run_id=run_id)
    log.info("Fetched %d new vendor emails (run_id=%s)", len(emails), run_id)
    return emails


@task(memory="1g", cpu=1)
def summarize_with_ai(emails: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run each email through the AI summarizer."""
    out = summarize_emails(emails)
    log.info("Summarized %d emails", len(out))
    return out


@task()
def publish_to_blob(enriched: list[dict[str, Any]]) -> dict[str, Any]:
    """Write each email + summary to blob storage and update the index."""
    from datatailr import Blob

    blob = Blob()

    written: list[dict[str, Any]] = []
    for em in enriched:
        eid = em.get("id")
        if not eid:
            continue
        key = f"{BLOB_PREFIX}/{eid}.json"
        blob.put(key, json.dumps(em).encode("utf-8"))
        written.append({
            "id": eid,
            "received_at": em.get("received_at"),
            "from_name": em.get("from_name"),
            "ticker": em.get("ticker"),
            "subject": em.get("subject"),
            "sentiment": (em.get("ai_summary") or {}).get("sentiment", "neutral"),
            "summary": (em.get("ai_summary") or {}).get("summary", ""),
            "key": key,
        })

    existing: list[dict[str, Any]] = []
    try:
        if blob.exists(INDEX_KEY):
            existing = json.loads(blob.get(INDEX_KEY).decode("utf-8"))
    except Exception as exc:
        log.warning("Could not read existing index %s: %s", INDEX_KEY, exc)

    seen_ids = {e["id"] for e in existing}
    merged = [e for e in written if e["id"] not in seen_ids] + existing
    merged.sort(key=lambda x: x.get("received_at", ""), reverse=True)
    merged = merged[:INDEX_MAX_ITEMS]

    blob.put(INDEX_KEY, json.dumps(merged, indent=2).encode("utf-8"))

    log.info("Published %d emails; index now has %d entries", len(written), len(merged))
    return {
        "published": len(written),
        "index_size": len(merged),
        "index_key": INDEX_KEY,
    }
