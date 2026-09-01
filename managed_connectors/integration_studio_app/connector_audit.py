"""Privacy-preserving connector audit events shared with Connector Gateway.

The audit database intentionally contains operational facts, not connector
records or credentials. Personal mailbox events use a stricter allowlist so a
future caller cannot accidentally persist a search, subject, address, or body.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


AUDIT_DB = Path(
    os.environ.get(
        "CONNECTOR_GATEWAY_AUDIT_DB",
        "/mnt/integration-studio/connector-gateway/audit.sqlite3",
    )
)
PERSONAL_CONNECTORS = {"gmail", "outlook", "zoom"}
MAX_EVENTS = max(1_000, int(os.environ.get("CONNECTOR_AUDIT_MAX_EVENTS", "20000")))

_COMMON_METADATA = {
    "data_mode",
    "days",
    "error_type",
    "http_status",
    "limit",
    "max_characters",
    "mode",
    "query_supplied",
    "surface",
}
_PERSONAL_METADATA = _COMMON_METADATA | {"connected", "selected"}
_SHARED_METADATA = _COMMON_METADATA | {
    "changed",
    "channel",
    "deleted",
    "dry_run",
    "fetched",
    "filter_supplied",
    "full",
    "idempotency_key",
    "legacy",
    "modified_after",
    "object_type",
    "scope",
    "selected",
    "target",
    "text_characters",
    "repository",
    "path",
    "ref",
    "state",
}


def sanitize_metadata(connector: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Return only explicitly approved scalar operational metadata."""
    allowed = _PERSONAL_METADATA if connector in PERSONAL_CONNECTORS else _SHARED_METADATA
    result: dict[str, Any] = {}
    for key, value in (metadata or {}).items():
        if key not in allowed or not isinstance(value, (str, int, float, bool, type(None))):
            continue
        result[key] = value
    return result


def _connect() -> sqlite3.Connection:
    AUDIT_DB.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(AUDIT_DB.parent, 0o700)
    conn = sqlite3.connect(AUDIT_DB, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS connector_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            at TEXT NOT NULL,
            user TEXT NOT NULL,
            connector TEXT NOT NULL,
            capability TEXT NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL,
            result_count INTEGER,
            duration_ms INTEGER,
            metadata TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS connector_events_at_idx
            ON connector_events(at DESC);
        CREATE INDEX IF NOT EXISTS connector_events_connector_at_idx
            ON connector_events(connector, at DESC);
        CREATE INDEX IF NOT EXISTS connector_events_user_at_idx
            ON connector_events(user, at DESC);
        """
    )
    conn.commit()
    os.chmod(AUDIT_DB, 0o600)
    return conn


def record_connector_event(
    *,
    user: str,
    connector: str,
    capability: str,
    operation: str,
    status: str,
    result_count: int | None = None,
    duration_ms: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Append an event without allowing audit availability to break a request."""
    if connector not in {"slack", "hubspot", "github", "gmail", "outlook", "zoom"}:
        return False
    safe = sanitize_metadata(connector, metadata)
    try:
        with _connect() as conn:
            conn.execute(
                """INSERT INTO connector_events(
                       at,user,connector,capability,operation,status,
                       result_count,duration_ms,metadata
                   ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    datetime.now(UTC).isoformat(),
                    str(user or "system")[:200],
                    connector,
                    str(capability or connector)[:200],
                    str(operation or "query")[:40],
                    str(status or "unknown")[:40],
                    result_count if isinstance(result_count, int) and result_count >= 0 else None,
                    duration_ms if isinstance(duration_ms, int) and duration_ms >= 0 else None,
                    json.dumps(safe, separators=(",", ":"), sort_keys=True),
                ),
            )
            conn.execute(
                """DELETE FROM connector_events
                   WHERE id <= COALESCE((SELECT MAX(id) FROM connector_events), 0) - ?""",
                (MAX_EVENTS,),
            )
            conn.commit()
        return True
    except (OSError, sqlite3.Error):
        return False


def list_connector_events(
    *, connector: str = "", user: str = "", status: str = "", limit: int = 200
) -> list[dict[str, Any]]:
    limit = min(max(int(limit), 1), 500)
    clauses: list[str] = []
    parameters: list[Any] = []
    if connector:
        clauses.append("connector=?")
        parameters.append(connector)
    if user:
        clauses.append("user=?")
        parameters.append(user)
    if status:
        clauses.append("status=?")
        parameters.append(status)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    try:
        with _connect() as conn:
            rows = conn.execute(
                """SELECT id,at,user,connector,capability,operation,status,
                          result_count,duration_ms,metadata
                   FROM connector_events"""
                + where
                + " ORDER BY id DESC LIMIT ?",
                [*parameters, limit],
            ).fetchall()
    except (OSError, sqlite3.Error):
        return []
    events = []
    for row in rows:
        item = dict(row)
        try:
            decoded = json.loads(item.pop("metadata") or "{}")
        except (TypeError, json.JSONDecodeError):
            decoded = {}
        item["metadata"] = sanitize_metadata(item["connector"], decoded)
        item["metadata_only"] = item["connector"] in PERSONAL_CONNECTORS
        events.append(item)
    return events
