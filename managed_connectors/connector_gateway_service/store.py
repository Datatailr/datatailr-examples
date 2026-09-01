from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


KNOWLEDGE_DB = Path(os.environ.get("CONNECTOR_GATEWAY_KNOWLEDGE_DB", "/mnt/integration-studio/knowledge/knowledge.sqlite3"))
AUDIT_DB = Path(os.environ.get("CONNECTOR_GATEWAY_AUDIT_DB", "/mnt/integration-studio/connector-gateway/audit.sqlite3"))
_NUMBER = re.compile(r"^-?(?:\d+\.?\d*|\.\d+)$")


def principals(user: str, groups: Iterable[str]) -> list[str]:
    return [f"user:{user}", *[f"group:{group}" for group in sorted(set(groups)) if group]]


def _readonly() -> sqlite3.Connection:
    if not KNOWLEDGE_DB.exists():
        raise RuntimeError("The Integration Studio knowledge index is not available")
    conn = sqlite3.connect(f"file:{KNOWLEDGE_DB}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _audit() -> sqlite3.Connection:
    AUDIT_DB.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(AUDIT_DB.parent, 0o700)
    conn = sqlite3.connect(AUDIT_DB, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    conn.executescript(
        """CREATE TABLE IF NOT EXISTS actions(
        id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, user TEXT NOT NULL,
        capability TEXT NOT NULL, target TEXT NOT NULL, idempotency_key TEXT NOT NULL,
        status TEXT NOT NULL, detail TEXT NOT NULL,
        UNIQUE(user, capability, idempotency_key));
        CREATE TABLE IF NOT EXISTS queries(
        id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, user TEXT NOT NULL,
        capability TEXT NOT NULL, result_count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS connector_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, user TEXT NOT NULL,
        connector TEXT NOT NULL, capability TEXT NOT NULL, operation TEXT NOT NULL,
        status TEXT NOT NULL, result_count INTEGER, duration_ms INTEGER,
        metadata TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS connector_events_at_idx
            ON connector_events(at DESC);
        CREATE INDEX IF NOT EXISTS connector_events_connector_at_idx
            ON connector_events(connector, at DESC);
        CREATE INDEX IF NOT EXISTS connector_events_user_at_idx
            ON connector_events(user, at DESC);
        CREATE TABLE IF NOT EXISTS audit_migrations(
            name TEXT PRIMARY KEY, applied_at TEXT NOT NULL
        );"""
    )
    migrated = conn.execute(
        "SELECT 1 FROM audit_migrations WHERE name='legacy-query-action-v1'"
    ).fetchone()
    if migrated is None:
        for row in conn.execute("SELECT at,user,capability,result_count FROM queries"):
            connector = str(row["capability"]).split(".", 1)[0]
            if connector in SHARED_SOURCES:
                conn.execute(
                    """INSERT INTO connector_events(
                           at,user,connector,capability,operation,status,result_count,metadata
                       ) VALUES(?,?,?,?,?,?,?,?)""",
                    (
                        row["at"], row["user"], connector, row["capability"], "query",
                        "succeeded", row["result_count"], '{"legacy":true}',
                    ),
                )
        for row in conn.execute(
            "SELECT at,user,capability,target,idempotency_key,status FROM actions"
        ):
            connector = str(row["capability"]).split(".", 1)[0]
            if connector in SHARED_SOURCES:
                metadata = sanitize_audit_metadata(
                    connector,
                    {
                        "legacy": True,
                        "target": row["target"],
                        "idempotency_key": row["idempotency_key"],
                    },
                )
                conn.execute(
                    """INSERT INTO connector_events(
                           at,user,connector,capability,operation,status,metadata
                       ) VALUES(?,?,?,?,?,?,?)""",
                    (
                        row["at"], row["user"], connector, row["capability"], "action",
                        row["status"], json.dumps(metadata, separators=(",", ":"), sort_keys=True),
                    ),
                )
        conn.execute(
            "INSERT INTO audit_migrations(name,applied_at) VALUES(?,?)",
            ("legacy-query-action-v1", datetime.now(UTC).isoformat()),
        )
    conn.commit()
    os.chmod(AUDIT_DB, 0o600)
    return conn


SHARED_SOURCES = {"slack", "hubspot", "github"}
PERSONAL_SOURCES = {"gmail", "outlook", "zoom"}
MAX_AUDIT_EVENTS = max(1_000, int(os.environ.get("CONNECTOR_AUDIT_MAX_EVENTS", "20000")))
_COMMON_AUDIT_METADATA = {
    "calendar_operation", "data_mode", "days", "error_type", "http_status",
    "interval_minutes", "limit", "max_characters", "mode", "query_supplied",
    "schedule_count", "surface",
}
_PERSONAL_AUDIT_METADATA = _COMMON_AUDIT_METADATA | {"connected", "selected"}
_SHARED_AUDIT_METADATA = _COMMON_AUDIT_METADATA | {
    "activity_types", "association_type", "changed", "channel", "deleted", "dry_run", "fetched",
    "filter_supplied", "full", "idempotency_key", "legacy",
    "modified_after", "object_type", "scope", "selected", "target",
    "comment_characters", "file_bytes", "file_extension", "text_characters",
    "repository", "path", "ref", "state", "max_characters",
}


def sanitize_audit_metadata(connector: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Enforce a narrow schema, with an extra-strict personal-mail allowlist."""
    allowed = _PERSONAL_AUDIT_METADATA if connector in PERSONAL_SOURCES else _SHARED_AUDIT_METADATA
    return {
        key: value
        for key, value in (metadata or {}).items()
        if key in allowed and isinstance(value, (str, int, float, bool, type(None)))
    }


def visible_documents(user: str, groups: Iterable[str], source: str) -> list[dict[str, Any]]:
    """Return connector data visible to this signed-in gateway caller.

    Slack, HubSpot, and GitHub are administrator-managed workspace connectors. Their
    credential determines the upstream data boundary, while the Datatailr app
    and gateway ACLs determine who may consume it. GitHub queries bypass this
    index and fetch through a short-lived installation token; an empty GitHub
    index is therefore expected. Gmail, Outlook, and Zoom are deliberately
    rejected here because their records are fetched live.
    """
    if source not in SHARED_SOURCES:
        raise RuntimeError(f"{source.title()} is live-only and cannot be read from the knowledge database")
    sql = """SELECT id,source,title,content,ref,source_updated_at,metadata
             FROM documents WHERE source=?"""
    parameters: list[str] = [source]
    with _readonly() as conn:
        rows = conn.execute(sql, parameters).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        try:
            item["metadata"] = json.loads(item.get("metadata") or "{}")
        except (TypeError, json.JSONDecodeError):
            item["metadata"] = {}
        result.append(item)
    return result


def connection_counts(user: str, groups: Iterable[str]) -> dict[str, int]:
    return {source: len(visible_documents(user, groups, source)) for source in SHARED_SOURCES}


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
    """Append a safe operational event without failing the connector request."""
    if connector not in SHARED_SOURCES | PERSONAL_SOURCES:
        return False
    safe = sanitize_audit_metadata(connector, metadata)
    try:
        with _audit() as conn:
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
                (MAX_AUDIT_EVENTS,),
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
        with _audit() as conn:
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
        item["metadata"] = sanitize_audit_metadata(item["connector"], decoded)
        item["metadata_only"] = item["connector"] in PERSONAL_SOURCES
        events.append(item)
    return events


def visible_slack_channel(user: str, groups: Iterable[str], channel: str) -> dict[str, str] | None:
    target = channel.lstrip("#").casefold()
    for doc in visible_documents(user, groups, "slack"):
        metadata = doc["metadata"]
        channel_id = str(metadata.get("channel_id") or "")
        channel_name = str(metadata.get("channel") or "").lstrip("#")
        if target in {channel_id.casefold(), channel_name.casefold()} and channel_id:
            return {"id": channel_id, "name": channel_name}
    return None


def _properties(content: str) -> dict[str, str]:
    result = {}
    for line in str(content or "").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result


def _date(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


HUBSPOT_ACTIVITY_TYPES = {"calls", "meetings", "notes", "tasks"}


def _hubspot_activity_row(doc: dict[str, Any]) -> dict[str, Any]:
    metadata = doc.get("metadata") or {}
    properties = metadata.get("properties")
    if not isinstance(properties, dict):
        properties = _properties(doc.get("content") or "")
    activity_type = str(metadata.get("activity_type") or metadata.get("object_type") or "")
    timestamp = str(
        metadata.get("timestamp")
        or properties.get("hs_meeting_start_time")
        or properties.get("hs_timestamp")
        or ""
    )
    associations = metadata.get("associations")
    if not isinstance(associations, dict):
        associations = {}
    normalized_associations = {
        target: sorted(
            {
                str(value)
                for value in associations.get(target) or []
                if str(value).strip()
            }
        )
        for target in ("contacts", "companies", "deals", "tickets")
    }
    return {
        "id": str(metadata.get("id") or doc.get("id") or ""),
        "activity_type": activity_type,
        "title": str(doc.get("title") or ""),
        "text": str(doc.get("content") or "")[:20_000],
        "timestamp": timestamp,
        "start_time": str(metadata.get("start_time") or timestamp),
        "end_time": str(metadata.get("end_time") or properties.get("hs_meeting_end_time") or ""),
        "status": str(metadata.get("status") or properties.get("hs_task_status") or properties.get("hs_call_status") or ""),
        "outcome": str(metadata.get("outcome") or properties.get("hs_meeting_outcome") or properties.get("hs_call_disposition") or ""),
        "owner_id": str(metadata.get("owner_id") or properties.get("hubspot_owner_id") or ""),
        "associations": normalized_associations,
        "properties": {str(key): str(value) for key, value in properties.items()},
        "updated_at": str(doc.get("source_updated_at") or ""),
        "ref": str(doc.get("ref") or ""),
    }


def _hubspot_activity_matches(row: dict[str, Any], params: dict[str, Any]) -> bool:
    if row["activity_type"] not in set(params.get("activity_types") or HUBSPOT_ACTIVITY_TYPES):
        return False
    needle = str(params.get("query") or "").casefold()
    if needle and needle not in f"{row['title']}\n{row['text']}".casefold():
        return False
    owner_id = str(params.get("owner_id") or "")
    if owner_id and row["owner_id"] != owner_id:
        return False
    association_type = str(params.get("association_type") or "")
    association_id = str(params.get("association_id") or "")
    if association_type and association_id not in row["associations"].get(association_type, []):
        return False
    return True


def query(capability: str, params: dict[str, Any], *, user: str, groups: Iterable[str]) -> Any:
    if capability.startswith("slack."):
        docs = visible_documents(user, groups, "slack")
        channel = str(params.get("channel") or "").lstrip("#").casefold()
        needle = str(params.get("query") or "").casefold()
        rows = []
        for doc in docs:
            metadata = doc["metadata"]
            name = str(metadata.get("channel") or "").lstrip("#")
            if channel and name.casefold() != channel:
                continue
            if needle and needle not in str(doc["content"]).casefold() and needle not in str(doc["title"]).casefold():
                continue
            rows.append({
                "id": doc["id"], "channel": name, "text": str(doc["content"] or "")[:8000],
                "updated_at": doc["source_updated_at"], "reply_count": int(metadata.get("reply_count") or 0),
                "ref": doc["ref"],
            })
        result = sorted(rows, key=lambda row: _date(row["updated_at"]), reverse=True)[: params["limit"]]
    elif capability == "hubspot.objects.recent":
        docs = visible_documents(user, groups, "hubspot")
        object_type = params["object_type"]
        cutoff = _date(params["modified_after"]) if params.get("modified_after") else None
        result = []
        for doc in docs:
            if doc["metadata"].get("object_type") != object_type:
                continue
            if cutoff and _date(doc["source_updated_at"]) <= cutoff:
                continue
            properties = doc["metadata"].get("properties")
            if not isinstance(properties, dict):
                properties = _properties(doc["content"])
            result.append({
                "id": str(doc["metadata"].get("id") or doc["id"]), "object_type": object_type,
                "title": doc["title"],
                "properties": {str(key): str(value) for key, value in properties.items()},
                "updated_at": doc["source_updated_at"], "ref": doc["ref"],
            })
        result = sorted(result, key=lambda row: _date(row["updated_at"]), reverse=True)[: params["limit"]]
    elif capability == "hubspot.deals.summary":
        docs = [doc for doc in visible_documents(user, groups, "hubspot") if doc["metadata"].get("object_type") == "deals"]
        stages: dict[str, dict[str, Any]] = defaultdict(lambda: {"deal_count": 0, "total_amount": 0.0})
        total = 0.0
        won = 0
        open_count = 0
        latest = ""
        for doc in docs:
            properties = doc["metadata"].get("properties")
            if not isinstance(properties, dict):
                properties = _properties(doc["content"])
            values = {str(key).casefold(): str(value) for key, value in properties.items()}
            stage = values.get("dealstage") or "unknown"
            amount_text = str(values.get("amount") or "").replace(",", "").replace("$", "")
            amount = float(amount_text) if _NUMBER.match(amount_text) else 0.0
            stages[stage]["deal_count"] += 1
            stages[stage]["total_amount"] += amount
            total += amount
            won += int(stage.casefold() == "closedwon")
            open_count += int(stage.casefold() not in {"closedwon", "closedlost"})
            latest = max(latest, doc["source_updated_at"])
        result = {
            "total_deals": len(docs), "total_amount": total, "open_deals": open_count,
            "closed_won": won,
            "by_stage": [{"stage": stage, **values} for stage, values in sorted(stages.items(), key=lambda item: -item[1]["total_amount"])],
            "latest_update": latest,
        }
    elif capability in {"hubspot.activities.recent", "hubspot.activities.upcoming"}:
        docs = [
            doc for doc in visible_documents(user, groups, "hubspot")
            if doc["metadata"].get("object_type") in HUBSPOT_ACTIVITY_TYPES
        ]
        rows = [
            row for row in (_hubspot_activity_row(doc) for doc in docs)
            if _hubspot_activity_matches(row, params)
        ]
        if capability == "hubspot.activities.recent":
            cutoff = _date(params["modified_after"]) if params.get("modified_after") else None
            if cutoff:
                rows = [row for row in rows if _date(row["updated_at"]) > cutoff]
            result = sorted(
                rows,
                key=lambda row: max(_date(row["timestamp"]), _date(row["updated_at"])),
                reverse=True,
            )[: params["limit"]]
        else:
            now = datetime.now(UTC)
            end = now + timedelta(days=int(params.get("days") or 14))
            upcoming: list[dict[str, Any]] = []
            for row in rows:
                when = _date(row["start_time"] or row["timestamp"])
                if when < now or when >= end:
                    continue
                if row["activity_type"] == "tasks" and row["status"].casefold() == "completed":
                    continue
                if row["activity_type"] == "meetings" and row["outcome"].casefold() in {
                    "canceled", "cancelled", "completed", "no_show", "no show",
                }:
                    continue
                upcoming.append(row)
            result = sorted(
                upcoming,
                key=lambda row: _date(row["start_time"] or row["timestamp"]),
            )[: params["limit"]]
    else:
        source = capability.split(".", 1)[0]
        needle = str(params.get("query") or "").casefold()
        rows = []
        for doc in visible_documents(user, groups, source):
            if needle and needle not in str(doc["content"]).casefold() and needle not in str(doc["title"]).casefold():
                continue
            rows.append({"id": doc["id"], "title": doc["title"], "text": str(doc["content"] or "")[:8000], "updated_at": doc["source_updated_at"], "ref": doc["ref"]})
        result = sorted(rows, key=lambda row: _date(row["updated_at"]), reverse=True)[: params["limit"]]
    return result


def reserve_action(user: str, capability: str, target: str, key: str) -> dict[str, Any] | None:
    with _audit() as conn:
        try:
            conn.execute(
                "INSERT INTO actions(at,user,capability,target,idempotency_key,status,detail) VALUES(?,?,?,?,?,?,?)",
                (datetime.now(UTC).isoformat(), user, capability, target, key, "pending", "{}"),
            )
            conn.commit()
            return None
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT status,detail FROM actions WHERE user=? AND capability=? AND idempotency_key=?",
                (user, capability, key),
            ).fetchone()
            if row["status"] == "failed":
                retry = conn.execute(
                    """UPDATE actions SET at=?,target=?,status='pending',detail='{}'
                       WHERE user=? AND capability=? AND idempotency_key=? AND status='failed'""",
                    (datetime.now(UTC).isoformat(), target, user, capability, key),
                )
                conn.commit()
                if retry.rowcount:
                    return None
                row = conn.execute(
                    "SELECT status,detail FROM actions WHERE user=? AND capability=? AND idempotency_key=?",
                    (user, capability, key),
                ).fetchone()
    detail = json.loads(row["detail"] or "{}")
    detail.update({"duplicate": True, "status": row["status"]})
    return detail


def complete_action(user: str, capability: str, key: str, status: str, detail: dict[str, Any]) -> None:
    with _audit() as conn:
        conn.execute(
            "UPDATE actions SET status=?,detail=? WHERE user=? AND capability=? AND idempotency_key=?",
            (status, json.dumps(detail, separators=(",", ":")), user, capability, key),
        )
        conn.commit()
