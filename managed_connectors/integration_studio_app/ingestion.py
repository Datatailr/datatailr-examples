"""Incremental shared-connector ingestion for Integration Studio.

Connectors return normalized KnowledgeDocuments plus opaque checkpoints. The
manager commits documents first and advances checkpoints only after a successful
write, making retries idempotent and preventing gaps after partial failures.
Gmail, Outlook, and Zoom deliberately bypass this module and are fetched live per request.
"""

from __future__ import annotations

import base64
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html import unescape
from typing import Any
from urllib.parse import quote

import requests

from integration_studio_app.knowledge import KnowledgeDocument, KnowledgeIndex, get_index
from integration_studio_app.connector_audit import record_connector_event
from integration_studio_app.providers import ProviderError, _refresh_token
from integration_studio_app.storage import read_state, settings_for_user, update_state


SYNC_INTERVAL_SECONDS = max(300, int(os.environ.get("INTEGRATION_STUDIO_SYNC_INTERVAL", "900")))
FULL_RECONCILE_SECONDS = max(3600, int(os.environ.get("INTEGRATION_STUDIO_FULL_RECONCILE", "86400")))
MAX_SLACK_MESSAGES_PER_CHANNEL = int(os.environ.get("INTEGRATION_STUDIO_SLACK_MAX_MESSAGES", "10000"))
MAX_MAIL_MESSAGES = int(os.environ.get("INTEGRATION_STUDIO_MAIL_MAX_MESSAGES", "1000"))
MAX_HUBSPOT_RECORDS = int(os.environ.get("INTEGRATION_STUDIO_HUBSPOT_MAX_RECORDS", "5000"))
_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"[ \t]+")


class IngestionError(RuntimeError):
    pass


class ApiClient:
    def __init__(self, headers: dict[str, str], timeout: float = 45):
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.timeout = timeout

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last = ""
        for attempt in range(6):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            except requests.RequestException as exc:
                last = str(exc)
                if attempt == 5:
                    raise IngestionError(f"Cannot reach connector API: {exc}") from exc
                time.sleep(min(2**attempt, 20))
                continue
            if response.status_code == 429 or response.status_code >= 500:
                last = f"HTTP {response.status_code}"
                retry = response.headers.get("Retry-After")
                try:
                    delay = float(retry) if retry else min(2**attempt, 30)
                except ValueError:
                    delay = min(2**attempt, 30)
                if attempt == 5:
                    break
                time.sleep(max(0.25, delay))
                continue
            return response
        raise IngestionError(f"Connector API remained unavailable ({last})")

    def json(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        response = self.request(method, url, **kwargs)
        try:
            data = response.json()
        except ValueError as exc:
            raise IngestionError(f"Connector returned HTTP {response.status_code} with invalid JSON") from exc
        if not response.ok:
            error = data.get("error") or data.get("message") or response.reason
            if isinstance(error, dict):
                error = error.get("message") or json.dumps(error)
            raise IngestionError(f"Connector returned HTTP {response.status_code}: {error}")
        return data


@dataclass(slots=True)
class ConnectorResult:
    documents: list[KnowledgeDocument]
    cursors: dict[str, str] = field(default_factory=dict)
    deleted_external_ids: list[str] = field(default_factory=list)
    replace_scope: bool = False
    detail: str = ""


def _parse_json(response: requests.Response, provider: str) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise IngestionError(f"{provider} returned invalid JSON") from exc
    if not response.ok:
        message = data.get("error") or data.get("message") or response.reason
        if isinstance(message, dict):
            message = message.get("message") or json.dumps(message)
        raise IngestionError(f"{provider} returned HTTP {response.status_code}: {message}")
    return data


def _iso_from_slack(ts: str) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC).isoformat()
    except (TypeError, ValueError, OSError):
        return datetime.now(UTC).isoformat()


def _plain_html(value: str) -> str:
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", value)
    text = _TAG.sub(" ", text)
    return _SPACE.sub(" ", unescape(text)).strip()


class SlackConnector:
    base = "https://slack.com/api"

    def __init__(self, token: str, index: KnowledgeIndex):
        self.client = ApiClient({"Authorization": f"Bearer {token}"})
        self.index = index
        self.scope = "workspace"

    def _call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        data = self.client.json("GET", f"{self.base}/{method}", params=params)
        if not data.get("ok"):
            needed = f"; required scope: {data.get('needed')}" if data.get("needed") else ""
            raise IngestionError(f"Slack {method} failed: {data.get('error', 'unknown_error')}{needed}")
        return data

    def _paginate(self, method: str, params: dict[str, Any], key: str, limit: int = 200) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        cursor = ""
        while True:
            data = self._call(method, {**params, "limit": limit, "cursor": cursor})
            values.extend(value for value in data.get(key, []) if isinstance(value, dict))
            cursor = str((data.get("response_metadata") or {}).get("next_cursor") or "")
            if not cursor:
                return values

    def _history(self, channel_id: str, oldest: str | None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"channel": channel_id, "inclusive": "false"}
        if oldest:
            params["oldest"] = oldest
        messages = self._paginate("conversations.history", params, "messages", limit=200)
        return messages[:MAX_SLACK_MESSAGES_PER_CHANNEL]

    def _replies(self, channel_id: str, ts: str) -> list[dict[str, Any]]:
        values = self._paginate(
            "conversations.replies", {"channel": channel_id, "ts": ts}, "messages", limit=200
        )
        return values[1:] if values and str(values[0].get("ts")) == ts else values

    @staticmethod
    def _render_message(message: dict[str, Any]) -> str:
        author = message.get("user") or message.get("username") or message.get("bot_id") or "unknown"
        ts = str(message.get("ts") or "0")
        at = _iso_from_slack(ts)
        return f"[{at}] {author}: {str(message.get('text') or '').strip()}"

    def load(self, full: bool) -> ConnectorResult:
        channels = self._paginate(
            "users.conversations",
            {"types": "public_channel", "exclude_archived": "true"},
            "channels",
        )
        if not channels:
            raise IngestionError("The Slack bot is not a member of a readable public channel")
        docs: list[KnowledgeDocument] = []
        cursors: dict[str, str] = {}
        warnings: list[str] = []
        for channel in channels:
            channel_id = str(channel.get("id") or "")
            channel_name = str(channel.get("name") or channel_id)
            oldest = None if full else self.index.cursor("slack", self.scope, f"channel:{channel_id}")
            try:
                messages = self._history(channel_id, oldest)
            except IngestionError as exc:
                warnings.append(f"#{channel_name}: {exc}")
                continue
            max_ts = oldest or "0"
            for message in sorted(messages, key=lambda item: float(item.get("ts") or 0)):
                subtype = str(message.get("subtype") or "")
                if subtype in {"channel_join", "channel_leave", "channel_topic", "channel_purpose"}:
                    continue
                text = str(message.get("text") or "").strip()
                ts = str(message.get("ts") or "")
                if not text or not ts:
                    continue
                max_ts = ts if float(ts) > float(max_ts or 0) else max_ts
                rendered = [self._render_message(message)]
                if int(message.get("reply_count") or 0) > 0:
                    try:
                        rendered.extend(self._render_message(reply) for reply in self._replies(channel_id, ts))
                    except IngestionError as exc:
                        warnings.append(f"#{channel_name} thread {ts}: {exc}")
                updated = str(message.get("latest_reply") or ts)
                docs.append(
                    KnowledgeDocument(
                        source="slack",
                        scope=self.scope,
                        external_id=f"{channel_id}:{ts}",
                        title=f"Slack #{channel_name} · {_iso_from_slack(ts)[:10]}",
                        content=f"Slack channel: #{channel_name}\n" + "\n".join(rendered),
                        ref=f"slack://{channel_id}/{ts}",
                        source_updated_at=_iso_from_slack(updated),
                        metadata={
                            "channel_id": channel_id,
                            "channel": channel_name,
                            "thread_ts": ts,
                            "reply_count": int(message.get("reply_count") or 0),
                        },
                    )
                )
            if max_ts and max_ts != "0":
                cursors[f"channel:{channel_id}"] = max_ts
        detail = f"{len(channels)} joined public channels"
        if warnings:
            detail += "; " + "; ".join(warnings[:10])
        return ConnectorResult(docs, cursors, replace_scope=full and not warnings, detail=detail)


_NEXT_ACTIVITY_PROPERTIES = [
    "notes_next_activity_date",
    "hs_notes_next_activity",
    "hs_notes_next_activity_type",
    "notes_last_updated",
]
HUBSPOT_CORE_OBJECTS: dict[str, list[str]] = {
    "companies": [
        "name", "domain", "industry", "city", "country", "numberofemployees",
        "annualrevenue", "lifecyclestage", "description", "hubspot_owner_id",
        "hs_lastmodifieddate", *_NEXT_ACTIVITY_PROPERTIES,
    ],
    "contacts": [
        "firstname", "lastname", "email", "jobtitle", "phone", "company",
        "lifecyclestage", "hs_lead_status", "hubspot_owner_id", "lastmodifieddate",
        *_NEXT_ACTIVITY_PROPERTIES,
    ],
    "deals": [
        "dealname", "dealstage", "pipeline", "amount", "closedate", "description",
        "hubspot_owner_id", "hs_lastmodifieddate", *_NEXT_ACTIVITY_PROPERTIES,
    ],
    "tickets": [
        "subject", "content", "hs_pipeline", "hs_pipeline_stage", "hs_ticket_priority",
        "hs_ticket_category", "hubspot_owner_id", "hs_lastmodifieddate",
        *_NEXT_ACTIVITY_PROPERTIES,
    ],
}
HUBSPOT_ACTIVITY_OBJECTS: dict[str, list[str]] = {
    "calls": [
        "hs_timestamp", "hs_call_title", "hs_call_body", "hs_call_status",
        "hs_call_direction", "hs_call_disposition", "hs_call_duration",
        "hs_activity_type", "hubspot_owner_id", "hs_lastmodifieddate",
    ],
    "meetings": [
        "hs_timestamp", "hs_meeting_title", "hs_meeting_body",
        "hs_internal_meeting_notes", "hs_meeting_start_time", "hs_meeting_end_time",
        "hs_meeting_outcome", "hs_activity_type", "hs_meeting_location",
        "hs_meeting_external_url", "hubspot_owner_id", "hs_lastmodifieddate",
    ],
    "notes": [
        "hs_timestamp", "hs_note_body", "hubspot_owner_id", "hs_lastmodifieddate",
    ],
    "tasks": [
        "hs_timestamp", "hs_task_subject", "hs_task_body", "hs_task_status",
        "hs_task_priority", "hs_task_type", "hubspot_owner_id", "hs_lastmodifieddate",
    ],
}
HUBSPOT_OBJECTS = {**HUBSPOT_CORE_OBJECTS, **HUBSPOT_ACTIVITY_OBJECTS}
HUBSPOT_ASSOCIATION_TARGETS = ("contacts", "companies", "deals", "tickets")
_ACTIVITY_TITLE_PROPERTIES = {
    "calls": "hs_call_title",
    "meetings": "hs_meeting_title",
    "notes": "hs_note_body",
    "tasks": "hs_task_subject",
}
_ACTIVITY_BODY_PROPERTY_NAMES = {
    "hs_call_body", "hs_meeting_body", "hs_internal_meeting_notes",
    "hs_note_body", "hs_task_body",
}


class HubSpotConnector:
    def __init__(self, config: dict[str, Any], index: KnowledgeIndex):
        self.base = str(config.get("base_url") or "https://api.hubapi.com").rstrip("/")
        self.client = ApiClient({"Authorization": f"Bearer {config.get('access_token', '')}"})
        self.index = index
        self.scope = "portal"

    def _list(self, object_type: str) -> list[dict[str, Any]]:
        url = f"{self.base}/crm/v3/objects/{object_type}"
        params: dict[str, Any] = {
            "limit": 100,
            "archived": "false",
            "properties": ",".join(HUBSPOT_OBJECTS[object_type]),
        }
        values: list[dict[str, Any]] = []
        while True:
            data = self.client.json("GET", url, params=params)
            values.extend(value for value in data.get("results", []) if isinstance(value, dict))
            if len(values) >= MAX_HUBSPOT_RECORDS:
                return values[:MAX_HUBSPOT_RECORDS]
            after = (((data.get("paging") or {}).get("next") or {}).get("after"))
            if not after:
                return values
            params["after"] = after

    def _search(self, object_type: str, modified_after: str) -> list[dict[str, Any]]:
        """Read records changed after a millisecond checkpoint."""
        url = f"{self.base}/crm/v3/objects/{object_type}/search"
        property_name = "lastmodifieddate" if object_type == "contacts" else "hs_lastmodifieddate"
        body: dict[str, Any] = {
            "filterGroups": [
                {"filters": [{"propertyName": property_name, "operator": "GT", "value": modified_after}]}
            ],
            "sorts": [property_name],
            "properties": HUBSPOT_OBJECTS[object_type],
            "limit": 200,
        }
        values: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            if after:
                body["after"] = after
            data = self.client.json("POST", url, json=body)
            values.extend(value for value in data.get("results", []) if isinstance(value, dict))
            if len(values) >= MAX_HUBSPOT_RECORDS:
                return values[:MAX_HUBSPOT_RECORDS]
            after = str((((data.get("paging") or {}).get("next") or {}).get("after")) or "")
            if not after:
                return values

    def _activity_associations(
        self, object_type: str, record_ids: list[str]
    ) -> tuple[dict[str, dict[str, list[str]]], list[str]]:
        """Batch-read CRM records associated with an activity."""
        result = {
            record_id: {target: [] for target in HUBSPOT_ASSOCIATION_TARGETS}
            for record_id in record_ids
        }
        warnings: list[str] = []
        for target in HUBSPOT_ASSOCIATION_TARGETS:
            for offset in range(0, len(record_ids), 1000):
                chunk = record_ids[offset : offset + 1000]
                try:
                    data = self.client.json(
                        "POST",
                        f"{self.base}/crm/v3/associations/{object_type}/{target}/batch/read",
                        json={"inputs": [{"id": record_id} for record_id in chunk]},
                    )
                except IngestionError as exc:
                    warnings.append(f"{object_type}->{target}: {exc}")
                    continue
                for item in data.get("results") or []:
                    if not isinstance(item, dict):
                        continue
                    from_id = str((item.get("from") or {}).get("id") or "")
                    if from_id not in result:
                        continue
                    result[from_id][target] = sorted(
                        {
                            str(value.get("id") or "")
                            for value in item.get("to") or []
                            if isinstance(value, dict) and value.get("id")
                        }
                    )
        return result, warnings

    @staticmethod
    def _normalized_properties(properties: dict[str, Any]) -> dict[str, str]:
        values: dict[str, str] = {}
        for key, raw in properties.items():
            if raw in (None, ""):
                continue
            value = str(raw)
            if key in _ACTIVITY_BODY_PROPERTY_NAMES:
                value = _plain_html(value)
            values[str(key)] = value[:65_536]
        return values

    @staticmethod
    def _modified_millis(record: dict[str, Any]) -> str:
        properties = record.get("properties") or {}
        value = str(
            record.get("updatedAt")
            or properties.get("hs_lastmodifieddate")
            or properties.get("lastmodifieddate")
            or ""
        )
        if value.isdigit():
            return value
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return str(int(parsed.timestamp() * 1000))
        except ValueError:
            return "0"

    @staticmethod
    def _title(object_type: str, properties: dict[str, Any], record_id: str) -> str:
        if object_type == "companies":
            value = properties.get("name") or properties.get("domain")
        elif object_type == "contacts":
            value = " ".join(filter(None, [properties.get("firstname"), properties.get("lastname")])) or properties.get("email")
        elif object_type == "deals":
            value = properties.get("dealname")
        elif object_type == "tickets":
            value = properties.get("subject")
        else:
            value = properties.get(_ACTIVITY_TITLE_PROPERTIES[object_type])
            if object_type == "notes" and value:
                value = _plain_html(str(value))[:100]
        return f"HubSpot {object_type[:-1].title()} · {value or record_id}"

    @staticmethod
    def _activity_metadata(
        object_type: str,
        record_id: str,
        properties: dict[str, str],
        associations: dict[str, list[str]],
    ) -> dict[str, Any]:
        timestamp = str(
            properties.get("hs_meeting_start_time")
            or properties.get("hs_timestamp")
            or ""
        )
        return {
            "object_type": object_type,
            "activity_type": object_type,
            "id": record_id,
            "timestamp": timestamp,
            "start_time": str(properties.get("hs_meeting_start_time") or timestamp),
            "end_time": str(properties.get("hs_meeting_end_time") or ""),
            "status": str(properties.get("hs_task_status") or properties.get("hs_call_status") or ""),
            "outcome": str(properties.get("hs_meeting_outcome") or properties.get("hs_call_disposition") or ""),
            "owner_id": str(properties.get("hubspot_owner_id") or ""),
            "associations": associations,
            "properties": properties,
        }

    def load(self, full: bool) -> ConnectorResult:
        # Search checkpoints make routine syncs cheap; the daily paginated list
        # reconciliation catches deletions and permission/property drift that a
        # timestamp-only feed cannot observe.
        docs: list[KnowledgeDocument] = []
        warnings: list[str] = []
        association_warnings: list[str] = []
        cursors: dict[str, str] = {}
        successful_reads = 0
        for object_type in HUBSPOT_OBJECTS:
            try:
                previous = None if full else self.index.cursor(
                    "hubspot", self.scope, f"modified:{object_type}"
                )
                records = self._list(object_type) if not previous else self._search(object_type, previous)
            except IngestionError as exc:
                warnings.append(f"{object_type}: {exc}")
                continue
            # An empty incremental page is still a successful provider read.
            # Track request success independently from changed document count so
            # a quiet portal is not reported as an ingestion outage merely
            # because another object type is outside the token's scopes.
            successful_reads += 1
            associations: dict[str, dict[str, list[str]]] = {}
            if object_type in HUBSPOT_ACTIVITY_OBJECTS and records:
                associations, related_warnings = self._activity_associations(
                    object_type,
                    [str(record.get("id") or "") for record in records if record.get("id")],
                )
                association_warnings.extend(related_warnings)
            latest = previous or "0"
            for record in records:
                record_id = str(record.get("id") or "")
                props = self._normalized_properties(record.get("properties") or {})
                rendered = [f"HubSpot object: {object_type[:-1]}"]
                rendered.extend(f"{key}: {value}" for key, value in props.items() if value not in (None, ""))
                updated = str(record.get("updatedAt") or props.get("hs_lastmodifieddate") or props.get("lastmodifieddate") or datetime.now(UTC).isoformat())
                metadata: dict[str, Any] = {
                    "object_type": object_type,
                    "id": record_id,
                    "properties": props,
                }
                if object_type in HUBSPOT_ACTIVITY_OBJECTS:
                    metadata = self._activity_metadata(
                        object_type,
                        record_id,
                        props,
                        associations.get(
                            record_id,
                            {target: [] for target in HUBSPOT_ASSOCIATION_TARGETS},
                        ),
                    )
                docs.append(
                    KnowledgeDocument(
                        source="hubspot",
                        scope=self.scope,
                        external_id=f"{object_type}:{record_id}",
                        title=self._title(object_type, props, record_id),
                        content="\n".join(rendered),
                        ref=f"hubspot://{object_type}/{record_id}",
                        source_updated_at=updated,
                        metadata=metadata,
                    )
                )
                modified = self._modified_millis(record)
                if int(modified or 0) > int(latest or 0):
                    latest = modified
            if latest and latest != "0":
                cursors[f"modified:{object_type}"] = latest
        if successful_reads == 0 and warnings:
            raise IngestionError("No HubSpot object could be read: " + "; ".join(warnings))
        return ConnectorResult(
            docs,
            cursors,
            replace_scope=full and not warnings,
            detail="; ".join([*warnings, *association_warnings][:10]),
        )


def _gmail_parts(payload: dict[str, Any]) -> list[str]:
    parts: list[str] = []
    mime = str(payload.get("mimeType") or "")
    body = payload.get("body") or {}
    encoded = str(body.get("data") or "")
    if encoded and mime in {"text/plain", "text/html"}:
        try:
            decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode(errors="replace")
            parts.append(_plain_html(decoded) if mime == "text/html" else decoded)
        except (ValueError, UnicodeError):
            pass
    for child in payload.get("parts") or []:
        if isinstance(child, dict) and not child.get("filename"):
            parts.extend(_gmail_parts(child))
    return parts


class GmailConnector:
    base = "https://gmail.googleapis.com/gmail/v1/users/me"

    def __init__(self, token: str, user: str, index: KnowledgeIndex):
        self.client = ApiClient({"Authorization": f"Bearer {token}"})
        self.user = user
        self.scope = user
        self.index = index

    def _message(self, message_id: str) -> tuple[KnowledgeDocument, str]:
        item = self.client.json("GET", f"{self.base}/messages/{quote(message_id)}", params={"format": "full"})
        headers = {
            str(value.get("name") or "").casefold(): str(value.get("value") or "")
            for value in (item.get("payload") or {}).get("headers", [])
            if isinstance(value, dict)
        }
        body = "\n".join(value.strip() for value in _gmail_parts(item.get("payload") or {}) if value.strip())
        internal = int(item.get("internalDate") or 0)
        updated = datetime.fromtimestamp(internal / 1000, tz=UTC).isoformat() if internal else datetime.now(UTC).isoformat()
        content = (
            f"From: {headers.get('from', '')}\nTo: {headers.get('to', '')}\n"
            f"Date: {headers.get('date', updated)}\nSubject: {headers.get('subject', '(no subject)')}\n\n"
            f"{body or item.get('snippet', '')}"
        )[:200_000]
        doc = KnowledgeDocument(
            source="gmail",
            scope=self.scope,
            external_id=message_id,
            title=f"Gmail · {headers.get('subject', '(no subject)')}",
            content=content,
            ref=f"gmail://message/{message_id}",
            source_updated_at=updated,
            acl_users=[self.user],
            metadata={"message_id": message_id, "thread_id": item.get("threadId", "")},
        )
        return doc, str(item.get("historyId") or "")

    def _full(self) -> ConnectorResult:
        ids: list[str] = []
        token = ""
        while len(ids) < MAX_MAIL_MESSAGES:
            params: dict[str, Any] = {"maxResults": min(500, MAX_MAIL_MESSAGES - len(ids))}
            if token:
                params["pageToken"] = token
            data = self.client.json("GET", f"{self.base}/messages", params=params)
            ids.extend(str(item.get("id")) for item in data.get("messages", []) if item.get("id"))
            token = str(data.get("nextPageToken") or "")
            if not token:
                break
        docs: list[KnowledgeDocument] = []
        latest_history = ""
        for message_id in ids:
            doc, history_id = self._message(message_id)
            docs.append(doc)
            if history_id and (not latest_history or int(history_id) > int(latest_history)):
                latest_history = history_id
        return ConnectorResult(
            docs,
            {"history_id": latest_history} if latest_history else {},
            replace_scope=True,
            detail=f"full mailbox snapshot capped at {MAX_MAIL_MESSAGES} messages",
        )

    def load(self, full: bool) -> ConnectorResult:
        history_id = None if full else self.index.cursor("gmail", self.scope, "history_id")
        if not history_id:
            return self._full()
        added: set[str] = set()
        deleted: set[str] = set()
        page = ""
        latest = history_id
        while True:
            params: dict[str, Any] = {"startHistoryId": history_id, "maxResults": 500}
            if page:
                params["pageToken"] = page
            response = self.client.request("GET", f"{self.base}/history", params=params)
            if response.status_code == 404:
                return self._full()
            data = _parse_json(response, "Gmail")
            for event in data.get("history") or []:
                for value in event.get("messagesAdded") or []:
                    message = value.get("message") or {}
                    if message.get("id"):
                        added.add(str(message["id"]))
                for value in event.get("messagesDeleted") or []:
                    message = value.get("message") or {}
                    if message.get("id"):
                        deleted.add(str(message["id"]))
            latest = str(data.get("historyId") or latest)
            page = str(data.get("nextPageToken") or "")
            if not page:
                break
        docs = [self._message(message_id)[0] for message_id in sorted(added - deleted)]
        return ConnectorResult(docs, {"history_id": latest}, sorted(deleted), detail="Gmail history delta")


class OutlookConnector:
    graph = "https://graph.microsoft.com/v1.0"
    folders = ("inbox", "sentitems")

    def __init__(self, token: str, user: str, index: KnowledgeIndex):
        self.client = ApiClient(
            {
                "Authorization": f"Bearer {token}",
                "Prefer": 'outlook.body-content-type="text", IdType="ImmutableId"',
            }
        )
        self.user = user
        self.scope = user
        self.index = index

    @staticmethod
    def _address(value: Any) -> str:
        item = value.get("emailAddress", {}) if isinstance(value, dict) else {}
        name, address = str(item.get("name") or ""), str(item.get("address") or "")
        return f"{name} <{address}>" if name and address else address or name

    def _document(self, value: dict[str, Any], folder: str) -> KnowledgeDocument:
        message_id = str(value.get("id") or "")
        recipients = ", ".join(self._address(item) for item in value.get("toRecipients") or [])
        body = value.get("body") or {}
        content = str(body.get("content") or value.get("bodyPreview") or "")
        if str(body.get("contentType") or "").casefold() == "html":
            content = _plain_html(content)
        updated = str(value.get("lastModifiedDateTime") or value.get("receivedDateTime") or datetime.now(UTC).isoformat())
        subject = str(value.get("subject") or "(no subject)")
        rendered = (
            f"Folder: {folder}\nFrom: {self._address(value.get('from'))}\nTo: {recipients}\n"
            f"Received: {value.get('receivedDateTime', '')}\nSubject: {subject}\n\n{content}"
        )[:200_000]
        return KnowledgeDocument(
            source="outlook",
            scope=self.scope,
            external_id=f"{folder}:{message_id}",
            title=f"Outlook · {subject}",
            content=rendered,
            ref=str(value.get("webLink") or f"outlook://{folder}/{message_id}"),
            source_updated_at=updated,
            acl_users=[self.user],
            metadata={"message_id": message_id, "folder": folder},
        )

    def load(self, full: bool) -> ConnectorResult:
        docs: list[KnowledgeDocument] = []
        deleted: list[str] = []
        cursors: dict[str, str] = {}
        any_delta = False
        for folder in self.folders:
            cursor = None if full else self.index.cursor("outlook", self.scope, f"delta:{folder}")
            url = cursor or f"{self.graph}/me/mailFolders/{folder}/messages/delta"
            params: dict[str, Any] | None = None if cursor else {
                "$select": "id,subject,from,toRecipients,receivedDateTime,lastModifiedDateTime,body,bodyPreview,webLink",
                "$top": min(MAX_MAIL_MESSAGES, 250),
            }
            seen = 0
            while url and seen < MAX_MAIL_MESSAGES:
                data = self.client.json("GET", url, params=params)
                for value in data.get("value") or []:
                    if not isinstance(value, dict):
                        continue
                    message_id = str(value.get("id") or "")
                    if value.get("@removed"):
                        deleted.append(f"{folder}:{message_id}")
                    elif message_id:
                        docs.append(self._document(value, folder))
                    seen += 1
                next_url = str(data.get("@odata.nextLink") or "")
                delta_url = str(data.get("@odata.deltaLink") or "")
                if delta_url:
                    cursors[f"delta:{folder}"] = delta_url
                    any_delta = True
                url = next_url
                params = None
        return ConnectorResult(
            docs,
            cursors,
            deleted,
            replace_scope=full and any_delta,
            detail="Microsoft Graph folder delta",
        )


class SyncManager:
    def __init__(self, index: KnowledgeIndex | None = None):
        self.index = index or get_index()
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self._threads: dict[str, threading.Thread] = {}
        self._scheduler_started = False

    @staticmethod
    def scope(source: str, user: str | None) -> str:
        return user or "workspace" if source == "slack" else user or "portal"

    def _lock(self, key: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def _oauth(self, source: str, user: str, state: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        token = dict(state.get("users", {}).get(user, {}).get("tokens", {}).get(source) or {})
        if not token:
            raise IngestionError(f"{source.title()} is not connected for {user}")
        try:
            refreshed = _refresh_token(source, settings_for_user(state, user)[source], token)
        except ProviderError as exc:
            raise IngestionError(str(exc)) from exc
        if refreshed != token:
            update_state(
                lambda current: current["users"].setdefault(user, {}).setdefault("tokens", {}).__setitem__(source, refreshed)
            )
        return str(refreshed.get("access_token") or ""), refreshed

    def _connector(self, source: str, user: str | None, state: dict[str, Any]):
        if source == "slack":
            cfg = state["settings"]["slack"]
            if not cfg.get("bot_token"):
                raise IngestionError("Slack is not configured")
            return SlackConnector(str(cfg["bot_token"]), self.index)
        if source == "hubspot":
            cfg = state["settings"]["hubspot"]
            if not cfg.get("access_token"):
                raise IngestionError("HubSpot is not configured")
            return HubSpotConnector(cfg, self.index)
        if source in {"gmail", "outlook", "zoom"}:
            raise IngestionError(f"{source.title()} is live-only and must never be ingested")
        raise IngestionError(f"Unknown source {source}")

    def sync(
        self,
        source: str,
        user: str | None = None,
        *,
        force_full: bool = False,
        actor: str | None = None,
    ) -> dict[str, Any]:
        scope = self.scope(source, user)
        key = f"{source}:{scope}"
        lock = self._lock(key)
        if not lock.acquire(blocking=False):
            return {"ok": False, "source": source, "scope": scope, "reason": "sync already running"}
        full = force_full or self.index.cursor(source, scope, "last_success") is None
        run_id = self.index.start_run(source, scope, "full" if full else "incremental")
        started = time.monotonic()
        try:
            state = read_state()
            connector = self._connector(source, user, state)
            result = connector.load(full)
            changed, reconciled_deleted, embedding_note = self.index.upsert_documents(
                result.documents,
                state["settings"],
                run_id=run_id,
                replace_scope=result.replace_scope,
                source=source,
                scope=scope,
            )
            deleted = reconciled_deleted + self.index.delete_external_ids(
                source, scope, result.deleted_external_ids
            )
            for cursor_key, cursor_value in result.cursors.items():
                if cursor_value:
                    self.index.set_cursor(source, scope, cursor_key, cursor_value)
            self.index.set_cursor(source, scope, "last_success", datetime.now(UTC).isoformat())
            if full or result.replace_scope:
                self.index.set_cursor(source, scope, "last_full", datetime.now(UTC).isoformat())
            self.index.finish_run(
                run_id,
                "ok",
                fetched=len(result.documents),
                changed=changed,
                deleted=deleted,
                detail="; ".join(value for value in (result.detail, embedding_note) if value),
            )
            record_connector_event(
                user=actor or "system",
                connector=source,
                capability=f"{source}.sync",
                operation="sync",
                status="succeeded",
                result_count=len(result.documents),
                duration_ms=round((time.monotonic() - started) * 1000),
                metadata={
                    "surface": "integration-studio",
                    "data_mode": "indexed",
                    "mode": "full" if full else "incremental",
                    "scope": scope,
                    "full": full,
                    "fetched": len(result.documents),
                    "changed": changed,
                    "deleted": deleted,
                },
            )
            return {
                "ok": True,
                "source": source,
                "scope": scope,
                "mode": "full" if full else "incremental",
                "fetched": len(result.documents),
                "changed": changed,
                "deleted": deleted,
                "detail": result.detail,
            }
        except Exception as exc:
            self.index.finish_run(run_id, "error", detail=str(exc))
            record_connector_event(
                user=actor or "system",
                connector=source,
                capability=f"{source}.sync",
                operation="sync",
                status="failed",
                duration_ms=round((time.monotonic() - started) * 1000),
                metadata={
                    "surface": "integration-studio",
                    "data_mode": "indexed",
                    "mode": "full" if full else "incremental",
                    "scope": scope,
                    "full": full,
                    "error_type": type(exc).__name__,
                },
            )
            return {"ok": False, "source": source, "scope": scope, "error": str(exc)}
        finally:
            lock.release()

    def trigger(
        self,
        source: str,
        user: str | None = None,
        *,
        force_full: bool = False,
        actor: str | None = None,
    ) -> dict[str, Any]:
        scope = self.scope(source, user)
        key = f"{source}:{scope}"
        with self._guard:
            current = self._threads.get(key)
            if current and current.is_alive():
                return {"started": False, "reason": "sync already running", "source": source, "scope": scope}

            def worker() -> None:
                self.sync(source, user, force_full=force_full, actor=actor)

            thread = threading.Thread(target=worker, name=f"integration-sync-{source}-{scope}", daemon=True)
            self._threads[key] = thread
            thread.start()
        return {"started": True, "source": source, "scope": scope}

    def ensure_fresh(self, sources: list[str], user: str) -> list[dict[str, Any]]:
        state = read_state()
        started: list[dict[str, Any]] = []
        for source in sources:
            if source in {"slack", "hubspot"}:
                configured = bool(state["settings"].get(source, {}).get("bot_token" if source == "slack" else "access_token"))
                if configured and not self.index.cursor(source, self.scope(source, None), "last_success"):
                    started.append(self.trigger(source, actor=user))
        return started

    def start_scheduler(self) -> None:
        if self._scheduler_started or os.environ.get("INTEGRATION_STUDIO_DISABLE_SCHEDULER") == "1":
            return
        with self._guard:
            if self._scheduler_started:
                return
            self.index.purge_sources({"gmail", "outlook", "zoom"})
            self._scheduler_started = True

        def schedule() -> None:
            # Let gunicorn finish booting and the persistent mount settle.
            time.sleep(5)
            while True:
                try:
                    state = read_state()
                    if state["settings"]["slack"].get("bot_token"):
                        self.trigger("slack", force_full=self._full_due("slack", "workspace"))
                    if state["settings"]["hubspot"].get("access_token"):
                        self.trigger("hubspot", force_full=self._full_due("hubspot", "portal"))
                except Exception:
                    pass
                time.sleep(SYNC_INTERVAL_SECONDS)

        threading.Thread(target=schedule, name="integration-sync-scheduler", daemon=True).start()

    def _full_due(self, source: str, scope: str) -> bool:
        value = self.index.cursor(source, scope, "last_full")
        if not value:
            return True
        try:
            return (datetime.now(UTC) - datetime.fromisoformat(value)).total_seconds() >= FULL_RECONCILE_SECONDS
        except ValueError:
            return True


_MANAGER: SyncManager | None = None
_MANAGER_LOCK = threading.Lock()


def get_sync_manager() -> SyncManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = SyncManager()
        return _MANAGER
