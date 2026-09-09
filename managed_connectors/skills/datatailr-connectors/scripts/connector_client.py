"""Vendor this module into a Datatailr job package; it contains no credentials."""

from __future__ import annotations

import base64
import os
from typing import Any

import requests


class ConnectorGatewayError(RuntimeError):
    pass


class ConnectorClient:
    def __init__(self, base_url: str | None = None, timeout: int = 30):
        environment = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "dev")
        self.base_url = (base_url or os.environ.get("DATATAILR_CONNECTOR_GATEWAY_URL") or f"http://connector-gateway/job/{environment}/connector-gateway").rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.request(
            method,
            self.base_url + path,
            json=payload,
            headers={"X-Datatailr-Connector-Client": "1"},
            timeout=self.timeout,
        )
        try:
            data = response.json()
        except ValueError as exc:
            raise ConnectorGatewayError(f"Connector gateway returned invalid JSON (HTTP {response.status_code})") from exc
        if not response.ok:
            raise ConnectorGatewayError(str(data.get("error") or f"Connector gateway returned HTTP {response.status_code}"))
        return data

    def connections(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/connections")["connections"]

    def capabilities(self) -> dict[str, Any]:
        return self._request("GET", "/v1/capabilities")["capabilities"]

    def query(self, capability: str, **parameters: Any) -> Any:
        return self._request("POST", "/v1/query", {"capability": capability, "parameters": parameters})["data"]

    def action(self, capability: str, **parameters: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/actions", {"capability": capability, "parameters": parameters})

    def recent_slack_threads(self, channel: str, limit: int = 20) -> list[dict[str, Any]]:
        return self.query("slack.threads.recent", channel=channel, limit=limit)

    def recent_hubspot_objects(self, object_type: str, *, modified_after: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        values: dict[str, Any] = {"object_type": object_type, "limit": limit}
        if modified_after:
            values["modified_after"] = modified_after
        return self.query("hubspot.objects.recent", **values)

    def recent_hubspot_activities(
        self,
        *,
        activity_types: list[str] | None = None,
        query: str | None = None,
        modified_after: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        values: dict[str, Any] = {"limit": limit}
        if activity_types:
            values["activity_types"] = activity_types
        if query:
            values["query"] = query
        if modified_after:
            values["modified_after"] = modified_after
        return self.query("hubspot.activities.recent", **values)

    def upcoming_hubspot_activities(
        self,
        *,
        days: int = 14,
        activity_types: list[str] | None = None,
        query: str | None = None,
        owner_id: str | None = None,
        association_type: str | None = None,
        association_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        values: dict[str, Any] = {"days": days, "limit": limit}
        for key, value in {
            "activity_types": activity_types,
            "query": query,
            "owner_id": owner_id,
            "association_type": association_type,
            "association_id": association_id,
        }.items():
            if value:
                values[key] = value
        return self.query("hubspot.activities.upcoming", **values)

    def github_repositories(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.query("github.repositories.list", limit=limit)

    def recent_github_issues(
        self,
        repository: str,
        *,
        state: str = "open",
        limit: int = 20,
        max_characters: int = 8000,
    ) -> list[dict[str, Any]]:
        return self.query(
            "github.issues.recent",
            repository=repository,
            state=state,
            limit=limit,
            max_characters=max_characters,
        )

    def recent_github_pull_requests(
        self,
        repository: str,
        *,
        state: str = "open",
        limit: int = 20,
        max_characters: int = 8000,
    ) -> list[dict[str, Any]]:
        return self.query(
            "github.pull_requests.recent",
            repository=repository,
            state=state,
            limit=limit,
            max_characters=max_characters,
        )

    def upcoming_outlook_events(
        self, *, days: int = 14, time_zone: str = "UTC", limit: int = 20
    ) -> list[dict[str, Any]]:
        return self.query(
            "outlook.calendar.events.upcoming",
            days=days,
            time_zone=time_zone,
            limit=limit,
        )

    def outlook_events(
        self, start: str, end: str, *, time_zone: str = "UTC", limit: int = 20
    ) -> list[dict[str, Any]]:
        return self.query(
            "outlook.calendar.events.range",
            start=start,
            end=end,
            time_zone=time_zone,
            limit=limit,
        )

    def outlook_availability(
        self,
        schedules: list[str],
        start: str,
        end: str,
        *,
        time_zone: str = "UTC",
        interval_minutes: int = 30,
    ) -> list[dict[str, Any]]:
        return self.query(
            "outlook.calendar.availability",
            schedules=schedules,
            start=start,
            end=end,
            time_zone=time_zone,
            interval_minutes=interval_minutes,
        )

    def post_slack_message(self, channel: str, text: str, *, idempotency_key: str, dry_run: bool = False) -> dict[str, Any]:
        return self.action("slack.messages.post", channel=channel, text=text, idempotency_key=idempotency_key, dry_run=dry_run)

    def upload_slack_file(
        self,
        channel: str,
        content: bytes,
        filename: str,
        *,
        idempotency_key: str,
        title: str | None = None,
        initial_comment: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Upload a document from memory; the gateway forwards it without storing it."""
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        values: dict[str, Any] = {
            "channel": channel,
            "filename": filename,
            "content_base64": base64.b64encode(content).decode("ascii"),
            "idempotency_key": idempotency_key,
            "dry_run": dry_run,
        }
        if title:
            values["title"] = title
        if initial_comment:
            values["initial_comment"] = initial_comment
        return self.action("slack.files.upload", **values)
