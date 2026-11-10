"""Audit log: records in a memory buffer, written as JSONL files to blob storage."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import date, datetime, timezone
from typing import Callable

from llm_service.gateway.storage import BlobStore

logger = logging.getLogger("llm_service.audit")


def utc_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """Format `dt` as UTC ISO 8601 with milliseconds, for example 2026-11-10T12:00:00.123Z."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_time(text: str) -> datetime:
    """Parse a date (`2026-11-10`) or an ISO 8601 time into an aware UTC datetime.

    A time without a zone counts as UTC. Raise `ValueError` for any other text.
    """
    text = text.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        raise ValueError(f"not a date or an ISO time: {text!r}") from None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _day_of(ts: str) -> str:
    return parse_time(ts).strftime("%Y-%m-%d")


class AuditLog:
    """Buffers audit records and writes them to `<root>/audit/<day>/` as JSONL files.

    Each file is written once. The buffer is guarded by a lock, because
    `flush` runs in a worker thread.
    """

    def __init__(
        self,
        store: BlobStore,
        root: str,
        flush_size: int = 100,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self._store = store
        self._prefix = root.rstrip("/") + "/audit"
        self._flush_size = flush_size
        self._now = now
        self._buffer: list[dict] = []
        self._lock = threading.Lock()  # guards the buffer
        self._flush_lock = threading.Lock()  # one flush at a time
        self.listeners: list[Callable[[dict], None]] = []

    def add(self, record: dict) -> None:
        """Buffer `record`, tell the listeners, and flush when the buffer is full."""
        with self._lock:
            self._buffer.append(record)
            full = len(self._buffer) >= self._flush_size
        for listener in list(self.listeners):
            listener(record)
        if full:
            self.flush()

    def flush(self) -> None:
        """Write the buffered records to blob storage.

        On a store error the records stay in the buffer and a warning is logged.
        """
        with self._flush_lock:
            with self._lock:
                pending, self._buffer = self._buffer, []
            if not pending:
                return
            by_day: dict[str, list[dict]] = {}
            for record in pending:
                by_day.setdefault(_day_of(record["ts"]), []).append(record)
            stamp = self._now().astimezone(timezone.utc).strftime("%H%M%S.%f")
            failed: list[dict] = []
            for day, records in sorted(by_day.items()):
                path = f"{self._prefix}/{day}/{stamp}-{uuid.uuid4().hex}.jsonl"
                body = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records)
                try:
                    self._store.put(path, body)
                except Exception as error:
                    logger.warning(
                        "audit flush failed for %s, keeping %d records: %s",
                        day,
                        len(records),
                        error,
                    )
                    failed.extend(records)
            if failed:
                with self._lock:
                    self._buffer = failed + self._buffer

    def _day_files(self, day: str) -> list[str]:
        return self._store.ls(f"{self._prefix}/{day}")

    def _read_file(self, path: str) -> list[dict]:
        try:
            text = self._store.get(path).decode("utf-8", errors="replace")
        except KeyError:
            return []
        records = []
        for number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except ValueError:
                value = None
            if not isinstance(value, dict):
                logger.warning("skipping malformed audit line %d in %s", number, path)
                continue
            records.append(value)
        return records

    def _buffered(self) -> list[dict]:
        with self._lock:
            return list(self._buffer)

    def _stored_days(self) -> set[str]:
        days = set()
        start = len(self._prefix) + 1
        for path in self._store.ls(self._prefix):
            day = path[start:].split("/", 1)[0]
            if day:
                days.add(day)
        return days

    def records_for_day(self, day: str) -> list[dict]:
        """Return the stored records of `day` plus the buffered records of that day."""
        records = []
        for path in self._day_files(day):
            records.extend(self._read_file(path))
        records.extend(r for r in self._buffered() if _day_of(r["ts"]) == day)
        return records

    def days(self) -> list[str]:
        """Return the days that have stored files or buffered records, sorted."""
        days = self._stored_days()
        days.update(_day_of(r["ts"]) for r in self._buffered())
        return sorted(days)

    def query(
        self,
        user: str | None = None,
        model: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Return matching records, newest first.

        `since` is inclusive and `until` is exclusive. Without `since`, the range
        starts at the oldest day. Without `until`, it ends at the newest record.
        Only the days in the range are read.
        """
        first = since.astimezone(timezone.utc).strftime("%Y-%m-%d") if since else None
        last = until.astimezone(timezone.utc).strftime("%Y-%m-%d") if until else None
        found: list[tuple[datetime, dict]] = []
        for day in self.days():
            if (first and day < first) or (last and day > last):
                continue
            for record in self.records_for_day(day):
                if user is not None and record.get("user") != user:
                    continue
                if model is not None and record.get("model") != model:
                    continue
                when = parse_time(record["ts"])
                if (since and when < since) or (until and when >= until):
                    continue
                found.append((when, record))
        found.sort(key=lambda item: item[0], reverse=True)
        return [record for _, record in found[offset : offset + limit]]

    def users(self) -> list[dict]:
        """Return the first request, last request and request count of each user."""
        summary: dict[str, dict] = {}
        for day in self.days():
            for record in self.records_for_day(day):
                ts = iso(parse_time(record["ts"]))
                entry = summary.setdefault(
                    record["user"],
                    {"user": record["user"], "first_request": ts, "last_request": ts, "requests": 0},
                )
                entry["first_request"] = min(entry["first_request"], ts)
                entry["last_request"] = max(entry["last_request"], ts)
                entry["requests"] += 1
        return [summary[name] for name in sorted(summary)]


async def run_flusher(log: AuditLog, interval: float) -> None:
    """Flush `log` every `interval` seconds until cancelled, then flush once more."""
    try:
        while True:
            await asyncio.sleep(interval)
            await asyncio.to_thread(log.flush)
    finally:
        log.flush()
