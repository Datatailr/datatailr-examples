"""Audit log: records in a memory buffer, written as JSONL files to blob storage."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
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
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError:
        raise ValueError(f"time out of range: {text!r}") from None


def _valid_record(value: object) -> bool:
    """Return True for a dict with a string `user` and a `ts` that `parse_time` accepts."""
    if not isinstance(value, dict) or not isinstance(value.get("user"), str):
        return False
    ts = value.get("ts")
    if not isinstance(ts, str):
        return False
    try:
        parse_time(ts)
    except ValueError:
        return False
    return True


def _key(record: dict) -> str:
    """Return a text that is the same for a record and its copy read from a file."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)


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
        self._inflight: list[dict] = []  # records that a running flush writes
        self._lock = threading.Lock()  # guards the buffer and the in-flight list
        self._flush_lock = threading.Lock()  # one flush at a time
        self.listeners: list[Callable[[dict], None]] = []

    def add(self, record: dict) -> None:
        """Buffer `record` and tell the listeners. This does no storage I/O.

        The flusher writes the buffer, see `should_flush` and `run_flusher`.
        Raise `ValueError` if `ts` is missing or is not a time that
        `parse_time` accepts. The record is then neither buffered nor passed
        to a listener.
        """
        ts = record.get("ts") if isinstance(record, dict) else None
        if not isinstance(ts, str):
            raise ValueError("audit record needs a string 'ts'")
        parse_time(ts)
        with self._lock:
            self._buffer.append(record)
        for listener in list(self.listeners):
            listener(record)

    def should_flush(self) -> bool:
        """Return True when the buffer holds at least `flush_size` records."""
        with self._lock:
            return len(self._buffer) >= self._flush_size

    def flush(self) -> bool:
        """Write the buffered records to blob storage.

        On a store error the records stay in the buffer and a warning is logged.
        A record that `json.dumps` cannot write is dropped with a warning that
        names its `request_id`, so it cannot block later flushes. On any other
        error, the unwritten records go back to the buffer and a warning is
        logged. Return True when the buffer is empty afterwards, False when
        records were kept. While the flush runs, the records stay visible to
        queries as in-flight records.
        """
        with self._flush_lock:
            with self._lock:
                pending, self._buffer = self._buffer, []
                self._inflight = pending
            if not pending:
                return True
            done: set[int] = set()  # ids of the written and the dropped records
            try:
                self._write(pending, done)
            except Exception as error:
                logger.warning("audit flush stopped, keeping the unwritten records: %s", error)
            finally:
                kept = [record for record in pending if id(record) not in done]
                with self._lock:
                    self._buffer = kept + self._buffer
                    self._inflight = []
            return not kept

    def _write(self, pending: list[dict], done: set[int]) -> None:
        """Write `pending` as one file for each day. Add the handled records to `done`."""
        by_day: dict[str, list[dict]] = {}
        for record in pending:
            by_day.setdefault(_day_of(record["ts"]), []).append(record)
        stamp = self._now().astimezone(timezone.utc).strftime("%H%M%S.%f")
        for day, records in sorted(by_day.items()):
            lines = []
            written = []
            for record in records:
                try:
                    lines.append(json.dumps(record, separators=(",", ":")) + "\n")
                except (TypeError, ValueError, RecursionError) as error:
                    logger.warning(
                        "dropping the audit record %s, it cannot be written as JSON: %s",
                        record.get("request_id"),
                        error,
                    )
                    done.add(id(record))
                    continue
                written.append(record)
            if not written:
                continue
            path = f"{self._prefix}/{day}/{stamp}-{uuid.uuid4().hex}.jsonl"
            try:
                self._store.put(path, "".join(lines))
            except Exception as error:
                logger.warning(
                    "audit flush failed for %s, keeping %d records: %s",
                    day,
                    len(written),
                    error,
                )
                continue
            done.update(id(record) for record in written)

    def _day_files(self, day: str) -> list[str]:
        return self._store.ls(f"{self._prefix}/{day}")

    def _read_file(self, path: str) -> list[dict]:
        """Return the valid records of one file. Skip the others with one warning."""
        try:
            text = self._store.get(path).decode("utf-8", errors="replace")
        except KeyError:
            return []
        records = []
        skipped = 0
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except ValueError:
                value = None
            if not _valid_record(value):
                skipped += 1
                continue
            records.append(value)
        if skipped:
            logger.warning("skipping %d malformed audit lines in %s", skipped, path)
        return records

    def _buffered(self) -> list[dict]:
        """Return the in-flight and the buffered records."""
        with self._lock:
            return self._inflight + self._buffer

    def _stored_days(self) -> set[str]:
        days = set()
        start = len(self._prefix) + 1
        for path in self._store.ls(self._prefix):
            day = path[start:].split("/", 1)[0]
            if day:
                days.add(day)
        return days

    def records_for_day(self, day: str) -> list[dict]:
        """Return the stored records of `day` plus the buffered records of that day.

        The buffer is read before the files. A record that a flush writes in
        between is then in both, and only the stored copy is kept. A record is
        never missing, because a flush clears the in-flight list only after
        the file is written.
        """
        pending = [r for r in self._buffered() if _day_of(r["ts"]) == day]
        records = []
        for path in self._day_files(day):
            records.extend(self._read_file(path))
        if pending:
            stored = Counter(_key(r) for r in records)
            for record in pending:
                key = _key(record)
                if stored[key]:
                    stored[key] -= 1
                else:
                    records.append(record)
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
        Only the days in the range are read. The days are read newest first,
        and the reading stops when `offset + limit` records match.
        """
        first = since.astimezone(timezone.utc).strftime("%Y-%m-%d") if since else None
        last = until.astimezone(timezone.utc).strftime("%Y-%m-%d") if until else None
        found: list[tuple[datetime, dict]] = []
        for day in sorted(self.days(), reverse=True):
            if (first and day < first) or (last and day > last):
                continue
            matches: list[tuple[datetime, dict]] = []
            for record in self.records_for_day(day):
                if user is not None and record.get("user") != user:
                    continue
                if model is not None and record.get("model") != model:
                    continue
                when = parse_time(record["ts"])
                if (since and when < since) or (until and when >= until):
                    continue
                matches.append((when, record))
            # Records are in write order. Reverse them first, so that the stable
            # sort puts the later record first when two have the same time.
            matches.reverse()
            matches.sort(key=lambda item: item[0], reverse=True)
            found.extend(matches)
            if len(found) >= offset + limit:
                break
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


async def run_flusher(
    log: AuditLog,
    interval: float,
    tick: float = 0.25,
    on_tick: Callable[[], None] | None = None,
) -> None:
    """Flush `log` in a worker thread until cancelled, then flush once more.

    The flusher wakes every `tick` seconds. It flushes when `interval` seconds
    have passed since the last attempt, or when the buffer is full. After a
    failed flush it waits the full `interval` again, so a store outage costs one
    attempt per interval. `on_tick` runs in the same worker thread after each
    flush attempt. An error in it is logged and does not stop the flusher.
    """

    def cycle() -> bool:
        ok = log.flush()
        if on_tick is not None:
            try:
                on_tick()
            except Exception as error:
                logger.warning("flusher callback failed: %s", error)
        return ok

    last = time.monotonic()
    failed = False
    try:
        while True:
            await asyncio.sleep(min(tick, interval))
            due = time.monotonic() - last >= interval
            if due or (not failed and log.should_flush()):
                failed = not await asyncio.to_thread(cycle)
                last = time.monotonic()
    finally:
        cycle()
