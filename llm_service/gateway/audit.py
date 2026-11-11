"""Audit log: records in a memory buffer, written to blob storage as metadata rows and prompts.

A flush writes the prompts of each day to `prompts/<day>/<segment>.jsonl` and the metadata
rows to `audit/<day>/segments/<segment>.jsonl`. See `docs/audit-storage-design.md`.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable

from llm_service.gateway.audit_layout import (
    audit_dir,
    bucket_of,
    classify,
    day_end,
    dump_lines,
    join_record,
    legacy_rows,
    parse_lines,
    prompt_path,
    public_row,
    segment_name,
    segment_path,
    split_record,
)
from llm_service.gateway.storage import BlobStore

logger = logging.getLogger("llm_service.audit")

ROW_CACHE_SIZE = 1_000_000  # metadata rows kept in memory
PROMPT_CACHE_SIZE = 32  # prompt files kept in memory
READ_WORKERS = 8  # threads that read uncached files of one day
_KIND_ORDER = {"day": 0, "part": 1, "segment": 2, "legacy": 3}  # the order of "Which files count"


class PromptUnreadable(Exception):
    """The prompt of a record exists, but its file cannot be read or does not hold the record."""


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


def _valid_row(value: object) -> bool:
    """Return True for a valid record that also has a string `seg`."""
    return _valid_record(value) and isinstance(value.get("seg"), str)


def _day_of(ts: str) -> str:
    return parse_time(ts).strftime("%Y-%m-%d")


def _day_dir(root: str, day: str) -> str:
    """Return the folder of one day under `audit/`. `audit_layout` has no function for it."""
    return f"{audit_dir(root)}{day}"


def _legacy_path(root: str, day: str, name: str) -> str:
    """Return the path of the old layout file `name` of `day`."""
    return f"{_day_dir(root, day)}/{name}.jsonl"


def _may_hold_later(kind: str, name: str, after: str) -> bool:
    """Return True if a file can hold a row whose `seg` is greater than `after`.

    A segment holds only its own rows. A part holds the rows of its bucket. A day file can get
    a late record. An old layout file was written before the first segment, so it holds none.
    """
    if kind == "segment":
        return name > after
    if kind == "part":
        return name >= bucket_of(after)
    return kind == "day"


def _buffered_row(record: dict) -> dict:
    """Return the metadata row of a buffered record. It has no `seg` and no `prompt_ref`."""
    prompt = record.get("prompt")
    if not (isinstance(prompt, dict) and "event" in prompt):
        record = {**record, "prompt": None}  # do not copy a prompt that the row drops
    row, _ = split_record(record, None, None)
    row["prompt_ref"] = None
    return row


class _Entry:
    """A buffered record. `seg` is set while a flush writes its segment, and after."""

    __slots__ = ("record", "day", "seg")

    def __init__(self, record: dict, day: str) -> None:
        self.record = record
        self.day = day
        self.seg: str | None = None


class AuditLog:
    """Buffers audit records and writes them to blob storage.

    Each flush writes one prompt file and one segment file for each day. The log keeps the
    metadata rows of the files it wrote or read in a cache, up to `row_cache_size` rows, and
    drops the least recently used file first. It keeps the last `PROMPT_CACHE_SIZE` prompt
    files that `record` read in a second cache.

    `flush`, the reads and `record` run in worker threads at the same time. The buffer, the
    row cache and the prompt cache each have a lock. No lock is held during a storage call,
    except `flush_lock`, which keeps one flush at a time.

    Rows from the cache are shared. The callers of `rows_for_day`, `read_rows`, `cached_rows`
    and the flushed listeners must not change them. `query` and `record` return copies.
    """

    def __init__(
        self,
        store: BlobStore,
        root: str,
        flush_size: int = 100,
        now: Callable[[], datetime] = utc_now,
        row_cache_size: int = ROW_CACHE_SIZE,
    ) -> None:
        self._store = store
        self._root = root.rstrip("/")
        self._flush_size = flush_size
        self._now = now
        self._buffer: list[_Entry] = []
        self._inflight: list[_Entry] = []  # records that a running flush writes
        self._lock = threading.Lock()  # guards the buffer, the in-flight list and `_Entry.seg`
        self.flush_lock = threading.Lock()  # one flush at a time
        self._row_cache_size = row_cache_size
        self._rows: OrderedDict[str, list[dict]] = OrderedDict()  # path -> metadata rows
        self._row_count = 0  # rows in `_rows`
        self._loading = 0  # reads of uncached files that run now
        self._versions: dict[str, int] = {}  # path -> changes made while a read ran
        self._rows_lock = threading.Lock()  # guards the five fields above
        self._prompts: OrderedDict[str, list] = OrderedDict()  # path -> parsed prompt lines
        self._prompts_lock = threading.Lock()
        self.listeners: list[Callable[[dict], None]] = []
        self.flushed_listeners: list[Callable[[str, str, list[dict]], None]] = []

    # Writes

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
        entry = _Entry(record, _day_of(ts))
        with self._lock:
            self._buffer.append(entry)
        for listener in list(self.listeners):
            listener(record)

    def should_flush(self) -> bool:
        """Return True when the buffer holds at least `flush_size` records."""
        with self._lock:
            return len(self._buffer) >= self._flush_size

    def flush(self) -> bool:
        """Write the buffered records to blob storage.

        For each day, the flush writes the prompt file, then the segment file, then puts the
        rows in the cache and calls each `flushed_listeners` entry with `(day, seg, rows)`.
        On a store error the records of that day stay in the buffer and a warning is logged.
        The next flush writes them with a new segment name. A record that `json.dumps` cannot
        write is dropped with a warning that names its `request_id`, so it cannot block later
        flushes. On any other error, the unwritten records go back to the buffer and a
        warning is logged. Return True when the buffer is empty afterwards, False when
        records were kept. While the flush runs, the records stay visible to reads.
        """
        with self.flush_lock:
            with self._lock:
                pending, self._buffer = self._buffer, []
                self._inflight = pending
            if not pending:
                return True
            done: set[int] = set()  # ids of the written and the dropped entries
            try:
                self._write(pending, done)
            except Exception as error:
                logger.warning("audit flush stopped, keeping the unwritten records: %s", error)
            finally:
                kept = [entry for entry in pending if id(entry) not in done]
                with self._lock:
                    for entry in kept:
                        entry.seg = None
                    self._buffer = kept + self._buffer
                    self._inflight = []
            return not kept

    def _write(self, pending: list[_Entry], done: set[int]) -> None:
        """Write `pending` as one segment for each day. Add the handled entries to `done`."""
        by_day: dict[str, list[_Entry]] = {}
        for entry in pending:
            by_day.setdefault(entry.day, []).append(entry)
        now = self._now()
        for day, entries in sorted(by_day.items()):
            self._write_day(day, entries, segment_name(now, uuid.uuid4().hex), done)

    def _write_day(self, day: str, entries: list[_Entry], seg: str, done: set[int]) -> None:
        rows: list[dict] = []
        row_lines: list[str] = []
        prompt_lines: list[str] = []
        written: list[_Entry] = []
        for entry in entries:
            try:
                row, line = split_record(entry.record, seg, len(prompt_lines))
                row_line = dump_lines([row])
                prompt_line = dump_lines([line]) if line is not None else None
            except (TypeError, ValueError, RecursionError) as error:
                logger.warning(
                    "dropping the audit record %s, it cannot be written as JSON: %s",
                    entry.record.get("request_id"),
                    error,
                )
                done.add(id(entry))
                continue
            rows.append(row)
            row_lines.append(row_line)
            if prompt_line is not None:
                prompt_lines.append(prompt_line)
            written.append(entry)
        if not written:
            return
        path = segment_path(self._root, day, seg)
        try:
            if prompt_lines:
                self._store.put(prompt_path(self._root, day, seg), "".join(prompt_lines))
            # A read that lists the segment file skips these buffered records from now on.
            self._set_seg(written, seg)
            self._store.put(path, "".join(row_lines))
        except Exception as error:
            self._set_seg(written, None)
            logger.warning(
                "audit flush failed for %s, keeping %d records: %s", day, len(written), error
            )
            return
        done.update(id(entry) for entry in written)
        rows = [row for row in rows if _valid_row(row)]  # the rows that a read of the file gives
        self.cache_put(path, rows)
        for listener in list(self.flushed_listeners):
            try:
                listener(day, seg, rows)
            except Exception as error:
                logger.warning("audit flush listener failed: %s", error)

    def _set_seg(self, entries: list[_Entry], seg: str | None) -> None:
        with self._lock:
            for entry in entries:
                entry.seg = seg

    # Row cache

    def cached_rows(self, path: str) -> list[dict] | None:
        """Return the cached rows of `path`, or None. The caller must not change them."""
        with self._rows_lock:
            rows = self._rows.get(path)
            if rows is not None:
                self._rows.move_to_end(path)
            return rows

    def cache_put(self, path: str, rows: list[dict]) -> None:
        """Put the rows of a file that was just written into the cache. Replace an old entry."""
        with self._rows_lock:
            self._changed(path)
            self._remove(path)
            self._insert(path, list(rows))

    def cache_drop(self, paths: list[str]) -> None:
        """Remove the files `paths` from the cache."""
        with self._rows_lock:
            for path in paths:
                self._changed(path)
                self._remove(path)

    def read_rows(self, path: str) -> list[dict]:
        """Return the rows of one file: from the cache, else from one blob read.

        A missing file gives an empty list. A failed read raises and is not cached. The caller
        must not change the returned rows.
        """
        cached = self.cached_rows(path)
        if cached is not None:
            return cached
        with self._rows_lock:
            self._loading += 1
            version = self._versions.get(path, 0)
        rows = None
        try:
            rows = self._load_rows(path)
        finally:
            with self._rows_lock:
                self._loading -= 1
                # A write or a drop during the read makes the read old. Do not cache it then.
                if rows is not None and path not in self._rows:
                    if self._versions.get(path, 0) == version:
                        self._insert(path, rows)
                if not self._loading:
                    self._versions.clear()
        return rows if rows is not None else []

    def _changed(self, path: str) -> None:
        """Note a change of `path` for the reads that run now. Hold `_rows_lock`."""
        if self._loading:
            self._versions[path] = self._versions.get(path, 0) + 1

    def _remove(self, path: str) -> None:
        """Remove one entry. Hold `_rows_lock`."""
        old = self._rows.pop(path, None)
        if old is not None:
            self._row_count -= len(old)

    def _insert(self, path: str, rows: list[dict]) -> None:
        """Add one entry and drop the least recently used files. Hold `_rows_lock`.

        A file with more rows than the whole limit is not kept.
        """
        if len(rows) > self._row_cache_size:
            return
        self._rows[path] = rows
        self._row_count += len(rows)
        while self._row_count > self._row_cache_size:
            _, dropped = self._rows.popitem(last=False)
            self._row_count -= len(dropped)

    def _load_rows(self, path: str) -> list[dict] | None:
        """Read and parse one file. Return None when the file does not exist.

        An old layout file gives rows built from its full records. Lines that are not valid
        are skipped with one warning.
        """
        try:
            data = self._store.get(path)
        except KeyError:
            return None
        values = parse_lines(data.decode("utf-8", errors="replace"))
        info = classify(self._root, path)
        legacy = info is not None and info[1] == "legacy"
        valid = _valid_record if legacy else _valid_row
        good = [(index, value) for index, value in enumerate(values) if valid(value)]
        if len(good) < len(values):
            logger.warning("skipping %d malformed audit lines in %s", len(values) - len(good), path)
        if not legacy:
            return [value for _, value in good]
        rows = legacy_rows(info[2], [value for _, value in good])
        for (index, _), row in zip(good, rows):
            if row["prompt_ref"] is not None:
                row["prompt_ref"]["line"] = index  # the line in the file, skipped lines included
        return rows

    def _read_files(self, paths: list[str]) -> list[list[dict]]:
        """Return the rows of each file, in the order of `paths`.

        Files that are not cached are read by a small thread pool. An error in
        a read is raised for that file, in the order of `paths`.
        """
        with self._rows_lock:
            missing = [p for p in paths if p not in self._rows]
        if len(missing) < 2:
            return [self.read_rows(p) for p in paths]
        with ThreadPoolExecutor(max_workers=min(READ_WORKERS, len(missing))) as pool:
            futures = {p: pool.submit(self.read_rows, p) for p in missing}
            return [futures[p].result() if p in futures else self.read_rows(p) for p in paths]

    # Reads

    def _buffered(self) -> list[dict]:
        """Return the in-flight and the buffered records."""
        with self._lock:
            return [entry.record for entry in self._inflight + self._buffer]

    def _stored_rows(self, day: str, after: str | None) -> list[dict]:
        """Return the stored rows of `day`, each `seg` from one file only.

        The files count in the order day file, parts, segments, old layout files. A row whose
        `seg` occurred in an earlier file is skipped. With `after`, only rows whose `seg` is
        greater are returned, and files that cannot hold them are not read.
        """
        files = []
        for path in self._store.ls(_day_dir(self._root, day)):
            info = classify(self._root, path)
            if info is None or info[0] != day:
                continue
            _, kind, name = info
            if after is not None and not _may_hold_later(kind, name, after):
                continue
            files.append((_KIND_ORDER[kind], name, path))
        files.sort()
        rows: list[dict] = []
        seen: set[str] = set()
        for chunk in self._read_files([path for _, _, path in files]):
            segs = set()
            for row in chunk:
                seg = row["seg"]
                segs.add(seg)
                if seg in seen or (after is not None and seg <= after):
                    continue
                rows.append(row)
            seen |= segs
        return rows

    def _day_rows(self, day: str, after: str | None = None) -> list[tuple[dict, _Entry | None]]:
        """Return the rows of `rows_for_day`, each with its buffer entry or None.

        The buffer is read before the files. A record that a flush writes in between is then
        in both. Its entry has the `seg` of the segment, so only the stored row is kept. A
        record is never missing, because a flush clears the in-flight list only after the
        segment is written.
        """
        with self._lock:
            entries = [e for e in self._inflight + self._buffer if e.day == day]
        stored = self._stored_rows(day, after)
        seen = {row["seg"] for row in stored}
        with self._lock:
            segs = [entry.seg for entry in entries]
        result: list[tuple[dict, _Entry | None]] = [(row, None) for row in stored]
        for entry, seg in zip(entries, segs):
            if seg is not None and (seg in seen or (after is not None and seg <= after)):
                continue
            result.append((_buffered_row(entry.record), entry))
        return result

    def rows_for_day(self, day: str, after: str | None = None) -> list[dict]:
        """Return the metadata rows of `day`: stored rows in file order, then buffered rows.

        With `after`, return only the stored rows whose `seg` is greater, and the buffered
        rows. Buffered rows have `seg` and `prompt_ref` set to None. The caller must not
        change the rows.
        """
        return [row for row, _ in self._day_rows(day, after)]

    def days(self) -> list[str]:
        """Return the days that have stored files or buffered records, sorted."""
        days = set()
        for path in self._store.ls(audit_dir(self._root)):
            info = classify(self._root, path)
            if info is not None:
                days.add(info[0])
        with self._lock:
            days.update(entry.day for entry in self._inflight + self._buffer)
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
        """Return matching rows without `seg` and `prompt_ref`, newest first. Read no prompt.

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
            for row in self.rows_for_day(day):
                if user is not None and row.get("user") != user:
                    continue
                if model is not None and row.get("model") != model:
                    continue
                when = parse_time(row["ts"])
                if (since and when < since) or (until and when >= until):
                    continue
                matches.append((when, row))
            # Rows are in write order. Reverse them first, so that the stable
            # sort puts the later row first when two have the same time.
            matches.reverse()
            matches.sort(key=lambda item: item[0], reverse=True)
            found.extend(matches)
            if len(found) >= offset + limit:
                break
        return [copy.deepcopy(public_row(row)) for _, row in found[offset : offset + limit]]

    def users(self) -> list[dict]:
        """Return the first request, last request and request count of each user."""
        summary: dict[str, dict] = {}
        for day in self.days():
            for row in self.rows_for_day(day):
                ts = iso(parse_time(row["ts"]))
                entry = summary.setdefault(
                    row["user"],
                    {"user": row["user"], "first_request": ts, "last_request": ts, "requests": 0},
                )
                entry["first_request"] = min(entry["first_request"], ts)
                entry["last_request"] = max(entry["last_request"], ts)
                entry["requests"] += 1
        return [summary[name] for name in sorted(summary)]

    def record(self, day: str, request_id: str) -> dict | None:
        """Return the full record `request_id` of `day`, with its prompt, or None.

        A buffered record comes from the buffer. An event builds its prompt from its row. A
        stored request reads one prompt file, through a small cache. A missing prompt file
        gives `prompt` None and `prompt_deleted` True. Raise `PromptUnreadable` when the
        prompt file cannot be read or does not hold the record. Raise `ValueError` when
        `day` is not a day. The caller owns the returned record.
        """
        day_end(day)
        for row, entry in self._day_rows(day):
            if row.get("request_id") != request_id:
                continue
            if entry is not None:
                return copy.deepcopy(join_record(row, entry.record.get("prompt")))
            return copy.deepcopy(self._stored_record(day, row))
        return None

    def _stored_record(self, day: str, row: dict) -> dict:
        if row.get("event") is not None:
            return join_record(row, None)
        ref = row.get("prompt_ref")
        if not (
            isinstance(ref, dict)
            and isinstance(ref.get("file"), str)
            and isinstance(ref.get("line"), int)
        ):
            raise PromptUnreadable(f"the record {row.get('request_id')} has no prompt reference")
        legacy = bool(ref.get("legacy"))
        if legacy:
            path = _legacy_path(self._root, day, ref["file"])
        else:
            path = prompt_path(self._root, day, ref["file"])
        lines = self._prompt_lines(path)
        if lines is None:
            if legacy:
                raise PromptUnreadable(f"the audit file {path} is missing")
            return join_record(row, None, deleted=True)
        index = ref["line"]
        line = lines[index] if 0 <= index < len(lines) else None
        if not (
            isinstance(line, dict)
            and "prompt" in line
            and line.get("request_id") == row.get("request_id")
        ):
            raise PromptUnreadable(f"line {index} of {path} does not hold the record")
        return join_record(row, line["prompt"])

    def _prompt_lines(self, path: str) -> list | None:
        """Return the parsed lines of a prompt file, or None when it does not exist."""
        with self._prompts_lock:
            lines = self._prompts.get(path)
            if lines is not None:
                self._prompts.move_to_end(path)
                return lines
        try:
            data = self._store.get(path)
        except KeyError:
            return None
        except Exception as error:
            raise PromptUnreadable(f"cannot read {path}: {error}") from error
        lines = parse_lines(data.decode("utf-8", errors="replace"))
        with self._prompts_lock:
            self._prompts[path] = lines
            self._prompts.move_to_end(path)
            while len(self._prompts) > PROMPT_CACHE_SIZE:
                self._prompts.popitem(last=False)
        return lines


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
