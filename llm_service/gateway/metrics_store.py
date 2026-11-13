"""Metrics store: one file for each process and UTC day in blob storage, and the read of the history.

Each process writes only its own files, `<blob root>/metrics/<day>/<writer>.jsonl`. No process
writes a file of another process, so two processes never overwrite their points.
"""

from __future__ import annotations

import json
import logging
import math
import re
import secrets
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from .storage import BlobStore

logger = logging.getLogger(__name__)

MAX_POINTS = 500
MAX_DAYS = 31
DROP_AFTER_SECONDS = 3600.0
WRITE_EVERY_SECONDS = 300.0
RETAIN_EVERY_SECONDS = 3600.0
READ_THREADS = 8
DELETES_PER_CALL = 64

_WRITER = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")  # the start time of the process and 8 random hex characters
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")

_SUM_FIELDS = ("requests", "failed_requests", "prompt_tokens", "completion_tokens")
_MAX_FIELDS = (
    "in_flight_max", "waiting_max", "kv_cache_use_max", "gpu_memory_used_max",
    "ttft_ms_p50", "ttft_ms_p95", "duration_ms_p50", "duration_ms_p95",
)


def _maximum(values: list) -> object:
    values = [v for v in values if v is not None]
    return max(values) if values else None


def merge_points(
    points: list[dict], since: datetime, until: datetime, limit: int = MAX_POINTS
) -> tuple[list[dict], int]:
    """Merge the points of the range from `since` to `until` into time buckets. Return them and the step in seconds.

    The buckets start at the minute of `since`. The step is `ceil(minutes / limit)` minutes, at
    least 1, where `minutes` counts the minutes that the range touches. So at most `limit` buckets
    exist. A point goes to the bucket `floor((ts - start) / step)`. An empty bucket gives no point.
    The `ts` of a merged point is the start of its bucket. Counts and token numbers add up.
    Maximum values and percentiles take the maximum, or null if each value is null.
    `gpu_memory_total` takes the last value that is not null. A point without `kv_cache_use_max`
    comes from a file of an older version. Its value reads as null.
    """
    start = since.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minutes = int((until - start).total_seconds() // 60) + 1
    step = max(1, math.ceil(minutes / limit))
    if step == 1:
        return [_with_defaults(p) for p in points], 60
    buckets: dict[int, list[dict]] = {}
    for p in points:
        index = int((_parse_ts(p["ts"]) - start).total_seconds() // (60 * step))
        buckets.setdefault(index, []).append(p)
    merged = [_combine(_iso(start + timedelta(minutes=step * i)), buckets[i]) for i in sorted(buckets)]
    return merged, 60 * step


def _combine(ts: str, group: list[dict]) -> dict:
    """Combine the points of `group` into one point with the time `ts`, by the rules of `merge_points`."""
    out: dict = {"ts": ts}
    for name in _SUM_FIELDS:
        out[name] = sum(p.get(name) or 0 for p in group)
    for name in _MAX_FIELDS:
        out[name] = _maximum([p.get(name) for p in group])
    totals = [p.get("gpu_memory_total") for p in group if p.get("gpu_memory_total") is not None]
    out["gpu_memory_total"] = totals[-1] if totals else None
    return out


def _with_defaults(point: dict) -> dict:
    """A copy of a point with null for each field that an older version did not write."""
    copy = dict(point)
    copy.setdefault("kv_cache_use_max", None)
    return copy


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _parse_lines(raw: bytes, day: str) -> dict[str, dict]:
    """Return the points of a point file by `ts`. Skip each line that is not a point of `day`."""
    found: dict[str, dict] = {}
    for line in raw.decode("utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            if not isinstance(entry, dict) or not isinstance(entry.get("ts"), str):
                continue
            if entry["ts"][:10] != day:
                continue
            _parse_ts(entry["ts"])
        except (ValueError, TypeError, RecursionError):
            continue
        found[entry["ts"]] = entry
    return found


def _day_of(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _day_end(day: str) -> datetime:
    return datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1)


class MetricsStore:
    """The points of the load history, in memory and in `<blob_root>/metrics/<day>/<writer>.jsonl`.

    `writer` names the files of this process. It is the start time of the process and 8 random
    hex characters. A test can fix it. A legacy file `<blob_root>/metrics/<day>.jsonl` is read too.
    Each history read lists `metrics/` once, so it always sees the files as they are in storage.
    """

    def __init__(
        self,
        store: BlobStore,
        blob_root: str,
        now: Callable[[], datetime],
        retention_days: int = 30,
        writer: str | None = None,
    ) -> None:
        if writer is None:
            started = now().astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            writer = f"{started}-{secrets.token_hex(4)}"
        if not _WRITER.fullmatch(writer):
            raise ValueError(f"not a writer name: {writer!r}")
        self.writer = writer
        self._store = store
        self._dir = f"{blob_root.strip('/')}/metrics"
        self._now = now
        self._retention_days = retention_days
        self._write_lock = threading.Lock()  # serializes the writers across a blob call
        self._lock = threading.Lock()  # guards the fields below, never held for a blob call
        self._points: dict[str, dict[str, dict]] = {}  # day -> ts -> own point
        self._version: dict[str, int] = {}  # day -> count of changes
        self._written: dict[str, int] = {}  # day -> version of the last good write
        self._last_good: dict[str, datetime] = {}  # day -> time of the last good write
        self._last_write: datetime | None = None
        self._last_retain: datetime | None = None
        self._doomed: list[str] = []  # old files that the next calls of `retain` delete
        self._write_error: str | None = None

    def _own_path(self, day: str) -> str:
        return f"{self._dir}/{day}/{self.writer}.jsonl"

    def _listing(self) -> list[str]:
        """List `metrics/` once. The listing is recursive and holds files only. Raise on a failure."""
        return self._store.ls(self._dir)

    def _read_files(self, files: list[tuple[str, str]]) -> tuple[dict[str, dict[str, dict[str, dict]]], dict[str, str]]:
        """Read the `(day, path)` files with up to `READ_THREADS` threads.

        Return day -> path -> ts -> point, and day -> error for each day with a failed read. A
        file that is missing at the read gives no points and no error.
        """
        errors: dict[str, str] = {}
        stored: dict[str, dict[str, dict[str, dict]]] = {}

        def read(task: tuple[str, str]) -> dict[str, dict] | None:
            day, path = task
            try:
                raw = self._store.get(path)
            except KeyError:
                return {}
            except Exception as exc:  # noqa: BLE001
                errors.setdefault(day, str(exc))
                return None
            return _parse_lines(raw, day)

        if files:
            with ThreadPoolExecutor(max_workers=min(READ_THREADS, len(files))) as pool:
                for (day, path), found in zip(files, pool.map(read, files)):
                    if found:
                        stored.setdefault(day, {})[path] = found
        return stored, errors

    def add(self, points: list[dict]) -> None:
        """Keep the points in memory. A point with the `ts` of an older point replaces it."""
        with self._lock:
            for point in points:
                day = point["ts"][:10]
                self._points.setdefault(day, {})[point["ts"]] = dict(point)
                self._version[day] = self._version.get(day, 0) + 1

    def _dirty_days(self) -> list[str]:
        return sorted(d for d, v in self._version.items() if self._written.get(d) != v)

    def _forget(self, day: str) -> None:
        for table in (self._points, self._version, self._written, self._last_good):
            table.pop(day, None)

    def _drop_stale(self, now: datetime, today: str) -> None:
        """Drop each past day that has no good write for an hour, and each past day that is clean."""
        with self._lock:
            dirty = set(self._dirty_days())
            for day in [d for d in self._points if d < today and d not in dirty]:
                self._forget(day)
            for day in self._dirty_days():
                if day >= today:
                    continue
                since = max(_day_end(day), self._last_good.get(day, _day_end(day)))
                if (now - since).total_seconds() >= DROP_AFTER_SECONDS:
                    logger.error(
                        "dropping %d points of %s, no good write for %d seconds",
                        len(self._points.get(day, {})), day, DROP_AFTER_SECONDS,
                    )
                    self._forget(day)

    def write(self, force: bool = False) -> None:
        """Write the own file of each day with new points, with all own points of that day. Never raises.

        The store reads nothing before a put. Without `force` the store writes at most once
        in `WRITE_EVERY_SECONDS`.
        """
        try:
            with self._write_lock:
                self._write(force)
        except Exception as exc:  # noqa: BLE001
            logger.error("the write of the metrics failed: %s", exc)

    def _write(self, force: bool) -> None:
        now = self._now()
        today = _day_of(now)
        self._drop_stale(now, today)
        with self._lock:
            days = self._dirty_days()
            if not days:
                return
            if not force and self._last_write is not None:
                if (now - self._last_write).total_seconds() < WRITE_EVERY_SECONDS:
                    return
            self._last_write = now
        for day in days:
            with self._lock:
                version = self._version.get(day, 0)
                points = [self._points[day][ts] for ts in sorted(self._points.get(day, {}))]
            body = "".join(json.dumps(p) + "\n" for p in points)
            try:
                self._store.put(self._own_path(day), body)
            except Exception as exc:  # noqa: BLE001
                if str(exc) != self._write_error:
                    logger.error("cannot write %s: %s", self._own_path(day), exc)
                self._write_error = str(exc)
                continue
            self._write_error = None
            with self._lock:
                self._written[day] = version
                self._last_good[day] = now
                if day < today and self._version.get(day) == version:
                    self._forget(day)

    def history(self, since: datetime, until: datetime) -> tuple[list[dict], int, list[str]]:
        """Return the points with `since <= ts <= until`, the step and the failed days.

        The call lists `metrics/` once and reads each file of the days of the range, the current
        day too. The own points in memory count as well. For a `ts` in the own file and in memory,
        the memory wins. Points of one `ts` from different writers are partial minutes. They
        combine by the rules of `merge_points`. The points then merge into at most `MAX_POINTS`
        time buckets. The failed days are the sorted days with a failed file read. A failed
        listing fails each day of the range. A day without a file is not a failure. A failure
        logs one error for the call. Raise `ValueError` if `since` is after `until` or if the range is
        longer than `MAX_DAYS` days.
        """
        since = since.astimezone(timezone.utc) if since.tzinfo else since.replace(tzinfo=timezone.utc)
        until = until.astimezone(timezone.utc) if until.tzinfo else until.replace(tzinfo=timezone.utc)
        if since > until:
            raise ValueError("since is after until")
        first = since.date()
        if until - since > timedelta(days=MAX_DAYS):
            raise ValueError(f"the range is over {MAX_DAYS} days")
        count = (until.date() - first).days + 1
        days = [(first + timedelta(days=i)).isoformat() for i in range(count)]
        with self._lock:
            memory = {d: dict(self._points.get(d, {})) for d in days}
        wanted = set(days)
        try:
            files = [(day, path) for path in self._listing() if (day := self._day_of_path(path)) in wanted]
        except Exception as exc:  # noqa: BLE001
            logger.error("cannot list the metrics files: %s", exc)
            stored, errors = {}, dict.fromkeys(days, str(exc))
        else:
            stored, errors = self._read_files(files)
            if errors:
                logger.error("cannot read the metrics of %d of %d days", len(errors), len(days))
        chosen = []
        for day in days:
            sources = dict(stored.get(day, {}))
            own = self._own_path(day)
            sources[own] = {**sources.get(own, {}), **memory[day]}
            groups: dict[str, list[dict]] = {}
            for points in sources.values():
                for ts, p in points.items():
                    groups.setdefault(ts, []).append(p)
            for ts in sorted(groups):
                try:
                    moment = _parse_ts(ts)
                except ValueError:
                    continue
                if since <= moment <= until:
                    group = groups[ts]
                    chosen.append(group[0] if len(group) == 1 else _combine(ts, group))
        merged_points, step = merge_points(chosen, since, until)
        return merged_points, step, sorted(errors)

    def _day_of_path(self, path: str) -> str | None:
        """Return the day of a point file or a legacy file under `metrics/`, or None for any other path."""
        prefix = f"{self._dir}/"
        if not path.startswith(prefix):
            return None
        parts = path[len(prefix):].split("/")
        if len(parts) == 1 and parts[0].endswith(".jsonl"):
            day = parts[0][: -len(".jsonl")]
        elif len(parts) == 2 and parts[1].endswith(".jsonl") and parts[1] != ".jsonl":
            day = parts[0]
        else:
            return None
        return day if _DAY.fullmatch(day) else None

    def retain(self) -> None:
        """Delete the files of days older than the retention. Never raises.

        The store lists the directory at most once in an hour. One call deletes at most
        `DELETES_PER_CALL` files, and the next call continues with the rest.
        """
        try:
            now = self._now()
            with self._lock:
                pending = bool(self._doomed)
                if not pending:
                    if self._last_retain is not None:
                        if (now - self._last_retain).total_seconds() < RETAIN_EVERY_SECONDS:
                            return
                    self._last_retain = now
            if not pending:
                oldest = (now.astimezone(timezone.utc) - timedelta(days=self._retention_days)).date().isoformat()
                doomed = []
                for path in self._listing():
                    day = self._day_of_path(path)
                    if day is not None and day < oldest:
                        doomed.append(path)
                with self._lock:
                    self._doomed = doomed
            with self._lock:
                batch = self._doomed[:DELETES_PER_CALL]
                self._doomed = self._doomed[DELETES_PER_CALL:]
            for path in batch:
                try:
                    self._store.delete(path)
                except Exception as exc:  # noqa: BLE001
                    logger.error("cannot delete %s: %s", path, exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("the clean up of the metrics files failed: %s", exc)
