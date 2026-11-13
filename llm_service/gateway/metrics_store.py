"""Metrics store: one file for each UTC day in blob storage, and the read of the history."""

from __future__ import annotations

import json
import logging
import math
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
    merged = []
    for index in sorted(buckets):
        group = buckets[index]
        out = {"ts": _iso(start + timedelta(minutes=step * index))}
        for name in _SUM_FIELDS:
            out[name] = sum(p.get(name) or 0 for p in group)
        for name in _MAX_FIELDS:
            out[name] = _maximum([p.get(name) for p in group])
        totals = [p.get("gpu_memory_total") for p in group if p.get("gpu_memory_total") is not None]
        out["gpu_memory_total"] = totals[-1] if totals else None
        merged.append(out)
    return merged, 60 * step


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
    """Return the points of a day file by `ts`. Skip each line that is not a point of `day`."""
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
    """The points of the load history, in memory and in `<blob_root>/metrics/<day>.jsonl`."""

    def __init__(
        self, store: BlobStore, blob_root: str, now: Callable[[], datetime], retention_days: int = 30
    ) -> None:
        self._store = store
        self._dir = f"{blob_root.strip('/')}/metrics"
        self._now = now
        self._retention_days = retention_days
        self._write_lock = threading.Lock()  # serializes the writers across a blob call
        self._lock = threading.Lock()  # guards the fields below, never held for a blob call
        self._points: dict[str, dict[str, dict]] = {}  # day -> ts -> point
        self._read: set[str] = set()  # days whose file is read, so memory has all of their points
        self._version: dict[str, int] = {}  # day -> count of changes
        self._written: dict[str, int] = {}  # day -> version of the last good write
        self._last_good: dict[str, datetime] = {}  # day -> time of the last good write
        self._last_write: datetime | None = None
        self._last_retain: datetime | None = None
        self._read_errors: dict[str, str] = {}
        self._write_error: str | None = None

    def _path(self, day: str) -> str:
        return f"{self._dir}/{day}.jsonl"

    def _today(self) -> str:
        return _day_of(self._now())

    def _fetch(self, day: str) -> dict[str, dict] | None:
        """Read the file of `day`. A missing or broken file gives no points. None means an error."""
        try:
            raw = self._store.get(self._path(day))
        except KeyError:
            self._read_errors.pop(day, None)
            return {}
        except Exception as exc:  # noqa: BLE001
            if str(exc) != self._read_errors.get(day):
                logger.error("cannot read %s: %s", self._path(day), exc)
            self._read_errors[day] = str(exc)
            return None
        self._read_errors.pop(day, None)
        return _parse_lines(raw, day)

    def _ensure_read(self, day: str) -> bool:
        """Merge the stored points of `day` under the points in memory once. Return False on an error.

        Call it with the write lock held. The current day stays read. A past day leaves memory.
        """
        with self._lock:
            if day in self._read:
                return True
        return self._read_again(day)

    def _read_again(self, day: str) -> bool:
        """Read the file of `day` and keep each stored point whose `ts` memory lacks. Return False on an error.

        Call it with the write lock held. The memory wins for a `ts` that both have. A write
        calls it before each put, so a point of another writer stays in the file.
        """
        stored = self._fetch(day)
        if stored is None:
            return False
        with self._lock:
            mine = self._points.setdefault(day, {})
            mine.update({ts: p for ts, p in stored.items() if ts not in mine})
            self._read.add(day)
        return True

    def load_today(self) -> None:
        """Read the file of the current day. A missing or broken file means no points.

        After another read error the day stays unread. The next write reads again, and
        it skips the day while the read fails.
        """
        with self._write_lock:
            self._ensure_read(self._today())

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
        self._read.discard(day)

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
        """Write the whole file of each day with new points. Never raises.

        Without `force` the store writes at most once in `WRITE_EVERY_SECONDS`.
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
            if not self._read_again(day):
                continue  # the points stay in memory, a later write reads again
            with self._lock:
                version = self._version.get(day, 0)
                points = [self._points[day][ts] for ts in sorted(self._points[day])]
            body = "".join(json.dumps(p) + "\n" for p in points)
            try:
                self._store.put(self._path(day), body)
            except Exception as exc:  # noqa: BLE001
                if str(exc) != self._write_error:
                    logger.error("cannot write %s: %s", self._path(day), exc)
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

        The points merge into at most `MAX_POINTS` time buckets, see `merge_points`. The failed days are the sorted days
        whose file read failed. A missing file is not a failure. A failed read logs one
        error for the call. Raise `ValueError` if `since` is after `until` or if the
        range is longer than `MAX_DAYS` days.
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
            complete = [d for d in days if d in self._read]
        stored: dict[str, dict[str, dict]] = {}
        failed: list[str] = []
        missing = [d for d in days if d not in complete]
        if missing:
            with ThreadPoolExecutor(max_workers=min(READ_THREADS, len(missing))) as pool:
                for day, found in zip(missing, pool.map(self._fetch_for_history, missing)):
                    if found is None:
                        failed.append(day)
                    else:
                        stored[day] = found
            if failed:
                logger.error("cannot read the metrics of %d of %d days", len(failed), len(days))
        chosen = []
        for day in days:
            merged = {**stored.get(day, {}), **memory[day]}
            for ts in sorted(merged):
                try:
                    moment = _parse_ts(ts)
                except ValueError:
                    continue
                if since <= moment <= until:
                    chosen.append(merged[ts])
        merged_points, step = merge_points(chosen, since, until)
        return merged_points, step, sorted(failed)

    def _fetch_for_history(self, day: str) -> dict[str, dict] | None:
        """Return the points of a day file. None means a read error."""
        try:
            raw = self._store.get(self._path(day))
        except KeyError:
            return {}
        except Exception:  # noqa: BLE001
            return None
        return _parse_lines(raw, day)

    def retain(self) -> None:
        """Delete the files of days older than the retention. Never raises.

        The store lists the directory at most once in an hour.
        """
        try:
            now = self._now()
            with self._lock:
                if self._last_retain is not None:
                    if (now - self._last_retain).total_seconds() < RETAIN_EVERY_SECONDS:
                        return
                self._last_retain = now
            oldest = (now.astimezone(timezone.utc) - timedelta(days=self._retention_days)).date().isoformat()
            for name in self._store.ls(self._dir):
                base = name.rsplit("/", 1)[-1]
                if not base.endswith(".jsonl"):
                    continue
                day = base[: -len(".jsonl")]
                try:
                    datetime.fromisoformat(day)
                except ValueError:
                    continue
                if day < oldest:
                    try:
                        self._store.delete(name)
                    except Exception as exc:  # noqa: BLE001
                        logger.error("cannot delete %s: %s", name, exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("the clean up of the metrics files failed: %s", exc)
