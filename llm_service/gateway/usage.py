"""Usage totals by user, model and day, computed from the audit records."""

from __future__ import annotations

import json
import logging
import re
import threading
from datetime import datetime, timedelta
from typing import Callable

from llm_service.gateway.audit import AuditLog, utc_now
from llm_service.gateway.storage import BlobStore

logger = logging.getLogger("llm_service.usage")

_COUNTERS = ("requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens")
_GROUPS = ("user", "model", "day")
_KEY_INDEX = {"day": 0, "user": 1, "model": 2}  # position in the `(day, user, model)` key
CHECKPOINT_INTERVAL = timedelta(seconds=60)  # the shortest time between two checkpoint writes
_SEGMENT_NAME = re.compile(r"\d{8}T\d{6}\.\d{6}-.+")


def is_segment_name(name: object) -> bool:
    """Return True for a segment name `<stamp>-<id>`, False for an old layout file name.

    Only a segment name can become `through`. Old names do not sort with segment names.
    """
    return isinstance(name, str) and _SEGMENT_NAME.fullmatch(name) is not None


def _empty_row() -> dict:
    row = {name: 0 for name in _COUNTERS}
    row["last_request"] = None
    return row


def _add(target: dict, source: dict) -> None:
    for name in _COUNTERS:
        target[name] += source[name]
    last = source["last_request"]
    if last is not None and (target["last_request"] is None or last > target["last_request"]):
        target["last_request"] = last


def _file_rows(totals: dict[tuple[str, str], dict]) -> list[dict]:
    """Return totals keyed by `(user, model)` as the rows of a usage file, sorted."""
    return [{"user": u, "model": m, **row} for (u, m), row in sorted(totals.items())]


def _check_rows(rows: object) -> None:
    """Raise `ValueError` unless `rows` is a list of valid usage file rows."""
    if not isinstance(rows, list):
        raise ValueError("rows is not a list")
    for row in rows:
        if not isinstance(row["user"], str) or not isinstance(row["model"], str):
            raise ValueError("bad user or model")
        for name in _COUNTERS:
            if not isinstance(row[name], int):
                raise ValueError(f"bad {name}")
        if row["last_request"] is not None and not isinstance(row["last_request"], str):
            raise ValueError("bad last_request")


class UsageTotals:
    """Keeps request and token totals in memory, keyed by `(day, user, model)`.

    The audit log is the source of truth. A finished day is also saved to
    `<root>/usage/<day>.json`, so that `load` reads one small file for it instead
    of all the audit files of that day.

    The live totals count a record when it enters the audit buffer. For each day
    without a daily file, a second set, the flushed totals, counts only the rows that
    a flush wrote, up to the segment `through`. `checkpoint` saves them to
    `<root>/usage/<day>.partial.json`, so that `load` reads that file and the rows
    after `through` only.

    `record` and `flushed` do no storage I/O and never raise. `load`, `checkpoint`
    and `close_days` hold the lock only between storage calls.
    """

    def __init__(
        self,
        store: BlobStore,
        root: str,
        audit: AuditLog,
        today: Callable[[], str],
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._prefix = root.rstrip("/") + "/usage"
        self._audit = audit
        self._today = today
        self._now = now if now is not None else utc_now
        self._lock = threading.Lock()  # guards all fields below
        self._totals: dict[tuple[str, str, str], dict] = {}
        self._saved: set[str] = set()  # days whose daily file is up to date
        self._newest: str | None = None
        self._closed: set[str] = set()  # days that have a daily file
        self._flushed: dict[str, dict[tuple[str, str], dict]] = {}  # day -> flushed totals
        self._through: dict[str, str | None] = {}  # day -> the greatest flushed segment name
        self._version: dict[str, int] = {}  # day -> changes of the flushed totals
        self._written: dict[str, int] = {}  # day -> the version of the last checkpoint write
        self._stale_partials: set[str] = set()  # days whose partial file must be deleted
        self._last_checkpoint: datetime | None = None  # clock time of the last checkpoint write

    def _path(self, day: str) -> str:
        return f"{self._prefix}/{day}.json"

    def _partial_path(self, day: str) -> str:
        return f"{self._prefix}/{day}.partial.json"

    def load(self) -> None:
        """Read the totals of each day, then save finished days. See "Start" in the design.

        A day with a daily file uses it. Else a day with a partial file uses its rows and the
        rows after its `through`. Else the day is read in full. Buffered rows count in the
        live totals only.
        """
        partials = self._partial_days()
        for day in self._audit.days():
            daily = self._read_day(day)
            if daily is not None:
                with self._lock:
                    self._set_live(day, daily)
                    self._saved.add(day)
                    self._closed.add(day)
                    self._forget_flushed(day)
                    if day in partials:
                        self._stale_partials.add(day)
                continue
            saved = self._read_partial(day) if day in partials else None
            if saved is not None and saved["through"] is not None:
                start, through = saved["rows"], saved["through"]
                later = self._audit.rows_for_day(day, after=through)
            else:
                start, through = [], None
                later = self._audit.rows_for_day(day)
            flushed: dict[tuple[str, str], dict] = {}
            for row in start:
                _add(flushed.setdefault((row["user"], row["model"]), _empty_row()), row)
            live = {key: dict(entry) for key, entry in flushed.items()}
            for record in later:
                seg = record.get("seg")
                parts = self._parse(record)
                if parts is not None:
                    _, user, model, row = parts
                    _add(live.setdefault((user, model), _empty_row()), row)
                    if seg is not None:  # a buffered row is not flushed
                        _add(flushed.setdefault((user, model), _empty_row()), row)
                if is_segment_name(seg) and (through is None or seg > through):
                    through = seg
            unchanged = saved is not None and saved["through"] == through
            with self._lock:
                self._set_live(day, _file_rows(live))
                self._saved.discard(day)
                self._closed.discard(day)
                self._flushed[day] = flushed
                self._through[day] = through
                version = self._version.get(day, 0) + 1
                self._version[day] = version
                if unchanged:
                    self._written[day] = version
                else:
                    self._written.pop(day, None)
        self.close_days()

    def _set_live(self, day: str, rows: list[dict]) -> None:
        """Replace the live totals of `day` with `rows`. Hold the lock."""
        for key in [k for k in self._totals if k[0] == day]:
            del self._totals[key]
        for row in rows:
            entry = _empty_row()
            _add(entry, row)
            self._totals[(day, row["user"], row["model"])] = entry
        if self._newest is None or day > self._newest:
            self._newest = day

    def _forget_flushed(self, day: str) -> None:
        """Drop the flushed totals of `day`. Hold the lock."""
        for state in (self._flushed, self._through, self._version, self._written):
            state.pop(day, None)

    def _partial_days(self) -> set[str]:
        """Return the days that have a partial file. One listing of `usage/`."""
        suffix = ".partial.json"
        days = set()
        for path in self._store.ls(self._prefix):
            name = path[len(self._prefix) + 1 :]
            if name.endswith(suffix) and "/" not in name:
                days.add(name[: -len(suffix)])
        return days

    def _read_usage(self, path: str, day: str, partial: bool) -> dict | None:
        """Return a valid usage file of `day`, or None if it is missing or invalid.

        An invalid file is logged with one warning.
        """
        try:
            data = self._store.get(path)
        except KeyError:
            return None
        kind = "partial usage file" if partial else "usage file"
        try:
            body = json.loads(data.decode("utf-8"))
            if body["day"] != day:
                raise ValueError("wrong day")
            _check_rows(body["rows"])
            if partial and body["through"] is not None and not is_segment_name(body["through"]):
                raise ValueError("through is not a segment name")
            return body
        except (KeyError, ValueError, TypeError, AttributeError) as error:
            logger.warning("ignoring invalid %s %s, rebuilding the day: %s", kind, path, error)
            return None

    def _read_day(self, day: str) -> list[dict] | None:
        """Return the rows of the daily file, or None if it is missing or invalid."""
        path = self._path(day)
        if not self._store.exists(path):
            return None
        body = self._read_usage(path, day, partial=False)
        return None if body is None else body["rows"]

    def _read_partial(self, day: str) -> dict | None:
        """Return the partial file of `day` (`day`, `through`, `rows`), or None."""
        return self._read_usage(self._partial_path(day), day, partial=True)

    @staticmethod
    def _parse(record: dict) -> tuple[str, str, str, dict] | None:
        """Return `(day, user, model, row)` for a counted record, else None."""
        if not str(record.get("endpoint", "")).startswith("/v1/"):
            return None
        ts = record["ts"]
        row = _empty_row()
        row["requests"] = 1
        row["failed_requests"] = 1 if (record.get("status") or 0) >= 400 else 0
        row["prompt_tokens"] = record.get("prompt_tokens") or 0
        row["completion_tokens"] = record.get("completion_tokens") or 0
        row["total_tokens"] = record.get("total_tokens") or 0
        row["last_request"] = ts
        return ts[:10], str(record["user"]), str(record.get("model") or ""), row

    def record(self, rec: dict) -> None:
        """Add one audit record to the totals. Never raises and does no storage I/O.

        A finished day gets its file at the next `close_days` call.
        """
        try:
            parts = self._parse(rec)
            if parts is None:
                return
            day, user, model, row = parts
            with self._lock:
                if self._newest is None or day > self._newest:
                    self._newest = day
                entry = self._totals.setdefault((day, user, model), _empty_row())
                _add(entry, row)
                self._saved.discard(day)  # a late record makes the daily file stale
        except Exception as error:
            logger.warning("could not count an audit record: %s", error)

    def flushed(self, day: str, seg: str, rows: list[dict]) -> None:
        """Add the rows of one written segment to the flushed totals of `day`.

        It is an `AuditLog.flushed_listeners` callback. A day with a daily file keeps no
        flushed totals. Never raises and does no storage I/O.
        """
        try:
            parsed = [self._parse(row) for row in rows]
            with self._lock:
                if day in self._closed:
                    return
                totals = self._flushed.setdefault(day, {})
                for parts in parsed:
                    if parts is not None:
                        _, user, model, row = parts
                        _add(totals.setdefault((user, model), _empty_row()), row)
                through = self._through.get(day)
                if is_segment_name(seg) and (through is None or seg > through):
                    self._through[day] = seg
                self._version[day] = self._version.get(day, 0) + 1
        except Exception as error:
            logger.warning("could not count a flushed audit segment: %s", error)

    def checkpoint(self) -> None:
        """Write the partial file of each day whose flushed totals changed. Never raises.

        It writes at most one time each `CHECKPOINT_INTERVAL` by the clock. A failed
        write is logged and tried again at the next call.
        """
        now = self._now()
        with self._lock:
            last = self._last_checkpoint
            if last is not None and timedelta(0) <= now - last < CHECKPOINT_INTERVAL:
                return
            due = [
                (day, version, self._through[day], _file_rows(self._flushed[day]))
                for day, version in sorted(self._version.items())
                if day not in self._closed
                and self._through.get(day) is not None
                and self._written.get(day) != version
            ]
        for day, version, through, rows in due:
            body = json.dumps({"day": day, "through": through, "rows": rows}, separators=(",", ":"))
            try:
                self._store.put(self._partial_path(day), body)
            except Exception as error:
                logger.warning(
                    "could not write the partial usage file of %s, will retry: %s", day, error
                )
                continue
            with self._lock:
                self._last_checkpoint = now
                if day in self._closed:  # the day closed during the write
                    self._stale_partials.add(day)
                else:
                    self._written[day] = version

    def close_days(self) -> None:
        """Write the daily file of each finished day that has none, then delete its partial file.

        A failed write or delete is logged and tried again at the next call.
        """
        today = self._today()
        with self._lock:
            pending: dict[str, list[dict]] = {}
            for (day, user, model), entry in sorted(self._totals.items()):
                if day < today and day not in self._saved:
                    pending.setdefault(day, []).append({"user": user, "model": model, **entry})
            for day in self._flushed:  # a day whose rows all were events
                if day < today and day not in self._saved:
                    pending.setdefault(day, [])
        for day, rows in sorted(pending.items()):
            body = json.dumps({"day": day, "rows": rows}, separators=(",", ":"))
            try:
                self._store.put(self._path(day), body)
            except Exception as error:
                logger.warning("could not write the usage file of %s, will retry: %s", day, error)
                continue
            with self._lock:
                self._saved.add(day)
                self._closed.add(day)
                self._forget_flushed(day)
                self._stale_partials.add(day)
        with self._lock:
            stale = sorted(self._stale_partials)
        for day in stale:
            try:
                self._store.delete(self._partial_path(day))
            except Exception as error:
                logger.warning(
                    "could not delete the partial usage file of %s, will retry: %s", day, error
                )
                continue
            with self._lock:
                self._stale_partials.discard(day)

    def summary(
        self,
        since: str | None,
        until: str | None,
        user: str | None,
        model: str | None,
        group_by: str = "user",
    ) -> list[dict]:
        """Return the totals grouped by `group_by` (`user`, `model` or `day`).

        `since` and `until` are days and both are inclusive. Rows are sorted by
        total tokens, largest first, then by key.
        """
        if group_by not in _GROUPS:
            raise ValueError(f"group_by must be one of {', '.join(_GROUPS)}, not {group_by!r}")
        index = _KEY_INDEX[group_by]
        groups: dict[str, dict] = {}
        with self._lock:
            for key, entry in self._totals.items():
                day = key[0]
                if (since and day < since) or (until and day > until):
                    continue
                if user is not None and key[1] != user:
                    continue
                if model is not None and key[2] != model:
                    continue
                _add(groups.setdefault(key[index], _empty_row()), entry)
        rows = [{group_by: name, **row} for name, row in groups.items()]
        rows.sort(key=lambda r: (-r["total_tokens"], r[group_by]))
        return rows

    def user_detail(self, user: str, since: str | None, until: str | None) -> dict:
        """Return the totals of one user, with the breakdown by model and by day."""
        found = self.summary(since, until, user, None, "user")
        totals = found[0] if found else {"user": user, **_empty_row()}
        return {
            "user": user,
            "totals": totals,
            "by_model": self.summary(since, until, user, None, "model"),
            "by_day": self.summary(since, until, user, None, "day"),
        }
