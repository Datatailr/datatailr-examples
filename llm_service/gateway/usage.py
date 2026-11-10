"""Usage totals by user, model and day, computed from the audit records."""

from __future__ import annotations

import json
import logging
import threading
from typing import Callable

from llm_service.gateway.audit import AuditLog
from llm_service.gateway.storage import BlobStore

logger = logging.getLogger("llm_service.usage")

_COUNTERS = ("requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens")
_GROUPS = ("user", "model", "day")
_KEY_INDEX = {"day": 0, "user": 1, "model": 2}  # position in the `(day, user, model)` key


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


class UsageTotals:
    """Keeps request and token totals in memory, keyed by `(day, user, model)`.

    The audit log is the source of truth. A finished day is also saved to
    `<root>/usage/<day>.json`, so that `load` reads one small file for it instead
    of all the audit files of that day.
    """

    def __init__(
        self,
        store: BlobStore,
        root: str,
        audit: AuditLog,
        today: Callable[[], str],
    ) -> None:
        self._store = store
        self._prefix = root.rstrip("/") + "/usage"
        self._audit = audit
        self._today = today
        self._lock = threading.Lock()  # guards the three fields below
        self._totals: dict[tuple[str, str, str], dict] = {}
        self._saved: set[str] = set()  # days whose daily file is up to date
        self._newest: str | None = None

    def _path(self, day: str) -> str:
        return f"{self._prefix}/{day}.json"

    def load(self) -> None:
        """Read the daily files, rebuild the days without one, and save finished days."""
        for day in self._audit.days():
            rows = self._read_day(day)
            if rows is None:
                rows = self._rebuild_day(day)
                saved = False
            else:
                saved = True
            with self._lock:
                for key in [k for k in self._totals if k[0] == day]:
                    del self._totals[key]
                for row in rows:
                    key = (day, row["user"], row["model"])
                    entry = _empty_row()
                    _add(entry, row)
                    self._totals[key] = entry
                if saved:
                    self._saved.add(day)
                else:
                    self._saved.discard(day)
                if self._newest is None or day > self._newest:
                    self._newest = day
        self.close_days()

    def _read_day(self, day: str) -> list[dict] | None:
        """Return the rows of the daily file, or None if it is missing or invalid."""
        path = self._path(day)
        try:
            if not self._store.exists(path):
                return None
            data = json.loads(self._store.get(path).decode("utf-8"))
            rows = data["rows"]
            if data["day"] != day or not isinstance(rows, list):
                raise ValueError("wrong day or rows")
            for row in rows:
                if not isinstance(row["user"], str) or not isinstance(row["model"], str):
                    raise ValueError("bad user or model")
                for name in _COUNTERS:
                    if not isinstance(row[name], int):
                        raise ValueError(f"bad {name}")
                if row["last_request"] is not None and not isinstance(row["last_request"], str):
                    raise ValueError("bad last_request")
            return rows
        except KeyError:
            return None
        except (ValueError, TypeError, AttributeError) as error:
            logger.warning("ignoring invalid usage file %s, rebuilding the day: %s", path, error)
            return None

    def _rebuild_day(self, day: str) -> list[dict]:
        totals: dict[tuple[str, str], dict] = {}
        for record in self._audit.records_for_day(day):
            parts = self._parse(record)
            if parts is None:
                continue
            _, user, model, row = parts
            entry = totals.setdefault((user, model), _empty_row())
            _add(entry, row)
        return [{"user": u, "model": m, **row} for (u, m), row in sorted(totals.items())]

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
        """Add one audit record to the totals. Never raises."""
        try:
            parts = self._parse(rec)
            if parts is None:
                return
            day, user, model, row = parts
            with self._lock:
                rollover = self._newest is None or day > self._newest
            if rollover:
                self.close_days()
            with self._lock:
                if self._newest is None or day > self._newest:
                    self._newest = day
                entry = self._totals.setdefault((day, user, model), _empty_row())
                _add(entry, row)
                self._saved.discard(day)  # a late record makes the daily file stale
        except Exception as error:
            logger.warning("could not count an audit record: %s", error)

    def close_days(self) -> None:
        """Write the daily file of each finished day that has none.

        A failed write is logged and tried again at the next call.
        """
        today = self._today()
        with self._lock:
            pending: dict[str, list[dict]] = {}
            for (day, user, model), entry in sorted(self._totals.items()):
                if day < today and day not in self._saved:
                    pending.setdefault(day, []).append({"user": user, "model": model, **entry})
        for day, rows in sorted(pending.items()):
            body = json.dumps({"day": day, "rows": rows}, separators=(",", ":"))
            try:
                self._store.put(self._path(day), body)
            except Exception as error:
                logger.warning("could not write the usage file of %s, will retry: %s", day, error)
                continue
            with self._lock:
                self._saved.add(day)

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
