"""Merges of the audit metadata, and the retention of prompts.

The flusher thread calls `AuditMerger.tick` after each flush attempt. A merge puts the segments
of a finished 10 minute bucket into one part, and the parts of a finished day into one day
file. Retention deletes the prompt folders of old days. See the sections "Merges" and
"Prompt retention" of `docs/audit-storage-design.md`. All times are UTC.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from llm_service.gateway.audit import AuditLog
from llm_service.gateway.audit_layout import (
    audit_dir,
    bucket_end,
    bucket_of,
    classify,
    day_end,
    day_path,
    dump_lines,
    part_path,
    prompts_dir,
    segment_path,
)
from llm_service.gateway.storage import BlobStore

logger = logging.getLogger("llm_service.audit")

BUCKET_DELAY = timedelta(minutes=1)  # a bucket is ready this long after its 10 minutes end
DAY_DELAY = timedelta(minutes=15)  # a day is ready this long after it ends
RETAIN_INTERVAL = timedelta(hours=1)  # the time between two retention checks


class _Folder:
    """The merge inputs of one day folder, from one listing."""

    __slots__ = ("segments", "parts", "has_day")

    def __init__(self) -> None:
        self.segments: dict[str, list[str]] = {}  # bucket -> segment names
        self.parts: set[str] = set()  # buckets that have a part
        self.has_day = False


def _ready(end_of: Callable[[str], datetime], name: str, delay: timedelta, now: datetime) -> bool:
    """Return True when `now` is at least `delay` after the end of `name`.

    A name that `end_of` rejects is never ready.
    """
    try:
        return now >= end_of(name) + delay
    except ValueError:
        logger.debug("skipping the audit name %r, it has no end time", name)
        return False


class AuditMerger:
    """Merges the audit metadata of `log` and deletes old prompts.

    Only the flusher thread calls it. `merge` holds `log.flush_lock`, so no flush runs
    during a merge. With `retention_days` None, `retain` keeps all prompts.
    """

    def __init__(
        self,
        store: BlobStore,
        root: str,
        log: AuditLog,
        now: Callable[[], datetime],
        retention_days: int | None = None,
    ) -> None:
        self._store = store
        self._root = root.rstrip("/")
        self._log = log
        self._now = now
        self._retention_days = retention_days
        self._last_retain: datetime | None = None  # time of the last retention check
        self._cleaned: set[str] = set()  # days whose prompt folder is deleted

    def tick(self) -> None:
        """Merge, then delete old prompts. Never raise."""
        self.merge()
        self.retain()

    def _time(self) -> datetime:
        now = self._now()
        return now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)

    # Merges

    def merge(self) -> None:
        """Merge each ready bucket into a part, then each ready day into a day file.

        A bucket is ready 1 minute after its 10 minutes end. A day is ready 15 minutes after
        it ends, when its folder has no segment left. A failed write or delete is logged and
        the other merges go on. Never raise.
        """
        try:
            with self._log.flush_lock:
                now = self._time()
                for day, folder in sorted(self._folders().items()):
                    self._merge_buckets(day, folder, now)
                    self._merge_day(day, folder, now)
        except Exception as error:
            logger.warning("audit merge stopped: %s", error)

    def _folders(self) -> dict[str, _Folder]:
        """List `audit/` one time and group the merge inputs by day. Old layout files are left out."""
        folders: dict[str, _Folder] = {}
        for path in self._store.ls(audit_dir(self._root)):
            info = classify(self._root, path)
            if info is None:
                continue
            day, kind, name = info
            if kind == "legacy":
                continue
            folder = folders.setdefault(day, _Folder())
            if kind == "segment":
                folder.segments.setdefault(bucket_of(name), []).append(name)
            elif kind == "part":
                folder.parts.add(name)
            else:
                folder.has_day = True
        return folders

    def _merge_buckets(self, day: str, folder: _Folder, now: datetime) -> None:
        for bucket in sorted(folder.segments):
            if not _ready(bucket_end, bucket, BUCKET_DELAY, now):
                continue
            names = sorted(folder.segments[bucket])
            target = part_path(self._root, day, bucket)
            inputs = [segment_path(self._root, day, name) for name in names]
            left = self._merge_files(target, bucket in folder.parts, inputs)
            if left is None:
                continue
            folder.parts.add(bucket)
            if left:
                folder.segments[bucket] = [n for n, p in zip(names, inputs) if p in left]
            else:
                del folder.segments[bucket]

    def _merge_day(self, day: str, folder: _Folder, now: datetime) -> None:
        if not folder.parts or folder.segments or not _ready(day_end, day, DAY_DELAY, now):
            return
        inputs = [part_path(self._root, day, bucket) for bucket in sorted(folder.parts)]
        self._merge_files(day_path(self._root, day), folder.has_day, inputs)

    def _merge_files(self, target: str, exists: bool, inputs: list[str]) -> list[str] | None:
        """Write the rows of `inputs` to `target`, then delete the inputs.

        When `exists` is true, the rows of `target` come first. A row whose `seg` occurred in
        an earlier file is skipped. Return the inputs that are left after a failed delete, or
        None when a read or the write failed and nothing changed.
        """
        rows: list[dict] = []
        seen: set[str] = set()
        try:
            for path in ([target] if exists else []) + inputs:
                segs = set()
                for row in self._log.read_rows(path):
                    segs.add(row["seg"])
                    if row["seg"] not in seen:
                        rows.append(row)
                seen |= segs
            self._store.put(target, dump_lines(rows))
        except Exception as error:
            logger.warning("audit merge into %s failed, keeping its inputs: %s", target, error)
            return None
        self._log.cache_put(target, rows)
        left = []
        for path in inputs:
            try:
                self._store.delete(path)
            except Exception as error:
                logger.warning("cannot delete the merged audit file %s: %s", path, error)
                left.append(path)
                continue
            self._log.cache_drop([path])
        return left

    # Prompt retention

    def retain(self) -> None:
        """Delete the prompt folder of each day that ended `retention_days` days ago or more.

        The check runs at most one time each hour. The day names come from the folders of
        `audit/`. A cleaned day is not listed again. Never raise.
        """
        if self._retention_days is None:
            return
        try:
            now = self._time()
            if self._last_retain is not None and now - self._last_retain < RETAIN_INTERVAL:
                return
            self._last_retain = now
            limit = timedelta(days=self._retention_days)
            days = set()
            for path in self._store.ls(audit_dir(self._root)):
                info = classify(self._root, path)
                if info is not None:
                    days.add(info[0])
            for day in sorted(days - self._cleaned):
                if _ready(day_end, day, limit, now) and self._delete_prompts(day):
                    self._cleaned.add(day)
        except Exception as error:
            logger.warning("prompt retention stopped: %s", error)

    def _delete_prompts(self, day: str) -> bool:
        """Delete the prompt files of `day`. Return True when every delete worked."""
        try:
            paths = self._store.ls(prompts_dir(self._root, day))
        except Exception as error:
            logger.warning("cannot list the prompts of %s: %s", day, error)
            return False
        done = True
        for path in paths:
            try:
                self._store.delete(path)
            except Exception as error:
                logger.warning("cannot delete the prompt file %s: %s", path, error)
                done = False
        self._log.prompt_cache_drop(day)
        return done
