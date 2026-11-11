"""Merges of the audit metadata, and the retention of prompts.

The task `run_maintenance` calls `AuditMerger.tick` in a worker thread of its own, apart from
the flusher. A merge puts the segments
of a finished 10 minute bucket into one part, and the parts of a finished day into one day
file. Retention deletes the prompt folders of old days. See the sections "Merges" and
"Prompt retention" of `docs/audit-storage-design.md`. All times are UTC.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
MERGE_INTERVAL = timedelta(minutes=1)  # the shortest time between two merges of `tick`
RETAIN_INTERVAL = timedelta(hours=1)  # the time between two retention checks
RETAIN_DELETES_PER_CALL = 64  # prompt files that one `retain` call deletes at most
RETAIN_LISTINGS_PER_CALL = 20  # prompt folders that one `retain` call lists at most
RETAIN_TRIES = 3  # calls in a row that a prompt file can fail before its day waits
DELETE_WORKERS = 8  # threads that delete merged inputs or prompt files
MAINTENANCE_INTERVAL = 5.0  # seconds between two calls of `AuditMerger.tick` by `run_maintenance`


class _Folder:
    """The merge inputs of one day folder, from one listing."""

    __slots__ = ("segments", "parts", "has_day")

    def __init__(self) -> None:
        self.segments: dict[str, list[str]] = {}  # bucket -> segment names
        self.parts: set[str] = set()  # buckets that have a part
        self.has_day = False


def _day_folder(root: str, day: str) -> str:
    """Return the folder of one day under `audit/`."""
    return f"{audit_dir(root)}{day}/"


class AuditMerger:
    """Merges the audit metadata of `log` and deletes old prompts.

    Only one thread at a time calls `merge`, `retain` and `tick`: the maintenance thread of
    `run_maintenance`. A flush runs in the flusher thread and calls `flushed`. `merge` holds
    `log.flush_lock` for the listing of a day folder, and for the reads, the write and the
    `cache_put` of each merged file. It deletes the inputs without the lock. So a flush waits at
    most for the write of one merged file.
    `merge` lists only the day folders that can have work: the first merge after a start finds
    them with one listing of `audit/`, and `flushed` adds the day of each new segment. With
    `retention_days` None, `retain` keeps all prompts.
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
        self._last_merge: datetime | None = None  # time of the last merge of `tick`
        self._listed = False  # True after the first listing of `audit/`
        self._work: set[str] = set()  # day folders that can hold a segment or a part
        self._flushes: dict[str, int] = {}  # day -> segments that a flush wrote, for `_work`
        self._work_lock = threading.Lock()  # guards `_work` and `_flushes`
        self._warned: set[str] = set()  # names without an end time that got a warning
        self._last_retain: datetime | None = None  # time of the last retention check
        self._queue: list[str] = []  # days of the last check that wait for retention
        self._pending: list[str] | None = None  # prompt files of the head of the queue to delete
        self._failures: dict[str, int] = {}  # prompt file -> failed calls in a row
        self._cleaned: set[str] = set()  # days whose prompt folder is deleted

    def tick(self) -> None:
        """Merge at most one time each minute, then delete old prompts. Never raise.

        The first call after a start merges.
        """
        try:
            now = self._time()
            if self._last_merge is None or now - self._last_merge >= MERGE_INTERVAL:
                self._last_merge = now
                self.merge()
        except Exception as error:
            logger.warning("audit merge stopped: %s", error)
        self.retain()

    def flushed(self, day: str, seg: str, rows: list[dict]) -> None:
        """Note that a flush wrote the segment `seg` to `day`. Do no I/O. Never raise.

        It has the signature of `AuditLog.flushed_listeners`.
        """
        with self._work_lock:
            self._work.add(day)
            self._flushes[day] = self._flushes.get(day, 0) + 1

    def _time(self) -> datetime:
        now = self._now()
        return now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)

    # Merges

    def merge(self) -> None:
        """Merge each ready bucket into a part, then each ready day into a day file.

        A bucket is ready 1 minute after its 10 minutes end. A day is ready 15 minutes after
        it ends, when its folder has no segment left. Only the day folders that can have work
        are listed. A folder leaves that set when it holds no segment and no part after the
        merge, and no flush wrote to it after its listing. A failed listing, write or delete is
        logged and the other merges go on. Never raise.
        """
        try:
            now = self._time()
            if not self._listed:
                self._list_all()
            with self._work_lock:
                days = sorted(self._work)
            for day in days:
                try:
                    with self._log.flush_lock:
                        folder = self._folder(day)
                        with self._work_lock:
                            flushes = self._flushes.get(day, 0)
                    self._merge_buckets(day, folder, now)
                    self._merge_day(day, folder, now)
                except Exception as error:
                    logger.warning("audit merge of %s stopped: %s", day, error)
                    continue
                if not folder.segments and not folder.parts:
                    with self._work_lock:
                        # Keep the day when a flush wrote to it after the listing.
                        if self._flushes.get(day, 0) == flushes:
                            self._work.discard(day)
                            self._flushes.pop(day, None)
        except Exception as error:
            logger.warning("audit merge stopped: %s", error)

    def _list_all(self) -> None:
        """List `audit/` one time and note each day folder that holds a segment or a part."""
        days = set()
        for path in self._store.ls(audit_dir(self._root)):
            info = classify(self._root, path)
            if info is not None and info[1] in ("segment", "part"):
                days.add(info[0])
        with self._work_lock:
            self._work |= days
        self._listed = True

    def _folder(self, day: str) -> _Folder:
        """List one day folder and group its merge inputs. Old layout files are left out."""
        folder = _Folder()
        for path in self._store.ls(_day_folder(self._root, day)):
            info = classify(self._root, path)
            if info is None or info[0] != day:
                continue
            _, kind, name = info
            if kind == "legacy":
                continue
            if kind == "segment":
                folder.segments.setdefault(bucket_of(name), []).append(name)
            elif kind == "part":
                folder.parts.add(name)
            else:
                folder.has_day = True
        return folder

    def _end(self, end_of: Callable[[str], datetime], name: str) -> datetime | None:
        """Return the end time of a bucket or a day, or None for a name that `end_of` rejects.

        The first time a name is rejected, log a warning.
        """
        try:
            return end_of(name)
        except ValueError:
            if name not in self._warned:
                self._warned.add(name)
                logger.warning("the audit name %r has no end time, it is never merged", name)
            return None

    def _merge_buckets(self, day: str, folder: _Folder, now: datetime) -> None:
        for bucket in sorted(folder.segments):
            end = self._end(bucket_end, bucket)
            if end is None or now < end + BUCKET_DELAY:
                continue
            names = sorted(folder.segments[bucket])
            target = part_path(self._root, day, bucket)
            inputs = [segment_path(self._root, day, name) for name in names]
            label = f"part bucket {bucket}"
            left = self._merge_files(day, label, target, bucket in folder.parts, inputs)
            if left is None:
                continue
            folder.parts.add(bucket)
            if left:
                folder.segments[bucket] = [n for n, p in zip(names, inputs) if p in left]
            else:
                del folder.segments[bucket]

    def _merge_day(self, day: str, folder: _Folder, now: datetime) -> None:
        if not folder.parts or folder.segments:
            return
        end = self._end(day_end, day)
        if end is None or now < end + DAY_DELAY:
            return
        buckets = sorted(folder.parts)
        inputs = [part_path(self._root, day, bucket) for bucket in buckets]
        target = day_path(self._root, day)
        left = self._merge_files(day, "the day file", target, folder.has_day, inputs)
        if left is not None:
            folder.has_day = True
            folder.parts = {b for b, path in zip(buckets, inputs) if path in left}

    def _merge_files(
        self, day: str, label: str, target: str, exists: bool, inputs: list[str]
    ) -> list[str] | None:
        """Write the rows of `inputs` to `target`, then delete the inputs.

        When `exists` is true, the rows of `target` come first. A row whose `seg` occurred in
        an earlier file is skipped. `log.flush_lock` is held for the reads, the write and the
        `cache_put`, and not for the deletes. A reader that sees the merged file and an input
        drops the copies by `seg`. Return the inputs that are left after a failed delete, or
        None when a read or the write failed and nothing changed. `label` names the target in
        the log line.
        """
        started = time.monotonic()
        rows: list[dict] = []
        seen: set[str] = set()
        with self._log.flush_lock:
            try:
                for chunk in self._log.read_many(([target] if exists else []) + inputs):
                    segs = set()
                    for row in chunk:
                        segs.add(row["seg"])
                        if row["seg"] not in seen:
                            rows.append(row)
                    seen |= segs
                self._store.put(target, dump_lines(rows))
            except Exception as error:
                logger.warning("audit merge into %s failed, keeping its inputs: %s", target, error)
                return None
            self._log.cache_put(target, rows)
        failed = self._delete_many(inputs, "merged audit file")
        self._log.cache_drop([path for path in inputs if path not in failed])
        logger.info(
            "merged %d files of %s into %s in %.1f s (%d rows)",
            len(inputs),
            day,
            label,
            time.monotonic() - started,
            len(rows),
        )
        return [path for path in inputs if path in failed]

    # Prompt retention

    def retain(self) -> None:
        """Delete the prompt files of each day that ended more than `retention_days` days ago.

        A check runs at most one time each hour. It takes the day names from `log.days()`,
        which lists `audit/`, and queues the days past the limit that are not cleaned yet,
        newest first. While the queue is not empty, the next call goes on with it without a
        wait. One call makes at most `RETAIN_DELETES_PER_CALL` deletes and lists at most
        `RETAIN_LISTINGS_PER_CALL` prompt folders. See `_retain_day`. A failed listing of
        `audit/` waits for the next hourly check. Never raise.
        """
        if self._retention_days is None:
            return
        try:
            if not self._queue:
                now = self._time()
                if self._last_retain is not None and now - self._last_retain < RETAIN_INTERVAL:
                    return
                self._last_retain = now
                limit = timedelta(days=self._retention_days)
                self._queue = [
                    day
                    for day in sorted(self._log.days(), reverse=True)
                    if day not in self._cleaned and self._past(day, limit, now)
                ]
                self._pending = None
            budget = [RETAIN_DELETES_PER_CALL, RETAIN_LISTINGS_PER_CALL]
            while self._queue and budget[0] > 0:
                if not self._retain_day(self._queue[0], budget):
                    break
        except Exception as error:
            logger.warning("prompt retention stopped: %s", error)

    def _retain_day(self, day: str, budget: list[int]) -> bool:
        """Do one retention step on `day`, the head of the queue. Return False to end the call.

        The first step lists the prompt folder of the day and keeps the list of its files.
        Each step deletes the next files of that list, at most `budget[0]` of them, with
        `DELETE_WORKERS` threads. `budget` holds the deletes and the listings that are left
        for this call. A file whose delete failed stays in the list and is tried at the next
        call. When the same file fails `RETAIN_TRIES` calls in a row, the rest of the day
        waits for the next hourly check. The day is cleaned when its list is empty.
        """
        if self._pending is None:
            if budget[1] == 0:
                return False
            budget[1] -= 1
            try:
                self._pending = self._store.ls(prompts_dir(self._root, day))
            except Exception as error:
                logger.warning("cannot list the prompts of %s: %s", day, error)
                self._next_day()
                return True
            self._failures = {}
        batch = self._pending[: budget[0]]
        budget[0] -= len(batch)
        failed = self._delete_many(batch, "prompt file")
        self._pending = [path for path in batch if path in failed] + self._pending[len(batch) :]
        deleted = len(batch) - len(failed)
        if deleted:
            logger.info(
                "retention deleted %d prompt files of %s, %d left", deleted, day, len(self._pending)
            )
        if deleted or not self._pending:
            self._log.prompt_cache_drop(day)
        if not self._pending:
            self._cleaned.add(day)
            self._next_day()
            return True
        if not failed:
            return True
        self._failures = {path: self._failures.get(path, 0) + 1 for path in failed}
        if max(self._failures.values()) >= RETAIN_TRIES:
            logger.warning("prompt retention of %s stops until the next hourly check", day)
            self._next_day()
            return True
        return False

    def _next_day(self) -> None:
        """Remove the head of the retention queue and forget its list of files."""
        self._queue.pop(0)
        self._pending = None
        self._failures = {}

    def _past(self, day: str, limit: timedelta, now: datetime) -> bool:
        """Return True when `day` ended more than `limit` before `now`."""
        end = self._end(day_end, day)
        return end is not None and now > end + limit

    def _delete_many(self, paths: list[str], kind: str) -> set[str]:
        """Delete `paths` with `DELETE_WORKERS` threads. Return the paths whose delete failed.

        Each failed delete is logged with one warning that names `kind`.
        """

        def delete(path: str) -> bool:
            try:
                self._store.delete(path)
                return True
            except Exception as error:
                logger.warning("cannot delete the %s %s: %s", kind, path, error)
                return False

        if len(paths) < 2:
            results = [delete(path) for path in paths]
        else:
            with ThreadPoolExecutor(max_workers=min(DELETE_WORKERS, len(paths))) as pool:
                results = list(pool.map(delete, paths))
        return {path for path, ok in zip(paths, results) if not ok}


async def run_maintenance(tick: Callable[[], None], interval: float = MAINTENANCE_INTERVAL) -> None:
    """Call `tick` in a worker thread, then sleep `interval` seconds, until cancelled.

    `tick` is `AuditMerger.tick`. It has its own limits: a merge at most one time each minute,
    a retention check each hour. The loop awaits each call, so one call runs at a time. An
    error in `tick` is logged and the loop goes on. A cancel returns at once. A call that runs
    then ends in its thread by itself.
    """
    while True:
        try:
            await asyncio.to_thread(tick)
        except Exception as error:
            logger.warning("the maintenance step merge failed: %s", error)
        await asyncio.sleep(interval)
