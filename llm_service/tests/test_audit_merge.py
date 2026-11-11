import asyncio
import contextlib
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from llm_service.gateway.audit import AuditLog
from llm_service.gateway.audit_layout import (
    audit_dir,
    day_path,
    dump_lines,
    parse_lines,
    part_path,
    classify,
    prompt_marker_path,
    prompt_path,
    prompts_dir,
    segment_path,
)
from llm_service.gateway.audit_merge import AuditMerger, run_maintenance
from llm_service.gateway.storage import MemoryBlobStore

ROOT = "root"
DAY = "2026-11-11"


class Clock:
    """A clock that a test sets by hand."""

    def __init__(self, when: datetime) -> None:
        self.when = when

    def __call__(self) -> datetime:
        return self.when


def at(day: int, hour: int, minute: int, second: int = 0, micro: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, second, micro, tzinfo=timezone.utc)


class Store(MemoryBlobStore):
    """A memory store that records `ls`, `get` and `delete` calls and fails chosen writes and deletes."""

    def __init__(self) -> None:
        super().__init__()
        self.ls_calls: list[str] = []
        self.gets: list[str] = []
        self.deletes: list[str] = []
        self.fail_put = lambda path: False
        self.fail_delete: set[str] = set()
        self._calls_lock = threading.Lock()

    def put(self, path, data):
        if self.fail_put(path):
            raise OSError("simulated write failure")
        super().put(path, data)

    def get(self, path):
        with self._calls_lock:
            self.gets.append(path)
        return super().get(path)

    def delete(self, path):
        with self._calls_lock:
            self.deletes.append(path)
        if path in self.fail_delete:
            raise OSError("simulated delete failure")
        super().delete(path)

    def ls(self, prefix):
        with self._calls_lock:
            self.ls_calls.append(prefix)
        return super().ls(prefix)


def rec(user: str, ts: str) -> dict:
    return {
        "ts": ts,
        "request_id": f"{user}-{ts}",
        "user": user,
        "endpoint": "/v1/chat/completions",
        "model": "m",
        "stream": False,
        "prompt": {"messages": [{"role": "user", "content": "hi"}]},
        "status": 200,
        "prompt_tokens": 1,
        "completion_tokens": 1,
        "total_tokens": 2,
        "duration_ms": 5,
    }


def make(when: datetime, retention_days=None, store=None):
    clock = Clock(when)
    store = store if store is not None else Store()
    log = AuditLog(store, ROOT, now=clock)
    merger = AuditMerger(store, ROOT, log, now=clock, retention_days=retention_days)
    log.flushed_listeners.append(merger.flushed)
    return clock, store, log, merger


def write(log: AuditLog, *records: dict) -> str:
    """Flush `records` as one segment and return its name."""
    names = []
    log.flushed_listeners.append(lambda day, seg, rows: names.append(seg))
    for record in records:
        log.add(record)
    assert log.flush()
    log.flushed_listeners.pop()
    return names[0]


def files(store, day=DAY):
    """Return the metadata files of `day`. The prompt marker is left out."""
    return [path for path in store.ls(f"{audit_dir(ROOT)}{day}") if path.endswith(".jsonl")]


def lines(store, path):
    return parse_lines(store.get(path).decode())


def test_a_bucket_merges_only_one_minute_after_its_end():
    clock, store, log, merger = make(at(11, 14, 3, 59))
    first = write(log, rec("alice", "2026-11-11T14:03:00.000Z"), rec("bob", "2026-11-11T14:03:30.000Z"))
    clock.when = at(11, 14, 10, 30)
    later = write(log, rec("carol", "2026-11-11T14:10:30.000Z"))
    clock.when = at(11, 14, 10, 59, 999999)
    merger.merge()
    assert files(store) == [segment_path(ROOT, DAY, first), segment_path(ROOT, DAY, later)]
    clock.when = at(11, 14, 11)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140"), segment_path(ROOT, DAY, later)]
    assert [row["user"] for row in lines(store, part_path(ROOT, DAY, "20261111T140"))] == ["alice", "bob"]


def test_rows_keep_their_seg_and_their_order():
    clock, store, log, merger = make(at(11, 14, 0, 1))
    expected = []
    for minute, users in ((0, ["alice", "bob"]), (3, ["carol"]), (7, ["bob", "alice"])):
        clock.when = at(11, 14, minute, 1)
        records = [rec(user, f"2026-11-11T14:0{minute}:00.000Z") for user in users]
        name = write(log, *records)
        expected.extend(lines(store, segment_path(ROOT, DAY, name)))
    clock.when = at(11, 14, 11)
    merger.merge()
    merged = lines(store, part_path(ROOT, DAY, "20261111T140"))
    assert merged == expected
    assert [row["user"] for row in merged] == ["alice", "bob", "carol", "bob", "alice"]
    assert len({row["seg"] for row in merged}) == 3


def test_the_cache_holds_the_merged_file_and_not_the_inputs():
    clock, store, log, merger = make(at(11, 23, 55))
    name = write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 1)
    merger.merge()
    part = part_path(ROOT, DAY, "20261111T235")
    assert log.cached_rows(segment_path(ROOT, DAY, name)) is None
    assert [row["user"] for row in log.cached_rows(part)] == ["alice"]
    clock.when = at(12, 0, 15)
    merger.merge()
    assert log.cached_rows(part) is None
    assert [row["user"] for row in log.cached_rows(day_path(ROOT, DAY))] == ["alice"]


def test_a_day_merges_only_fifteen_minutes_after_its_end():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 1)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    clock.when = at(12, 0, 14, 59, 999999)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice"]


def test_a_day_merges_only_when_no_segment_is_left():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 14, 30)
    late = write(log, rec("bob", "2026-11-11T23:59:59.000Z"))  # bucket 20261112T001
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235"), segment_path(ROOT, DAY, late)]
    clock.when = at(12, 0, 21)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob"]


def test_a_late_record_in_a_finished_day_ends_in_the_day_file():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"), rec("bob", "2026-11-11T23:56:00.000Z"))
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    clock.when = at(12, 0, 16)
    write(log, rec("carol", "2026-11-11T23:59:00.000Z"))
    merger.merge()
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob"]
    clock.when = at(12, 0, 21)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob", "carol"]
    assert [row["user"] for row in log.cached_rows(day_path(ROOT, DAY))] == ["alice", "bob", "carol"]
    fresh = AuditLog(store, ROOT)
    assert [row["user"] for row in fresh.rows_for_day(DAY)] == ["alice", "bob", "carol"]


def test_a_failed_merged_write_leaves_the_inputs_and_the_next_tick_merges(caplog):
    clock, store, log, merger = make(at(11, 14, 3))
    name = write(log, rec("alice", "2026-11-11T14:03:00.000Z"))
    store.fail_put = lambda path: "/parts/" in path
    clock.when = at(11, 14, 11)
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        merger.merge()
    assert files(store) == [segment_path(ROOT, DAY, name)]
    assert "failed" in caplog.text
    store.fail_put = lambda path: False
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140")]


def test_a_failed_day_write_leaves_the_parts_and_the_next_tick_merges():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    store.fail_put = lambda path: path.endswith("/day.jsonl")
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    store.fail_put = lambda path: False
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]


def test_a_failed_delete_leaves_a_duplicate_that_counts_once_and_the_next_tick_removes():
    clock, store, log, merger = make(at(11, 14, 1))
    first = write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 2)
    write(log, rec("bob", "2026-11-11T14:02:00.000Z"))
    store.fail_delete = {segment_path(ROOT, DAY, first)}
    clock.when = at(11, 14, 11)
    merger.merge()
    part = part_path(ROOT, DAY, "20261111T140")
    assert files(store) == [part, segment_path(ROOT, DAY, first)]
    assert [row["user"] for row in log.rows_for_day(DAY)] == ["alice", "bob"]
    assert [row["user"] for row in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice", "bob"]
    store.fail_delete = set()
    merger.merge()
    assert files(store) == [part]
    assert [row["user"] for row in lines(store, part)] == ["alice", "bob"]


def test_a_segment_next_to_an_existing_part_adds_its_rows_after_the_part_rows():
    clock, store, log, merger = make(at(11, 14, 1))
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 11)
    merger.merge()
    row = dict(lines(store, part_path(ROOT, DAY, "20261111T140"))[0], user="bob", seg="20261111T140500.000000-ff")
    store.put(segment_path(ROOT, DAY, row["seg"]), dump_lines([row]))
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140")]
    assert [r["user"] for r in lines(store, part_path(ROOT, DAY, "20261111T140"))] == ["alice", "bob"]


def test_old_layout_files_are_never_merged_changed_or_deleted():
    clock, store, log, merger = make(at(11, 23, 55))
    old = f"{audit_dir(ROOT)}2026-11-01/20261101T120000.jsonl"
    body = dump_lines([rec("alice", "2026-11-01T12:00:00.000Z")])
    store.put(old, body)
    write(log, rec("bob", "2026-11-01T12:00:00.000Z"))
    clock.when = at(13, 0, 0)
    merger.merge()
    assert files(store, "2026-11-01") == [old, day_path(ROOT, "2026-11-01")]
    assert store.get(old) == body.encode()
    assert [row["user"] for row in lines(store, day_path(ROOT, "2026-11-01"))] == ["bob"]


def test_merge_holds_the_flush_lock():
    entered = threading.Event()
    release = threading.Event()

    class Blocking(Store):
        def put(self, path, data):
            if "/parts/" in path:
                entered.set()
                assert release.wait(5)
            super().put(path, data)

    clock, store, log, merger = make(at(11, 14, 1), store=Blocking())
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 11)
    merging = threading.Thread(target=merger.merge)
    merging.start()
    assert entered.wait(5)
    log.add(rec("bob", "2026-11-11T14:11:00.000Z"))
    flushing = threading.Thread(target=log.flush)
    flushing.start()
    flushing.join(0.2)
    assert flushing.is_alive()
    release.set()
    merging.join(5)
    flushing.join(5)
    assert not flushing.is_alive()
    assert [row["user"] for row in log.rows_for_day(DAY)] == ["alice", "bob"]


def test_tick_never_raises(caplog):
    class Broken(Store):
        def ls(self, prefix):
            raise OSError("simulated listing failure")

    clock, store, log, merger = make(at(11, 14, 1), retention_days=1, store=Broken())
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        merger.tick()
    assert "simulated listing failure" in caplog.text


# Prompt retention


def put_day(store, day: str) -> str:
    """Write a day file and one prompt file for `day`. Return the prompt path."""
    store.put(day_path(ROOT, day), dump_lines([rec("alice", f"{day}T12:00:00.000Z")]))
    path = prompt_path(ROOT, day, "20261101T120000.000000-aa")
    store.put(path, "{}\n")
    return path


def test_retain_deletes_old_prompt_folders_and_keeps_newer_days_and_all_metadata():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    dropped = []
    log.prompt_cache_drop = dropped.append
    old = put_day(store, "2026-11-08")
    new = put_day(store, "2026-11-09")
    store.ls_calls.clear()
    merger.retain()
    assert not store.exists(old)
    assert store.exists(new)
    assert store.exists(day_path(ROOT, "2026-11-08"))
    assert store.exists(day_path(ROOT, "2026-11-09"))
    assert dropped == ["2026-11-08"]
    assert prompts_dir(ROOT, "2026-11-08") in store.ls_calls
    assert prompts_dir(ROOT, "2026-11-09") not in store.ls_calls
    assert all(not call.rstrip("/").endswith("/prompts") for call in store.ls_calls)


def test_retain_does_nothing_when_not_set():
    clock, store, log, merger = make(at(30, 0, 0))
    old = put_day(store, "2026-11-08")
    store.ls_calls.clear()
    merger.retain()
    merger.tick()
    assert store.exists(old)
    assert all("/prompts" not in call for call in store.ls_calls)


def test_retain_runs_at_most_once_an_hour_and_remembers_the_cleaned_days():
    clock, store, log, merger = make(at(10, 23, 30), retention_days=2)
    put_day(store, "2026-11-07")
    old = put_day(store, "2026-11-08")  # past the limit at 2026-11-11 00:00
    merger.retain()
    assert not store.exists(prompt_path(ROOT, "2026-11-07", "20261101T120000.000000-aa"))
    assert store.exists(old)
    clock.when = at(11, 0, 29)
    store.ls_calls.clear()
    merger.retain()
    assert store.exists(old)
    assert store.ls_calls == []
    clock.when = at(11, 0, 30)
    merger.retain()
    assert not store.exists(old)
    assert prompts_dir(ROOT, "2026-11-07") not in store.ls_calls
    clock.when = at(11, 1, 30)
    store.ls_calls.clear()
    merger.retain()
    assert store.ls_calls == [audit_dir(ROOT)]


def test_retain_tries_a_day_again_after_a_failed_delete():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    old = put_day(store, "2026-11-08")
    store.fail_delete = {old}
    merger.retain()
    assert store.exists(old)
    store.fail_delete = set()
    clock.when = clock.when + timedelta(hours=1)
    merger.retain()
    assert not store.exists(old)


def test_retain_keeps_a_day_exactly_at_the_limit_and_deletes_it_one_microsecond_later():
    clock, store, log, merger = make(at(11, 0, 0), retention_days=2)
    old = put_day(store, "2026-11-08")  # its limit is 2026-11-11 00:00
    merger.retain()
    assert store.exists(old)
    later = AuditMerger(store, ROOT, log, now=Clock(at(11, 0, 0, 0, 1)), retention_days=2)
    later.retain()
    assert not store.exists(old)


def prompt_listings(store) -> list[str]:
    return [call for call in store.ls_calls if call.startswith(f"{ROOT}/prompts/")]


def test_retain_cleans_at_most_twenty_days_in_one_call_newest_first():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    first = datetime(2026, 9, 1, tzinfo=timezone.utc)
    days = [(first + timedelta(days=n)).strftime("%Y-%m-%d") for n in range(50)]
    for day in days:
        put_day(store, day)
    store.ls_calls.clear()
    counts = []
    for _ in range(4):
        before = len(prompt_listings(store))
        merger.retain()
        counts.append(len(prompt_listings(store)) - before)
    assert counts == [20, 20, 10, 0]
    assert prompt_listings(store) == [prompts_dir(ROOT, day) for day in reversed(days)]
    assert store.ls(f"{ROOT}/prompts") == []


def test_retain_starts_no_new_check_before_the_hour():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    old = put_day(store, "2026-11-08")
    merger.retain()
    assert not store.exists(old)
    newer = put_day(store, "2026-11-07")  # a day that a new check would clean
    store.ls_calls.clear()
    clock.when = at(11, 1, 59, 59, 999999)
    merger.retain()
    assert store.ls_calls == []
    assert store.exists(newer)
    clock.when = at(11, 2, 0)
    merger.retain()
    assert not store.exists(newer)


def put_prompts(store, day: str, count: int) -> list[str]:
    """Write a day file and `count` prompt files for `day`. Return the prompt paths."""
    store.put(day_path(ROOT, day), dump_lines([rec("alice", f"{day}T12:00:00.000Z")]))
    paths = [prompt_path(ROOT, day, f"20261101T120000.{n:06d}-aa") for n in range(count)]
    for path in paths:
        store.put(path, "{}\n")
    return paths


def test_retain_tries_a_failed_delete_again_at_the_next_call_without_a_new_listing():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    old = put_day(store, "2026-11-08")
    store.fail_delete = {old}
    merger.retain()
    assert store.exists(old)
    assert "2026-11-08" not in merger._cleaned
    store.fail_delete = set()
    store.ls_calls.clear()
    clock.when = at(11, 1, 1)
    merger.retain()
    assert not store.exists(old)
    assert store.ls_calls == []
    assert "2026-11-08" in merger._cleaned


def test_retain_drops_a_day_after_three_failed_calls_and_the_next_hourly_check_cleans_it():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    stuck = put_day(store, "2026-11-08")
    older = put_prompts(store, "2026-11-07", 3)
    store.fail_delete = {stuck}
    for _ in range(3):
        merger.retain()
    assert store.deletes.count(stuck) == 3
    assert store.exists(stuck)
    assert "2026-11-08" not in merger._cleaned
    assert not any(store.exists(path) for path in older)  # the next day goes on
    assert "2026-11-07" in merger._cleaned
    store.fail_delete = set()
    store.ls_calls.clear()
    clock.when = at(11, 1, 59)
    merger.retain()
    assert store.exists(stuck)
    assert store.ls_calls == []
    clock.when = at(11, 2, 0)
    merger.retain()
    assert not store.exists(stuck)
    assert "2026-11-08" in merger._cleaned


def test_one_retain_call_makes_at_most_64_deletes_and_20_folder_listings():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    put_prompts(store, "2026-11-08", 100)
    store.deletes.clear()
    merger.retain()
    assert len(store.deletes) == 64
    first = datetime(2026, 9, 1, tzinfo=timezone.utc)
    for n in range(30):  # folders with a day file and no prompt file
        day = (first + timedelta(days=n)).strftime("%Y-%m-%d")
        store.put(day_path(ROOT, day), dump_lines([rec("alice", f"{day}T12:00:00.000Z")]))
    clock.when = at(11, 2, 0)
    merger.retain()  # it ends the 36 files that are left of 2026-11-08
    store.ls_calls.clear()
    store.deletes.clear()
    clock.when = at(11, 3, 0)
    merger.retain()
    assert len(prompt_listings(store)) == 20
    assert len(store.ls_calls) <= 21  # plus one listing of `audit/` for the check
    assert all(not call.rstrip("/").endswith("/prompts") for call in store.ls_calls)


def test_a_folder_of_200_files_is_cleaned_in_four_calls_with_one_listing():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    paths = put_prompts(store, "2026-11-08", 200)
    dropped = []
    log.prompt_cache_drop = dropped.append
    store.ls_calls.clear()
    left = []
    for _ in range(4):
        assert "2026-11-08" not in merger._cleaned
        merger.retain()
        left.append(sum(store.exists(path) for path in paths))
    assert left == [136, 72, 8, 0]
    assert prompt_listings(store) == [prompts_dir(ROOT, "2026-11-08")]
    assert "2026-11-08" in merger._cleaned
    assert dropped and dropped[-1] == "2026-11-08"
    assert len(store.deletes) == 200


def test_retain_logs_the_files_it_deleted(caplog):
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    put_prompts(store, "2026-11-08", 70)
    with caplog.at_level(logging.INFO, logger="llm_service.audit"):
        merger.retain()
    assert "retention deleted 64 prompt files of 2026-11-08, 6 left" in caplog.messages


# Cost of the merges


def day_folder(day: str) -> str:
    return f"{audit_dir(ROOT)}{day}/"


def test_the_first_merge_lists_audit_once_and_later_merges_list_only_folders_with_work():
    clock, store, log, merger = make(at(11, 14, 1))
    store.put(f"{audit_dir(ROOT)}2026-11-01/20261101T120000.jsonl", dump_lines([rec("alice", "2026-11-01T12:00:00.000Z")]))
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"), rec("bob", "2026-11-10T14:01:00.000Z"))
    merger = AuditMerger(store, ROOT, log, now=clock)  # a start after the flush
    store.ls_calls.clear()
    merger.merge()
    assert store.ls_calls == [audit_dir(ROOT), day_folder("2026-11-10"), day_folder(DAY)]
    store.ls_calls.clear()
    merger.merge()
    assert store.ls_calls == [day_folder("2026-11-10"), day_folder(DAY)]


def test_a_merged_day_leaves_the_set_and_a_flush_puts_it_back():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    store.ls_calls.clear()
    merger.merge()
    assert store.ls_calls == []
    clock.when = at(12, 0, 16)
    write(log, rec("bob", "2026-11-11T23:59:00.000Z"))
    clock.when = at(12, 0, 21)
    merger.merge()
    assert store.ls_calls == [day_folder(DAY)]
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob"]


def test_tick_merges_at_most_once_a_minute():
    clock, store, log, merger = make(at(11, 14, 1))
    runs = []
    merge = merger.merge
    merger.merge = lambda: runs.append(clock.when) or merge()
    merger.tick()
    clock.when = at(11, 14, 1, 59, 999999)
    merger.tick()
    assert len(runs) == 1
    clock.when = at(11, 14, 2)
    merger.tick()
    assert runs == [at(11, 14, 1), at(11, 14, 2)]


def test_a_segment_with_no_valid_bucket_warns_once(caplog):
    clock, store, log, merger = make(at(11, 14, 1))
    row = dict(rec("alice", "2026-11-11T14:01:00.000Z"), seg="bad-name")
    store.put(segment_path(ROOT, DAY, "bad-name"), dump_lines([row]))
    with caplog.at_level(logging.DEBUG, logger="llm_service.audit"):
        merger.merge()
        merger.merge()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "bad-name" in r.getMessage()]
    assert len(warnings) == 1


def test_a_failed_rewrite_of_a_part_keeps_the_part_and_the_late_segment():
    clock, store, log, merger = make(at(11, 14, 1))
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 11)
    merger.merge()
    part = part_path(ROOT, DAY, "20261111T140")
    row = dict(lines(store, part)[0], user="bob", seg="20261111T140500.000000-ff")
    late = segment_path(ROOT, DAY, row["seg"])
    store.put(late, dump_lines([row]))
    merger.flushed(DAY, row["seg"], [row])
    store.fail_put = lambda path: path == part
    merger.merge()
    assert files(store) == [part, late]
    assert [r["user"] for r in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice", "bob"]
    store.fail_put = lambda path: False
    merger.merge()
    assert files(store) == [part]
    assert [r["user"] for r in lines(store, part)] == ["alice", "bob"]


def test_a_day_rewrite_whose_part_delete_fails_counts_each_row_once():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"), rec("bob", "2026-11-11T23:56:00.000Z"))
    clock.when = at(12, 0, 15)
    merger.merge()
    clock.when = at(12, 0, 16)
    write(log, rec("carol", "2026-11-11T23:59:00.000Z"))
    part = part_path(ROOT, DAY, "20261112T001")
    store.fail_delete = {part}
    clock.when = at(12, 0, 21)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY), part]
    assert [r["user"] for r in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice", "bob", "carol"]
    store.fail_delete = set()
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [r["user"] for r in lines(store, day_path(ROOT, DAY))] == ["alice", "bob", "carol"]


# The flush lock during a merge


class LockProbe(Store):
    """A store that notes, at each put of a merged file and each delete, if a flush can run."""

    def __init__(self) -> None:
        super().__init__()
        self.log: AuditLog | None = None
        self.locked_at_put: list[bool] = []
        self.free_at_delete: list[bool] = []
        self.on_delete = lambda path: None

    def put(self, path, data):
        if "/parts/" in path or path.endswith("/day.jsonl"):
            self.locked_at_put.append(self.log.flush_lock.locked())
        super().put(path, data)

    def delete(self, path):
        free = self.log.flush_lock.acquire(timeout=1)
        if free:
            self.log.flush_lock.release()
        self.free_at_delete.append(free)
        if free:
            self.on_delete(path)
        super().delete(path)


def test_the_merge_holds_the_flush_lock_for_the_put_and_not_for_the_deletes():
    store = LockProbe()
    clock, store, log, merger = make(at(11, 23, 55), store=store)
    store.log = log
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(11, 23, 56)
    write(log, rec("bob", "2026-11-11T23:56:00.000Z"))
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY)]
    assert store.locked_at_put == [True, True]  # the part, then the day file
    assert store.free_at_delete == [True, True, True]  # two segments, then the part


def test_a_flush_during_the_delete_phase_keeps_the_day_in_the_work_set():
    store = LockProbe()
    clock, store, log, merger = make(at(11, 23, 55), store=store)
    store.log = log
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    part = part_path(ROOT, DAY, "20261111T235")
    late = []

    def flush_a_late_record(path):
        if path == part and not late:
            late.append(write(log, rec("carol", "2026-11-11T23:59:00.000Z")))

    store.on_delete = flush_a_late_record
    clock.when = at(12, 0, 15)
    merger.merge()
    assert files(store) == [day_path(ROOT, DAY), segment_path(ROOT, DAY, late[0])]
    store.ls_calls.clear()
    clock.when = at(12, 0, 21)
    merger.merge()
    assert day_folder(DAY) in store.ls_calls
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "carol"]


def test_the_merge_reads_its_inputs_in_parallel():
    store = Store()
    clock, store, log, merger = make(at(11, 14, 1), store=store)
    names = [write(log, rec("alice", "2026-11-11T14:01:00.000Z"))]
    clock.when = at(11, 14, 2)
    names.append(write(log, rec("bob", "2026-11-11T14:02:00.000Z")))
    barrier = threading.Barrier(2, timeout=5)
    inputs = {segment_path(ROOT, DAY, name) for name in names}
    get = store.get

    def paired_get(path):
        if path in inputs:
            barrier.wait()
        return get(path)

    store.get = paired_get
    fresh = AuditLog(store, ROOT, now=clock)  # an empty cache: the merge reads both segments
    merger = AuditMerger(store, ROOT, fresh, now=clock)
    clock.when = at(11, 14, 11)
    merger.merge()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140")]
    assert [row["user"] for row in lines(store, part_path(ROOT, DAY, "20261111T140"))] == ["alice", "bob"]


def test_each_merge_logs_one_line_with_its_cost(caplog):
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"), rec("bob", "2026-11-11T23:55:30.000Z"))
    clock.when = at(11, 23, 56)
    write(log, rec("carol", "2026-11-11T23:56:00.000Z"))
    clock.when = at(12, 0, 15)
    with caplog.at_level(logging.INFO, logger="llm_service.audit"):
        merger.merge()
    merged = [m for m in caplog.messages if m.startswith("merged ")]
    assert len(merged) == 2
    assert merged[0].startswith(f"merged 2 files of {DAY} into part bucket 20261111T235 in ")
    assert merged[0].endswith(" s (3 rows)")
    assert merged[1].startswith(f"merged 1 files of {DAY} into the day file in ")
    assert merged[1].endswith(" s (3 rows)")


# The maintenance task


class BlockedDelete(Store):
    """A store whose deletes of segments wait for `release`. `deleting` is set at the first one."""

    def __init__(self) -> None:
        super().__init__()
        self.deleting = threading.Event()
        self.release = threading.Event()

    def delete(self, path):
        if "/segments/" in path:
            self.deleting.set()
            assert self.release.wait(10)
        super().delete(path)


def test_a_flush_and_a_checkpoint_complete_while_a_merge_deletes_its_inputs():
    from llm_service.gateway.usage import UsageTotals

    store = BlockedDelete()
    clock, store, log, merger = make(at(11, 14, 1), store=store)
    usage = UsageTotals(store, ROOT, log, lambda: DAY, now=clock)
    log.flushed_listeners.append(usage.flushed)
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 11)
    merging = threading.Thread(target=merger.tick)
    merging.start()
    try:
        assert store.deleting.wait(5)
        log.add(rec("bob", "2026-11-11T14:11:00.000Z"))
        assert log.flush()
        usage.checkpoint(force=True)
        assert store.exists(f"{ROOT}/usage/{DAY}.partial.json")
        assert merging.is_alive()  # the merge still waits in its delete
    finally:
        store.release.set()
        merging.join(5)
    assert [row["user"] for row in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice", "bob"]


def test_the_maintenance_loop_survives_an_error_in_tick(caplog):
    calls = []

    def tick():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("simulated tick failure")

    async def scenario():
        task = asyncio.create_task(run_maintenance(tick, interval=0.01))
        for _ in range(500):
            if len(calls) >= 3:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        asyncio.run(scenario())
    assert len(calls) >= 3
    assert "the maintenance step merge failed: simulated tick failure" in caplog.messages


def test_a_cancelled_maintenance_loop_does_not_wait_for_a_running_tick():
    entered = threading.Event()
    release = threading.Event()

    def tick():
        entered.set()
        release.wait(10)

    async def scenario():
        task = asyncio.create_task(run_maintenance(tick, interval=0.01))
        assert await asyncio.to_thread(entered.wait, 5)
        started = time.monotonic()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < 1
        assert task.done()
        release.set()  # the worker thread ends by itself

    asyncio.run(scenario())


# Retention and late flushes


def test_a_late_flush_into_a_cleaned_day_makes_the_next_hourly_check_delete_its_prompts():
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2)
    put_day(store, "2026-11-08")
    merger.retain()
    assert "2026-11-08" in merger._cleaned
    write(log, rec("alice", "2026-11-08T13:00:00.000Z"))  # after a long outage
    (late,) = store.ls(prompts_dir(ROOT, "2026-11-08"))
    assert "2026-11-08" not in merger._cleaned
    clock.when = at(11, 2, 0)
    merger.retain()
    assert not store.exists(late)
    assert "2026-11-08" in merger._cleaned
    assert store.exists(day_path(ROOT, "2026-11-08"))


class PausedDelete(Store):
    """A store whose delete of `paused` waits for `release`. `deleting` is set at that delete."""

    def __init__(self) -> None:
        super().__init__()
        self.paused: str | None = None
        self.deleting = threading.Event()
        self.release = threading.Event()

    def delete(self, path):
        if path == self.paused:
            self.deleting.set()
            assert self.release.wait(5)
        super().delete(path)


def test_a_flush_during_a_retention_delete_keeps_the_day_from_being_cleaned():
    store = PausedDelete()
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2, store=store)
    day = "2026-11-08"
    store.paused = put_day(store, day)
    late = prompt_path(ROOT, day, "20261111T010000.000000-bb")
    retaining = threading.Thread(target=merger.retain)
    retaining.start()
    try:
        assert store.deleting.wait(5)
        store.put(late, "{}\n")  # a flush writes the prompt file, then calls the listeners
        with log.flush_lock:
            merger.flushed(day, "20261111T010000.000000-bb", [])
        store.fail_delete = {late}
    finally:
        store.release.set()
        retaining.join(5)
    assert not retaining.is_alive()
    assert not store.exists(store.paused)
    assert store.exists(late)
    assert day not in merger._cleaned
    assert prompt_listings(store).count(prompts_dir(ROOT, day)) == 2  # the day is listed again
    store.fail_delete = set()
    clock.when = at(11, 1, 1)
    merger.retain()
    assert not store.exists(late)
    assert day in merger._cleaned


def test_flushed_does_not_wait_for_a_retention_delete():
    store = PausedDelete()
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2, store=store)
    store.paused = put_day(store, "2026-11-08")
    retaining = threading.Thread(target=merger.retain)
    retaining.start()
    try:
        assert store.deleting.wait(5)

        def flush_listener():
            with log.flush_lock:
                merger.flushed("2026-11-08", "20261111T010000.000000-bb", [])

        flushing = threading.Thread(target=flush_listener)
        flushing.start()
        flushing.join(1)
        assert not flushing.is_alive()
        assert retaining.is_alive()  # the delete still waits
    finally:
        store.release.set()
        retaining.join(5)
    assert not retaining.is_alive()


# The prompt marker


def test_a_day_with_prompts_and_no_metadata_is_found_by_retention_after_a_restart():
    day = "2026-11-08"
    clock, store, log, merger = make(at(8, 12, 1), retention_days=2)
    store.fail_put = lambda path: "/segments/" in path
    log.add(rec("alice", f"{day}T12:00:00.000Z"))
    assert log.flush() is False  # the process then exits before the next flush
    (orphan,) = store.ls(prompts_dir(ROOT, day))
    marker = prompt_marker_path(ROOT, day)
    assert store.ls(f"{audit_dir(ROOT)}{day}") == [marker]
    store.fail_put = lambda path: False
    clock, store, log, merger = make(at(11, 1, 0), retention_days=2, store=store)  # a restart
    assert log.days() == [day]
    merger.tick()
    assert not store.exists(orphan)
    assert store.exists(marker)  # retention leaves the marker
    assert day in merger._cleaned
    assert log.rows_for_day(day) == []


class CountingPuts(Store):
    def __init__(self) -> None:
        super().__init__()
        self.puts: list[str] = []

    def put(self, path, data):
        self.puts.append(path)
        super().put(path, data)


def test_the_marker_of_a_day_is_written_one_time_in_a_process():
    clock, store, log, merger = make(at(11, 14, 1), store=CountingPuts())
    for minute in (1, 2, 3):
        clock.when = at(11, 14, minute)
        write(log, rec("alice", f"2026-11-11T14:0{minute}:00.000Z"), rec("bob", "2026-11-10T14:00:00.000Z"))
    assert store.puts.count(prompt_marker_path(ROOT, DAY)) == 1
    assert store.puts.count(prompt_marker_path(ROOT, "2026-11-10")) == 1
    assert store.puts[0] == prompt_marker_path(ROOT, "2026-11-10")  # before the first prompt file
    clock, store, log, merger = make(at(11, 14, 5), store=store)  # a restart
    log.days()  # the listing shows the markers
    write(log, rec("carol", "2026-11-11T14:05:00.000Z"))
    assert store.puts.count(prompt_marker_path(ROOT, DAY)) == 1


def test_a_merge_leaves_the_marker_in_place():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    marker = prompt_marker_path(ROOT, DAY)
    assert store.exists(marker)
    clock.when = at(12, 0, 1)
    merger.merge()
    clock.when = at(12, 0, 15)
    merger.merge()
    assert store.ls(f"{audit_dir(ROOT)}{DAY}") == [day_path(ROOT, DAY), marker]
    assert [row["user"] for row in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice"]


def test_the_marker_is_no_metadata_file():
    assert classify(ROOT, prompt_marker_path(ROOT, DAY)) is None
    assert prompt_marker_path(ROOT, DAY) == f"{ROOT}/audit/{DAY}/prompts.marker"


def test_a_day_folder_with_only_the_marker_reads_as_an_empty_day():
    from llm_service.gateway.usage import UsageTotals

    clock, store, log, merger = make(at(11, 14, 1))
    store.put(prompt_marker_path(ROOT, "2026-11-10"), "x\n")
    assert log.days() == ["2026-11-10"]
    assert log.rows_for_day("2026-11-10") == []
    assert log.query() == []
    assert log.users() == []
    usage = UsageTotals(store, ROOT, log, lambda: DAY, now=clock)
    assert usage.load() == 1
    assert usage.summary(None, None, None, None) == []
    merger.merge()
    assert store.ls(audit_dir(ROOT)) == [prompt_marker_path(ROOT, "2026-11-10")]
