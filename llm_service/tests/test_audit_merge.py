import logging
import threading
from datetime import datetime, timedelta, timezone

from llm_service.gateway.audit import AuditLog
from llm_service.gateway.audit_layout import (
    audit_dir,
    day_path,
    dump_lines,
    parse_lines,
    part_path,
    prompt_path,
    prompts_dir,
    segment_path,
)
from llm_service.gateway.audit_merge import AuditMerger
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
    """A memory store that records `ls` calls and fails chosen writes and deletes."""

    def __init__(self) -> None:
        super().__init__()
        self.ls_calls: list[str] = []
        self.fail_put = lambda path: False
        self.fail_delete: set[str] = set()

    def put(self, path, data):
        if self.fail_put(path):
            raise OSError("simulated write failure")
        super().put(path, data)

    def delete(self, path):
        if path in self.fail_delete:
            raise OSError("simulated delete failure")
        super().delete(path)

    def ls(self, prefix):
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
    return store.ls(f"{audit_dir(ROOT)}{day}")


def lines(store, path):
    return parse_lines(store.get(path).decode())


def test_a_bucket_merges_only_one_minute_after_its_end():
    clock, store, log, merger = make(at(11, 14, 3, 59))
    first = write(log, rec("alice", "2026-11-11T14:03:00.000Z"), rec("bob", "2026-11-11T14:03:30.000Z"))
    clock.when = at(11, 14, 10, 30)
    later = write(log, rec("carol", "2026-11-11T14:10:30.000Z"))
    clock.when = at(11, 14, 10, 59, 999999)
    merger.tick()
    assert files(store) == [segment_path(ROOT, DAY, first), segment_path(ROOT, DAY, later)]
    clock.when = at(11, 14, 11)
    merger.tick()
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
    merger.tick()
    merged = lines(store, part_path(ROOT, DAY, "20261111T140"))
    assert merged == expected
    assert [row["user"] for row in merged] == ["alice", "bob", "carol", "bob", "alice"]
    assert len({row["seg"] for row in merged}) == 3


def test_the_cache_holds_the_merged_file_and_not_the_inputs():
    clock, store, log, merger = make(at(11, 23, 55))
    name = write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 1)
    merger.tick()
    part = part_path(ROOT, DAY, "20261111T235")
    assert log.cached_rows(segment_path(ROOT, DAY, name)) is None
    assert [row["user"] for row in log.cached_rows(part)] == ["alice"]
    clock.when = at(12, 0, 15)
    merger.tick()
    assert log.cached_rows(part) is None
    assert [row["user"] for row in log.cached_rows(day_path(ROOT, DAY))] == ["alice"]


def test_a_day_merges_only_fifteen_minutes_after_its_end():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 1)
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    clock.when = at(12, 0, 14, 59, 999999)
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    clock.when = at(12, 0, 15)
    merger.tick()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice"]


def test_a_day_merges_only_when_no_segment_is_left():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    clock.when = at(12, 0, 14, 30)
    late = write(log, rec("bob", "2026-11-11T23:59:59.000Z"))  # bucket 20261112T001
    clock.when = at(12, 0, 15)
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235"), segment_path(ROOT, DAY, late)]
    clock.when = at(12, 0, 21)
    merger.tick()
    assert files(store) == [day_path(ROOT, DAY)]
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob"]


def test_a_late_record_in_a_finished_day_ends_in_the_day_file():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"), rec("bob", "2026-11-11T23:56:00.000Z"))
    clock.when = at(12, 0, 15)
    merger.tick()
    assert files(store) == [day_path(ROOT, DAY)]
    clock.when = at(12, 0, 16)
    write(log, rec("carol", "2026-11-11T23:59:00.000Z"))
    merger.tick()
    assert [row["user"] for row in lines(store, day_path(ROOT, DAY))] == ["alice", "bob"]
    clock.when = at(12, 0, 21)
    merger.tick()
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
        merger.tick()
    assert files(store) == [segment_path(ROOT, DAY, name)]
    assert "failed" in caplog.text
    store.fail_put = lambda path: False
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140")]


def test_a_failed_day_write_leaves_the_parts_and_the_next_tick_merges():
    clock, store, log, merger = make(at(11, 23, 55))
    write(log, rec("alice", "2026-11-11T23:55:00.000Z"))
    store.fail_put = lambda path: path.endswith("/day.jsonl")
    clock.when = at(12, 0, 15)
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T235")]
    store.fail_put = lambda path: False
    merger.tick()
    assert files(store) == [day_path(ROOT, DAY)]


def test_a_failed_delete_leaves_a_duplicate_that_counts_once_and_the_next_tick_removes():
    clock, store, log, merger = make(at(11, 14, 1))
    first = write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 2)
    write(log, rec("bob", "2026-11-11T14:02:00.000Z"))
    store.fail_delete = {segment_path(ROOT, DAY, first)}
    clock.when = at(11, 14, 11)
    merger.tick()
    part = part_path(ROOT, DAY, "20261111T140")
    assert files(store) == [part, segment_path(ROOT, DAY, first)]
    assert [row["user"] for row in log.rows_for_day(DAY)] == ["alice", "bob"]
    assert [row["user"] for row in AuditLog(store, ROOT).rows_for_day(DAY)] == ["alice", "bob"]
    store.fail_delete = set()
    merger.tick()
    assert files(store) == [part]
    assert [row["user"] for row in lines(store, part)] == ["alice", "bob"]


def test_a_segment_next_to_an_existing_part_adds_its_rows_after_the_part_rows():
    clock, store, log, merger = make(at(11, 14, 1))
    write(log, rec("alice", "2026-11-11T14:01:00.000Z"))
    clock.when = at(11, 14, 11)
    merger.tick()
    row = dict(lines(store, part_path(ROOT, DAY, "20261111T140"))[0], user="bob", seg="20261111T140500.000000-ff")
    store.put(segment_path(ROOT, DAY, row["seg"]), dump_lines([row]))
    merger.tick()
    assert files(store) == [part_path(ROOT, DAY, "20261111T140")]
    assert [r["user"] for r in lines(store, part_path(ROOT, DAY, "20261111T140"))] == ["alice", "bob"]


def test_old_layout_files_are_never_merged_changed_or_deleted():
    clock, store, log, merger = make(at(11, 23, 55))
    old = f"{audit_dir(ROOT)}2026-11-01/20261101T120000.jsonl"
    body = dump_lines([rec("alice", "2026-11-01T12:00:00.000Z")])
    store.put(old, body)
    write(log, rec("bob", "2026-11-01T12:00:00.000Z"))
    clock.when = at(13, 0, 0)
    merger.tick()
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
    clock, store, log, merger = make(at(11, 0, 0), retention_days=2)
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
    clock, store, log, merger = make(at(11, 0, 0), retention_days=2)
    old = put_day(store, "2026-11-08")
    store.fail_delete = {old}
    merger.retain()
    assert store.exists(old)
    store.fail_delete = set()
    clock.when = clock.when + timedelta(hours=1)
    merger.retain()
    assert not store.exists(old)
