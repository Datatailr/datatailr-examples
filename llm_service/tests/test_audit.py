import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone

import pytest

from llm_service.gateway.audit import (
    AuditLog,
    PromptUnreadable,
    iso,
    parse_time,
    run_flusher,
    utc_now,
)
from llm_service.gateway.audit_layout import (
    bucket_of,
    day_path,
    parse_lines,
    part_path,
    prompt_marker_path,
    prompt_path,
    segment_path,
)
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.tests.fakes import FlakyBlobStore

T = "2026-11-10T12:00:00.123Z"
DAY = "2026-11-10"
NOW = datetime(2026, 11, 10, 14, 3, 59, 123456, tzinfo=timezone.utc)
STAMP = "20261110T140359.123456"


def rec(user, ts, model="m", tokens=10):
    return {
        "ts": ts,
        "request_id": f"{user}-{ts}",
        "user": user,
        "endpoint": "/v1/chat/completions",
        "model": model,
        "stream": False,
        "prompt": {"messages": [{"role": "user", "content": "hi"}]},
        "status": 200,
        "prompt_tokens": tokens,
        "completion_tokens": tokens,
        "total_tokens": 2 * tokens,
        "duration_ms": 5,
    }


def event(user, ts, kind="model_switch"):
    record = rec(user, ts)
    record.update(
        request_id=f"event-{user}-{ts}",
        endpoint="/admin/model",
        prompt={"event": kind, "revision": "v2", "error": None},
        prompt_tokens=None,
        completion_tokens=None,
        total_tokens=None,
        duration_ms=None,
    )
    return record


def full(record):
    """Return the full record that `AuditLog.record` gives for an input record."""
    out = dict(record)
    prompt = record["prompt"]
    if isinstance(prompt, dict) and "event" in prompt:
        out["event"] = prompt["event"]
        out["detail"] = {k: v for k, v in prompt.items() if k != "event"}
    else:
        out["event"] = None
        out["detail"] = None
    return out


def make(store=None, flush_size=100, now=utc_now):
    store = store if store is not None else MemoryBlobStore()
    return store, AuditLog(store, "root", flush_size=flush_size, now=now)


def audit_files(store, prefix):
    """Return the metadata files under `prefix`. The prompt markers are left out."""
    return [path for path in store.ls(prefix) if path.endswith(".jsonl")]


def lines_of(store, path):
    return parse_lines(store.get(path).decode())


def test_flush_writes_one_segment_per_day():
    store, log = make()
    log.add(rec("alice", "2026-11-10T23:59:59.000Z"))
    log.add(rec("bob", "2026-11-11T00:00:01.000Z"))
    log.flush()
    paths = audit_files(store, "root/audit")
    assert [p.split("/")[2] for p in paths] == ["2026-11-10", "2026-11-11"]
    assert all(p.split("/")[3] == "segments" and p.endswith(".jsonl") for p in paths)
    assert lines_of(store, paths[0])[0]["user"] == "alice"
    assert len(store.ls("root/prompts")) == 2
    log.flush()
    assert len(audit_files(store, "root/audit")) == 2  # an empty buffer writes nothing


def test_add_never_writes_and_should_flush_at_size():
    store, log = make(flush_size=2)
    log.add(rec("alice", T))
    assert not log.should_flush()
    log.add(rec("bob", T))
    assert log.should_flush()
    assert audit_files(store, "root/audit") == []  # add does no storage I/O
    assert log.flush() is True
    assert len(audit_files(store, "root/audit")) == 1
    assert not log.should_flush()
    assert log.flush() is True  # the buffer is empty
    assert len(audit_files(store, "root/audit")) == 1


def test_flush_returns_false_when_records_are_kept():
    store = FlakyBlobStore()
    _, log = make(store)
    log.add(rec("alice", T))
    store.fail_puts = True
    assert log.flush() is False
    store.fail_puts = False
    assert log.flush() is True


def test_query_filters_and_order():
    store, log = make()
    all_ts = [
        "2026-11-10T08:00:00.000Z",
        "2026-11-10T09:00:00.000Z",
        "2026-11-11T08:00:00.000Z",
        "2026-11-11T09:00:00.000Z",
        "2026-11-11T10:00:00.000Z",
        "2026-11-11T11:00:00.000Z",
    ]
    users = ["alice", "bob", "alice", "bob", "alice", "bob"]
    for u, ts in zip(users[:3], all_ts[:3]):
        log.add(rec(u, ts))
    log.flush()  # the first half is on storage
    for u, ts in zip(users[3:], all_ts[3:]):
        log.add(rec(u, ts, model="other"))  # the second half is buffered
    assert [r["ts"] for r in log.query()] == sorted(all_ts, reverse=True)
    assert {r["user"] for r in log.query(user="alice")} == {"alice"}
    assert {r["model"] for r in log.query(model="other")} == {"other"}
    since = log.query(since=parse_time("2026-11-11"))
    assert [r["ts"] for r in since] == sorted(all_ts[2:], reverse=True)
    assert [r["ts"] for r in log.query(since=parse_time(all_ts[2]))] == [
        r["ts"] for r in since
    ]  # since is inclusive
    until = log.query(until=parse_time(all_ts[2]))
    assert [r["ts"] for r in until] == [all_ts[1], all_ts[0]]  # until is exclusive
    page = log.query(limit=1, offset=1)
    assert len(page) == 1 and page[0] == log.query()[1]


def test_query_reads_only_days_in_range():
    store, log = make()
    log.add(rec("alice", "2026-11-09T10:00:00.000Z"))
    log.add(rec("alice", "2026-11-11T10:00:00.000Z"))
    log.flush()
    log = AuditLog(store, "root")  # a fresh log has an empty cache
    reads = []
    original = store.get
    store.get = lambda p: (reads.append(p), original(p))[1]
    out = log.query(since=parse_time("2026-11-11"))
    assert len(out) == 1
    assert all("2026-11-11" in p for p in reads) and reads


def test_users_summary():
    store, log = make()
    log.add(rec("bob", "2026-11-10T10:00:00.000Z"))
    log.flush()
    log.add(rec("alice", "2026-11-11T10:00:00.000Z"))
    log.add(rec("alice", "2026-11-09T10:00:00.000Z"))
    assert log.users() == [
        {
            "user": "alice",
            "first_request": "2026-11-09T10:00:00.000Z",
            "last_request": "2026-11-11T10:00:00.000Z",
            "requests": 2,
        },
        {
            "user": "bob",
            "first_request": "2026-11-10T10:00:00.000Z",
            "last_request": "2026-11-10T10:00:00.000Z",
            "requests": 1,
        },
    ]


def test_days_include_buffer_only_days():
    store, log = make()
    log.add(rec("alice", "2026-11-10T10:00:00.000Z"))
    log.flush()
    log.add(rec("alice", "2026-11-12T10:00:00.000Z"))
    assert log.days() == ["2026-11-10", "2026-11-12"]


def test_rows_for_day():
    store, log = make()
    log.add(rec("alice", "2026-11-10T10:00:00.000Z"))
    log.flush()
    log.add(rec("bob", "2026-11-10T11:00:00.000Z"))
    log.add(rec("bob", "2026-11-11T11:00:00.000Z"))
    rows = log.rows_for_day("2026-11-10")
    assert [r["user"] for r in rows] == ["alice", "bob"]
    assert all("prompt" not in r for r in rows)
    assert rows[0]["seg"] is not None and rows[0]["prompt_ref"]["line"] == 0
    assert rows[1]["seg"] is None and rows[1]["prompt_ref"] is None  # buffered


def test_store_error_keeps_records(caplog):
    store = FlakyBlobStore()
    _, log = make(store)
    store.fail_puts = True
    log.add(rec("alice", T))
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        log.flush()
    assert any(r.name == "llm_service.audit" for r in caplog.records)
    assert log.query() != []  # still readable from the buffer
    store.fail_puts = False
    log.flush()
    assert len(audit_files(store, "root/audit")) == 1
    assert len(log.query()) == 1


def test_store_error_of_any_type_keeps_records():
    class Broken(MemoryBlobStore):
        def put(self, path, data):
            raise RuntimeError("boom")

    _, log = make(Broken())
    log.add(rec("alice", T))
    log.flush()
    assert len(log.query()) == 1


def test_listener_called_once_per_record():
    store, log = make(flush_size=2)
    seen = []
    log.listeners.append(seen.append)
    r1, r2, r3 = rec("a", T), rec("b", T), rec("c", T)
    for r in (r1, r2, r3):
        log.add(r)
    log.flush()
    assert seen == [r1, r2, r3]


def test_malformed_line_is_skipped(caplog):
    store, log = make()
    good = json.dumps(rec("alice", T))
    store.put("root/audit/2026-11-10/120000.000000-abc.jsonl", f"{good}\nnot json\n\n")
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        out = log.query()
    assert [r["user"] for r in out] == ["alice"]
    assert any("malformed" in r.getMessage() for r in caplog.records)


def test_parse_time_and_iso():
    assert parse_time("2026-11-10") == datetime(2026, 11, 10, tzinfo=timezone.utc)
    t = parse_time("2026-11-10T12:00:00.123Z")
    assert t == datetime(2026, 11, 10, 12, 0, 0, 123000, tzinfo=timezone.utc)
    assert iso(t) == "2026-11-10T12:00:00.123Z"
    assert parse_time("2026-11-10T14:00:00+02:00") == parse_time("2026-11-10T12:00:00Z")
    assert iso(utc_now()).endswith("Z")


def test_parse_time_rejects_garbage():
    with pytest.raises(ValueError):
        parse_time("yesterday")


def test_run_flusher_writes_and_flushes_on_cancel():
    store, log = make()

    async def scenario():
        task = asyncio.create_task(run_flusher(log, 0.01))
        log.add(rec("alice", T))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert len(audit_files(store, "root/audit")) == 1


def test_flusher_writes_a_full_buffer_within_a_tick():
    store, log = make(flush_size=2)

    async def scenario():
        task = asyncio.create_task(run_flusher(log, 3600, tick=0.01))
        log.add(rec("alice", T))
        await asyncio.sleep(0.1)
        assert audit_files(store, "root/audit") == []  # not full, interval not over
        log.add(rec("bob", T))
        await asyncio.sleep(0.2)
        assert len(audit_files(store, "root/audit")) == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_flusher_tries_once_per_interval_after_a_failure():
    attempts = []

    class Failing(MemoryBlobStore):
        def put(self, path, data):
            attempts.append(path)
            raise OSError("down")

    store, log = make(Failing(), flush_size=1)
    log.add(rec("alice", T))

    async def scenario():
        task = asyncio.create_task(run_flusher(log, 0.3, tick=0.01))
        await asyncio.sleep(0.1)  # the buffer is full, so the first attempt is quick
        assert len(attempts) == 1
        await asyncio.sleep(0.1)  # still full, but the flusher waits for the interval
        assert len(attempts) == 1
        await asyncio.sleep(0.4)  # the interval is over
        assert len(attempts) == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert len(attempts) == 3  # the last flush on cancel
    assert len(log.query()) == 1


def test_flusher_runs_on_tick_after_each_flush_and_survives_its_errors():
    store, log = make()
    calls = []

    def on_tick():
        calls.append(len(audit_files(store, "root/audit")))
        raise RuntimeError("boom")

    async def scenario():
        task = asyncio.create_task(run_flusher(log, 0.02, tick=0.01, on_tick=on_tick))
        log.add(rec("alice", T))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert len(calls) >= 3 and calls[-1] == 1  # kept running after the errors


@pytest.mark.parametrize("bad", [None, "yesterday", 12345])
def test_bad_ts_is_rejected_before_buffer_and_listeners(bad):
    store, log = make()
    seen = []
    log.listeners.append(seen.append)
    log.add(rec("alice", T))
    broken = rec("bob", T)
    if bad is None:
        del broken["ts"]
    else:
        broken["ts"] = bad
    with pytest.raises(ValueError):
        log.add(broken)
    assert len(seen) == 1
    log.flush()
    assert len(audit_files(store, "root/audit")) == 1
    assert [r["user"] for r in log.query()] == ["alice"]
    assert log.days() == ["2026-11-10"]


def test_parse_time_turns_an_overflow_into_value_error():
    with pytest.raises(ValueError):
        parse_time("0001-01-01T00:00:00+01:00")


class CountingReads(MemoryBlobStore):
    def __init__(self):
        super().__init__()
        self.reads: list[str] = []

    def get(self, path):
        self.reads.append(path)
        return super().get(path)


def test_query_without_since_stops_at_the_newest_days():
    store, log = make(CountingReads())
    for day in ("2026-11-07", "2026-11-08", "2026-11-09", "2026-11-10"):
        log.add(rec("alice", f"{day}T08:00:00.000Z"))
        log.add(rec("alice", f"{day}T09:00:00.000Z"))
    log.flush()
    log = AuditLog(store, "root")  # a fresh log has an empty cache
    store.reads.clear()
    out = log.query(limit=2)
    assert [r["ts"] for r in out] == ["2026-11-10T09:00:00.000Z", "2026-11-10T08:00:00.000Z"]
    assert all("2026-11-10" in p for p in store.reads) and store.reads
    store.reads.clear()
    out = log.query(limit=2, offset=1)
    assert [r["ts"] for r in out] == ["2026-11-10T08:00:00.000Z", "2026-11-09T09:00:00.000Z"]
    assert {p.split("/")[2] for p in store.reads} == {"2026-11-09"}  # 11-10 is cached
    store.reads.clear()
    assert len(log.query(user="nobody")) == 0
    assert {p.split("/")[2] for p in store.reads} == {"2026-11-07", "2026-11-08"}  # every day


def test_stored_records_need_a_valid_ts_and_a_string_user(caplog):
    store, log = make()
    good = rec("alice", T)
    bad = [
        {"user": "bob"},
        {"ts": "yesterday", "user": "bob"},
        {"ts": "0001-01-01T00:00:00+01:00", "user": "bob"},
        {"ts": 5, "user": "bob"},
        {"ts": T, "user": 7},
        {"ts": T},
        [1, 2],
    ]
    body = "".join(json.dumps(r) + "\n" for r in [good, *bad])
    store.put("root/audit/2026-11-10/120000.000000-abc.jsonl", body)
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        out = log.query()
    assert [r["user"] for r in out] == ["alice"]
    warnings = [r for r in caplog.records if r.name == "llm_service.audit"]
    assert len(warnings) == 1 and "7" in warnings[0].getMessage()


def test_usage_load_skips_bad_stored_records():
    from llm_service.gateway.usage import UsageTotals

    store, log = make()
    bad = [
        {"ts": T, "endpoint": "/v1/chat/completions"},
        {"ts": 5, "user": "x", "endpoint": "/v1/chat/completions"},
    ]
    body = "".join(json.dumps(r) + "\n" for r in [rec("alice", T), *bad])
    store.put("root/audit/2026-11-10/120000.000000-abc.jsonl", body)
    usage = UsageTotals(store, "root", log, lambda: "2026-11-10")
    usage.load()
    rows = usage.summary(None, None, None, None)
    assert [(r["user"], r["requests"]) for r in rows] == [("alice", 1)]


class BlockingStore(MemoryBlobStore):
    """A store whose `put` of a segment waits for `release`. It writes before or after the wait."""

    def __init__(self, write_first: bool):
        super().__init__()
        self.write_first = write_first
        self.entered = threading.Event()
        self.release = threading.Event()

    def put(self, path, data):
        if "/segments/" not in path:
            super().put(path, data)
            return
        if self.write_first:
            super().put(path, data)
        self.entered.set()
        assert self.release.wait(5)
        if not self.write_first:
            super().put(path, data)


@pytest.mark.parametrize("write_first", [False, True])
def test_query_during_a_flush_sees_each_record_once(write_first):
    store, log = make(BlockingStore(write_first))
    log.add(rec("alice", "2026-11-10T08:00:00.000Z"))
    log.add(rec("bob", "2026-11-10T09:00:00.000Z"))
    flusher = threading.Thread(target=log.flush)
    flusher.start()
    try:
        assert store.entered.wait(5)
        during = log.query()
        assert [r["user"] for r in during] == ["bob", "alice"]
        assert len(log.rows_for_day("2026-11-10")) == 2
    finally:
        store.release.set()
        flusher.join()
    assert [r["user"] for r in log.query()] == ["bob", "alice"]


def test_partial_flush_failure_keeps_only_the_failed_day():
    class FailOneDay(MemoryBlobStore):
        fail = True

        def put(self, path, data):
            if self.fail and "/2026-11-09/" in path:
                raise OSError("down")
            super().put(path, data)

    store, log = make(FailOneDay())
    log.add(rec("alice", "2026-11-09T08:00:00.000Z"))
    log.add(rec("bob", "2026-11-10T08:00:00.000Z"))
    assert log.flush() is False
    assert len(audit_files(store, "root/audit/2026-11-10")) == 1
    assert audit_files(store, "root/audit/2026-11-09") == []
    assert [r["user"] for r in log.query()] == ["bob", "alice"]
    store.fail = False
    assert log.flush() is True
    assert len(audit_files(store, "root/audit/2026-11-10")) == 1  # not written twice
    assert len(audit_files(store, "root/audit/2026-11-09")) == 1
    assert [r["user"] for r in log.query()] == ["bob", "alice"]


def test_query_puts_the_later_record_first_on_the_same_time():
    store, log = make()
    log.add(rec("alice", T))
    log.flush()
    log.add(rec("bob", T))
    log.add(rec("carol", T))
    assert [r["user"] for r in log.query()] == ["carol", "bob", "alice"]


def _stored(store) -> list[dict]:
    return [
        json.loads(line)
        for path in audit_files(store, "root/audit")
        for line in store.get(path).decode().splitlines()
    ]


def test_record_that_json_cannot_write_is_dropped_with_a_warning(caplog):
    store, log = make()
    bad = rec("bob", T)
    bad["prompt"] = {"input": object()}
    log.add(rec("alice", T))
    log.add(bad)
    log.add(rec("carol", "2026-11-11T08:00:00.000Z"))
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        assert log.flush() is True
    assert any(bad["request_id"] in m for m in caplog.messages)
    assert sorted(r["user"] for r in _stored(store)) == ["alice", "carol"]
    assert log._buffered() == []  # the bad record does not block later flushes
    log.add(rec("dave", T))
    assert log.flush() is True
    assert sorted(r["user"] for r in _stored(store)) == ["alice", "carol", "dave"]


def test_unexpected_error_during_flush_keeps_all_records(caplog):
    calls = []

    def now():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("clock broke")
        return utc_now()

    store = MemoryBlobStore()
    log = AuditLog(store, "root", now=now)
    log.add(rec("alice", T))
    log.add(rec("bob", T))
    with caplog.at_level(logging.WARNING, logger="llm_service.audit"):
        assert log.flush() is False
    assert any("clock broke" in m for m in caplog.messages)
    assert log._inflight == []
    assert [r["user"] for r in log._buffered()] == ["alice", "bob"]
    assert audit_files(store, "root/audit") == []
    assert log.flush() is True
    assert sorted(r["user"] for r in _stored(store)) == ["alice", "bob"]
    assert log._buffered() == []


class CountingStore(MemoryBlobStore):
    """A memory store that counts the `get` calls for each path and can fail some."""

    def __init__(self) -> None:
        super().__init__()
        self.gets: list[str] = []
        self.fail_paths: set[str] = set()
        self._count_lock = threading.Lock()

    def get(self, path: str) -> bytes:
        with self._count_lock:
            self.gets.append(path)
        if path in self.fail_paths:
            raise OSError("simulated read failure")
        return super().get(path)


def put_file(store, name, records, day="2026-11-10"):
    path = f"root/audit/{day}/{name}.jsonl"
    store.put(path, "".join(json.dumps(r) + "\n" for r in records))
    return path


def test_second_query_reads_no_file_again():
    store = CountingStore()
    for i in range(5):
        put_file(store, f"120000.00000{i}-a", [rec(f"u{i}", T)])
    log = AuditLog(store, "root")
    first = log.query()
    assert len(store.gets) == 5
    assert log.query() == first
    assert len(store.gets) == 5


def test_new_file_appears_and_only_it_is_read():
    store = CountingStore()
    put_file(store, "120000.000000-a", [rec("alice", "2026-11-10T12:00:00.000Z")])
    log = AuditLog(store, "root")
    assert len(log.query()) == 1
    new_path = put_file(store, "120001.000000-b", [rec("bob", "2026-11-10T12:00:01.000Z")])
    store.gets.clear()
    assert [r["user"] for r in log.query()] == ["bob", "alice"]
    assert store.gets == [new_path]


def test_failed_read_is_tried_again():
    store = CountingStore()
    good = put_file(store, "120000.000000-a", [rec("alice", T)])
    bad = put_file(store, "120001.000000-b", [rec("bob", T)])
    store.fail_paths.add(bad)
    log = AuditLog(store, "root")
    with pytest.raises(OSError):
        log.query()
    store.fail_paths.clear()
    store.gets.clear()
    assert {r["user"] for r in log.query()} == {"alice", "bob"}
    assert store.gets == [bad]  # the good file was cached by the first query
    assert good not in store.gets


def test_order_with_many_files_matches_a_fresh_log():
    store = CountingStore()
    for i in range(30):
        records = [rec(f"u{i}-{n}", f"2026-11-10T12:00:{i:02d}.000Z") for n in range(2)]
        put_file(store, f"1200{i:02d}.000000-a", records)
    same = [rec("same-a", "2026-11-10T13:00:00.000Z"), rec("same-b", "2026-11-10T13:00:00.000Z")]
    put_file(store, "130000.000000-z", same[:1])
    put_file(store, "130000.000001-z", same[1:])
    log = AuditLog(store, "root")
    cold = log.rows_for_day("2026-11-10")
    assert len(cold) == 62
    warm = log.rows_for_day("2026-11-10")
    fresh = AuditLog(store, "root").rows_for_day("2026-11-10")
    assert cold == warm == fresh
    assert [r["user"] for r in cold[-2:]] == ["same-a", "same-b"]
    assert log.query(limit=1000) == AuditLog(store, "root").query(limit=1000)
    assert [r["user"] for r in log.query(limit=2)] == ["same-b", "same-a"]


def test_mutating_a_returned_record_does_not_change_a_later_result():
    store = CountingStore()
    put_file(store, "120000.000000-a", [rec("alice", T), event("bob", T)])
    log = AuditLog(store, "root")
    first = log.query()
    first[0]["user"] = "mallory"
    first[0]["detail"]["revision"] = "x"
    again = log.query()
    assert again[0]["user"] == "bob" and again[0]["detail"]["revision"] == "v2"
    one = log.record(DAY, rec("alice", T)["request_id"])
    one["user"] = "mallory"
    one["prompt"]["messages"].append("x")
    again = log.record(DAY, rec("alice", T)["request_id"])
    assert again == full(rec("alice", T))
    assert [u["user"] for u in log.users()] == ["alice", "bob"]


def test_cache_evicts_the_least_recently_used_file():
    store = CountingStore()
    names = ("alice", "bob", "carol")
    paths = [put_file(store, f"12000{i}.000000-a", [rec(names[i], T)]) for i in range(3)]
    log = AuditLog(store, "root", row_cache_size=2)
    log.rows_for_day("2026-11-10")  # reads 3 files, keeps 2
    (evicted,) = [p for p in paths if log.cached_rows(p) is None]
    store.gets.clear()
    log.rows_for_day("2026-11-10")
    assert evicted in store.gets  # the evicted file is read again
    assert sum(log.cached_rows(p) is not None for p in paths) == 2


def test_users_and_rows_for_day_use_the_cache():
    store = CountingStore()
    for i in range(3):
        put_file(store, f"12000{i}.000000-a", [rec("alice", T)])
    log = AuditLog(store, "root")
    assert log.users()[0]["requests"] == 3
    assert len(store.gets) == 3
    assert len(log.rows_for_day("2026-11-10")) == 3
    assert log.users()[0]["requests"] == 3
    assert len(store.gets) == 3


def test_buffered_records_are_not_cached_and_still_appear():
    store = CountingStore()
    log = AuditLog(store, "root")
    seen = []
    log.flushed_listeners.append(lambda day, seg, rows: seen.append(seg))
    log.add(rec("alice", T))
    assert len(log.query()) == 1
    log.flush()
    path = segment_path("root", DAY, seen[0])
    assert log.cached_rows(path) == log.rows_for_day(DAY)
    assert len(log.query()) == 1
    assert store.gets == []


# New layout: metadata segments and prompt files


class RecordingStore(MemoryBlobStore):
    """A memory store that records its calls and fails the paths that hold a chosen text."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []
        self.fail_puts: set[str] = set()
        self.fail_gets: set[str] = set()
        self._calls_lock = threading.Lock()

    def _note(self, op: str, path: str) -> None:
        with self._calls_lock:
            self.calls.append((op, path))

    def put(self, path, data):
        self._note("put", path)
        if any(text in path for text in self.fail_puts):
            raise OSError("simulated write failure")
        super().put(path, data)

    def get(self, path):
        self._note("get", path)
        if any(text in path for text in self.fail_gets):
            raise OSError("simulated read failure")
        return super().get(path)

    def ls(self, prefix):
        self._note("ls", prefix)
        return super().ls(prefix)

    def ops(self, op: str) -> list[str]:
        with self._calls_lock:
            return [path for name, path in self.calls if name == op]

    def clear(self) -> None:
        with self._calls_lock:
            self.calls.clear()


def flushed(log):
    """Collect the `(day, seg, rows)` calls of the flushed listeners."""
    calls = []
    log.flushed_listeners.append(lambda day, seg, rows: calls.append((day, seg, rows)))
    return calls


def test_flush_writes_the_prompt_file_before_the_segment():
    store, log = make(RecordingStore(), now=lambda: NOW)
    calls = flushed(log)
    alice, switch = rec("alice", T), event("bob", T)
    log.add(alice)
    log.add(switch)
    assert log.flush() is True
    seg = calls[0][1]
    assert seg.startswith(STAMP + "-") and len(seg) > len(STAMP) + 1
    assert store.ops("put") == [
        prompt_marker_path("root", DAY),
        prompt_path("root", DAY, seg),
        segment_path("root", DAY, seg),
    ]
    rows = lines_of(store, segment_path("root", DAY, seg))
    assert [r["request_id"] for r in rows] == [alice["request_id"], switch["request_id"]]
    assert all("prompt" not in r and r["seg"] == seg for r in rows)
    assert rows[0]["prompt_ref"] == {"file": seg, "line": 0}
    assert rows[0]["event"] is None and rows[0]["detail"] is None
    assert rows[1]["event"] == "model_switch" and rows[1]["prompt_ref"] is None
    assert rows[1]["detail"] == {"revision": "v2", "error": None}
    prompts = lines_of(store, prompt_path("root", DAY, seg))
    assert prompts == [{"request_id": alice["request_id"], "prompt": alice["prompt"]}]


def test_flush_with_only_events_writes_no_prompt_file():
    store, log = make(RecordingStore())
    log.add(event("alice", T))
    assert log.flush() is True
    assert len(store.ops("put")) == 1
    assert store.ls("root/prompts") == []
    assert len(audit_files(store, "root/audit")) == 1


def test_failed_prompt_write_keeps_the_records_and_writes_no_segment():
    store, log = make(RecordingStore(), now=lambda: NOW)
    calls = flushed(log)
    log.add(rec("alice", T))
    store.fail_puts = {"/prompts/"}
    assert log.flush() is False
    marker, first = store.ops("put")
    assert marker == prompt_marker_path("root", DAY) and "/prompts/" in first
    assert audit_files(store, "root/audit") == [] and calls == []
    assert [r["user"] for r in log.query()] == ["alice"]
    store.fail_puts = set()
    store.clear()
    assert log.flush() is True
    seg = calls[0][1]
    assert prompt_path("root", DAY, seg) != first  # a new segment name
    assert store.ops("put") == [prompt_path("root", DAY, seg), segment_path("root", DAY, seg)]
    assert [r["user"] for r in log.query()] == ["alice"]


def test_failed_segment_write_keeps_the_records_and_leaves_an_orphan_prompt_file():
    store, log = make(RecordingStore(), now=lambda: NOW)
    calls = flushed(log)
    alice = rec("alice", T)
    log.add(alice)
    store.fail_puts = {"/segments/"}
    assert log.flush() is False
    assert len(store.ls("root/prompts")) == 1 and audit_files(store, "root/audit") == []
    assert [r["user"] for r in log.query()] == ["alice"]  # once, from the buffer
    store.fail_puts = set()
    assert log.flush() is True
    assert len(store.ls("root/prompts")) == 2  # the orphan stays
    assert len(audit_files(store, "root/audit")) == 1
    assert [r["user"] for r in log.query()] == ["alice"]
    fresh = AuditLog(store, "root")
    assert fresh.record(DAY, alice["request_id"]) == full(alice)
    assert fresh.rows_for_day(DAY)[0]["seg"] == calls[0][1]


def test_flush_puts_the_rows_in_the_cache():
    store, log = make(RecordingStore())
    log.add(rec("alice", T))
    log.add(event("bob", "2026-11-11T08:00:00.000Z"))
    log.flush()
    store.clear()
    assert len(log.query()) == 2
    assert len(log.rows_for_day(DAY)) == 1
    assert [u["user"] for u in log.users()] == ["alice", "bob"]
    assert store.ops("get") == []


def test_flushed_listeners_get_the_day_the_segment_and_the_rows():
    store, log = make(now=lambda: NOW)
    calls = flushed(log)

    def broken(day, seg, rows):
        raise RuntimeError("boom")

    log.flushed_listeners.insert(0, broken)
    log.add(rec("alice", T))
    log.add(rec("bob", "2026-11-11T08:00:00.000Z"))
    assert log.flush() is True  # a failing listener keeps no record
    assert [(day, seg[: len(STAMP)]) for day, seg, _ in calls] == [
        ("2026-11-10", STAMP),
        ("2026-11-11", STAMP),
    ]
    for day, seg, rows in calls:
        assert rows == lines_of(store, segment_path("root", day, seg))
    assert calls[0][1] != calls[1][1]
    assert log.flush() is True and len(calls) == 2  # an empty buffer calls nobody
    assert len(audit_files(store, "root/audit")) == 2


def test_a_row_counts_once_when_a_merged_file_and_its_inputs_exist():
    store, writer = make(now=lambda: NOW)
    calls = flushed(writer)
    writer.add(rec("alice", "2026-11-10T14:00:00.000Z"))
    writer.add(rec("bob", "2026-11-10T14:00:01.000Z"))  # same segment, same seg
    writer.flush()
    writer.add(rec("carol", "2026-11-10T14:05:00.000Z"))
    writer.flush()
    first, second = calls[0][1], calls[1][1]
    body = store.get(segment_path("root", DAY, first))
    store.put(part_path("root", DAY, bucket_of(first)), body)
    store.put(day_path("root", DAY), body)
    rows = AuditLog(store, "root").rows_for_day(DAY)
    assert [(r["user"], r["seg"]) for r in rows] == [
        ("alice", first),
        ("bob", first),
        ("carol", second),
    ]


def test_old_layout_file_with_a_request_and_an_event():
    store = CountingStore()
    alice, switch = rec("alice", T), event("bob", T)
    name = "120000.000000-abc"
    path = f"root/audit/{DAY}/{name}.jsonl"
    store.put(path, "not json\n" + json.dumps(alice) + "\n" + json.dumps(switch) + "\n")
    log = AuditLog(store, "root")
    rows = log.rows_for_day(DAY)
    assert [r["seg"] for r in rows] == [name, name]
    assert rows[0]["prompt_ref"] == {"file": name, "line": 1, "legacy": True}
    assert rows[0]["event"] is None and "prompt" not in rows[0]
    assert rows[1]["event"] == "model_switch" and rows[1]["prompt_ref"] is None
    assert rows[1]["detail"] == {"revision": "v2", "error": None}
    assert [r["user"] for r in log.query()] == ["bob", "alice"]
    assert "prompt" not in log.query()[0]
    store.gets.clear()
    assert log.record(DAY, switch["request_id"]) == full(switch)
    assert store.gets == []  # an event reads no file
    assert log.record(DAY, alice["request_id"]) == full(alice)
    assert store.gets == [path]


def test_rows_after_a_segment_read_only_the_files_that_can_hold_later_rows():
    times = iter(
        [
            datetime(2026, 11, 10, 14, 3, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 10, 14, 25, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 10, 14, 41, 0, tzinfo=timezone.utc),
        ]
    )
    store = RecordingStore()
    writer = AuditLog(store, "root", now=lambda: next(times))
    calls = flushed(writer)
    for user, ts in (
        ("alice", "2026-11-10T14:03:00.000Z"),
        ("bob", "2026-11-10T14:25:00.000Z"),
        ("carol", "2026-11-10T14:41:00.000Z"),
    ):
        writer.add(rec(user, ts))
        writer.flush()
    s1, s2, s3 = (seg for _, seg, _ in calls)
    store.put(part_path("root", DAY, bucket_of(s1)), store.get(segment_path("root", DAY, s1)))
    store.delete(segment_path("root", DAY, s1))
    store.put(day_path("root", DAY), store.get(part_path("root", DAY, bucket_of(s1))))
    put_file(store, "120000.000000-old", [rec("alice", "2026-11-10T12:00:00.000Z")])
    reader = AuditLog(store, "root")
    reader.add(rec("bob", "2026-11-10T15:00:00.000Z"))  # buffered rows are newer
    store.clear()
    rows = reader.rows_for_day(DAY, after=s2)
    assert [(r["user"], r["seg"]) for r in rows] == [("carol", s3), ("bob", None)]
    assert sorted(store.ops("get")) == sorted([day_path("root", DAY), segment_path("root", DAY, s3)])
    assert [r["user"] for r in reader.rows_for_day(DAY)] == ["alice", "bob", "carol", "alice", "bob"]
    assert [r["user"] for r in reader.rows_for_day(DAY, after=s1)] == ["bob", "carol", "bob"]


def test_lists_read_no_prompt_file():
    store = RecordingStore()
    _, writer = make(store)
    for user in ("alice", "bob"):
        writer.add(rec(user, T))
    writer.add(event("carol", T))
    writer.flush()
    log = AuditLog(store, "root")
    store.clear()
    assert len(log.query()) == 3
    assert len(log.users()) == 3
    assert len(log.rows_for_day(DAY)) == 3
    assert store.ops("get") and not any("/prompts/" in p for p in store.ops("get"))


def test_record_of_a_flushed_request_reads_its_prompt_file_once():
    store, writer = make(RecordingStore())
    alice = rec("alice", T)
    writer.add(alice)
    writer.add(rec("bob", T))
    writer.flush()
    assert writer.record(DAY, alice["request_id"]) == full(alice)
    log = AuditLog(store, "root")
    store.clear()
    assert log.record(DAY, alice["request_id"]) == full(alice)
    prompt_reads = [p for p in store.ops("get") if "/prompts/" in p]
    assert len(prompt_reads) == 1
    assert log.record(DAY, rec("bob", T)["request_id"]) == full(rec("bob", T))
    assert [p for p in store.ops("get") if "/prompts/" in p] == prompt_reads  # same file, cached


def test_record_of_a_buffered_request_and_an_event_reads_no_prompt():
    store, log = make(RecordingStore())
    alice, switch = rec("alice", T), event("bob", T)
    log.add(switch)
    log.flush()
    log.add(alice)
    store.clear()
    assert log.record(DAY, alice["request_id"]) == full(alice)
    assert log.record(DAY, switch["request_id"]) == full(switch)
    assert store.ops("get") == []


def test_record_of_a_deleted_prompt_file():
    store, log = make()
    alice = rec("alice", T)
    log.add(alice)
    log.flush()
    for path in store.ls("root/prompts"):
        store.delete(path)
    out = AuditLog(store, "root").record(DAY, alice["request_id"])
    assert out == {**full(alice), "prompt": None, "prompt_deleted": True}


def test_record_with_an_unreadable_prompt_raises():
    store, log = make(RecordingStore())
    alice = rec("alice", T)
    log.add(alice)
    log.flush()
    store.fail_gets = {"/prompts/"}
    with pytest.raises(PromptUnreadable):
        AuditLog(store, "root").record(DAY, alice["request_id"])
    store.fail_gets = set()
    (path,) = store.ls("root/prompts")
    for body in (
        json.dumps({"request_id": "someone-else", "prompt": {}}) + "\n",
        "not json\n",
        "",
    ):
        store.put(path, body)
        with pytest.raises(PromptUnreadable):
            AuditLog(store, "root").record(DAY, alice["request_id"])


def test_record_of_an_unknown_id_is_none():
    store, log = make()
    log.add(rec("alice", T))
    assert log.record(DAY, "nobody") is None
    log.flush()
    assert log.record(DAY, "nobody") is None
    assert log.record("2026-11-09", rec("alice", T)["request_id"]) is None


def test_record_just_before_midnight_goes_to_its_day_with_the_stamp_of_the_flush():
    later = datetime(2026, 11, 11, 0, 0, 3, tzinfo=timezone.utc)
    store, log = make(now=lambda: later)
    calls = flushed(log)
    late = rec("alice", "2026-11-10T23:59:59.900Z")
    log.add(late)
    log.flush()
    day, seg, _ = calls[0]
    assert day == DAY and seg.startswith("20261111T000003.000000-")
    assert store.exists(segment_path("root", DAY, seg))
    assert store.exists(prompt_path("root", DAY, seg))
    assert AuditLog(store, "root").record(DAY, late["request_id"]) == full(late)


def test_large_prompt_and_line_separators_survive_a_round_trip():
    store, log = make()
    big = rec("alice", T)
    big["prompt"] = {"messages": [{"role": "user", "content": "x" * (3 * 1024 * 1024)}]}
    odd = rec("bob", T)
    odd["prompt"] = {"messages": [{"role": "user", "content": "a b c\x85d\re\nf"}]}
    log.add(big)
    log.add(odd)
    log.flush()
    (path,) = store.ls("root/prompts")
    assert store.get(path).decode().count("\n") == 2
    fresh = AuditLog(store, "root")
    assert len(fresh.rows_for_day(DAY)) == 2
    assert fresh.record(DAY, big["request_id"]) == full(big)
    assert fresh.record(DAY, odd["request_id"]) == full(odd)


def test_row_cache_counts_rows():
    store = CountingStore()
    a = put_file(store, "120000.000000-a", [rec("alice", T), rec("bob", T)])
    b = put_file(store, "120001.000000-b", [rec("carol", T), rec("alice", T)])
    c = put_file(store, "120002.000000-c", [rec("bob", T)])
    big = put_file(store, "120003.000000-d", [rec("carol", T)] * 5)
    log = AuditLog(store, "root", row_cache_size=4)
    assert len(log.read_rows(a)) == 2 and len(log.read_rows(b)) == 2
    assert log.cached_rows(a) is not None and log.cached_rows(b) is not None  # 4 rows fit
    assert len(log.read_rows(c)) == 1  # 5 rows: the least recently used file goes
    assert log.cached_rows(a) is None
    assert log.cached_rows(b) is not None and log.cached_rows(c) is not None
    assert len(log.read_rows(big)) == 5  # larger than the limit: returned, not kept
    assert log.cached_rows(big) is None
    assert log.cached_rows(b) is not None and log.cached_rows(c) is not None
    log.cache_put(c, [])  # a new entry for a path replaces the old one
    log.cache_put(a, log.rows_for_day(DAY)[:3])  # 3 + 0 + 2 = 5 rows: b goes
    assert log.cached_rows(b) is None and log.cached_rows(c) == []
    log.cache_drop([a, c])
    assert log.cached_rows(a) is None and log.cached_rows(c) is None
    assert log.read_rows("root/audit/2026-11-10/missing.jsonl") == []


class SlowSegmentStore(MemoryBlobStore):
    """A memory store that pauses after each segment write."""

    def put(self, path, data):
        super().put(path, data)
        if "/segments/" in path:
            time.sleep(0.002)  # a read can list the segment before the flush ends


def stress_record(i):
    """Return record `i` of the stress test. Each has its own `ts`, so its own id."""
    return rec(("alice", "bob", "carol")[i % 3], f"2026-11-10T12:{i // 60:02d}:{i % 60:02d}.000Z")


def test_reads_during_flushes_see_each_record_once():
    store = SlowSegmentStore()
    log = AuditLog(store, "root")
    added = [0]
    stop = threading.Event()
    errors = []

    def write():
        try:
            for i in range(300):
                log.add(stress_record(i))
                added[0] = i + 1
                if i % 7 == 0:
                    log.flush()
            log.flush()
        finally:
            stop.set()

    def read():
        try:
            while not stop.is_set():
                before = added[0]
                ids = [r["request_id"] for r in log.rows_for_day(DAY)]
                assert len(ids) == len(set(ids)), "a record appears twice"
                assert {stress_record(i)["request_id"] for i in range(before)} <= set(ids)
        except Exception as error:  # reported by the main thread
            errors.append(error)

    readers = [threading.Thread(target=read) for _ in range(3)]
    for thread in readers:
        thread.start()
    write()
    for thread in readers:
        thread.join()
    assert errors == []
    assert len(AuditLog(store, "root").rows_for_day(DAY)) == 300


@pytest.mark.parametrize("change", ["put", "drop"])
def test_cached_day_file_is_not_replaced_by_an_older_read(change):
    gate, entered = threading.Event(), threading.Event()

    class SlowStore(MemoryBlobStore):
        def get(self, path):
            data = super().get(path)
            if path.endswith("/day.jsonl"):
                entered.set()
                assert gate.wait(5)
            return data

    store = SlowStore()
    old = [rec("alice", T)]
    path = day_path("root", DAY)
    rows_old = [{**r, "seg": "s1", "event": None, "detail": None, "prompt_ref": None} for r in old]
    store.put(path, "".join(json.dumps(r) + "\n" for r in rows_old))
    log = AuditLog(store, "root")
    reader = threading.Thread(target=log.read_rows, args=(path,))
    reader.start()
    assert entered.wait(5)
    rows_new = rows_old + [{**rows_old[0], "user": "bob", "seg": "s2"}]
    store.put(path, "".join(json.dumps(r) + "\n" for r in rows_new))
    log.cache_put(path, rows_new)  # a merge rewrites the day file
    if change == "drop":
        log.cache_drop([path])
    gate.set()
    reader.join()
    assert log.cached_rows(path) == (rows_new if change == "put" else None)
    assert [r["user"] for r in log.read_rows(path)] == ["alice", "bob"]


def test_stamps_grow_when_the_clock_steps_back():
    times = iter(
        [
            datetime(2026, 11, 10, 14, 0, 5, tzinfo=timezone.utc),
            datetime(2026, 11, 10, 14, 0, 1, tzinfo=timezone.utc),  # the clock steps back
        ]
    )
    store, log = make(now=lambda: next(times))
    calls = flushed(log)
    log.add(rec("alice", "2026-11-10T14:00:00.000Z"))
    log.flush()
    log.add(rec("bob", "2026-11-10T14:00:01.000Z"))
    log.flush()
    first, second = calls[0][1], calls[1][1]
    assert first.startswith("20261110T140005.000000-")
    assert second.startswith("20261110T140005.000001-") and second > first
    rows = AuditLog(store, "root").rows_for_day(DAY, after=first)
    assert [r["user"] for r in rows] == ["bob"]


def test_a_read_lists_again_when_a_merge_removes_a_listed_file():
    class MergingStore(MemoryBlobStore):
        """A store that merges a segment into a part between a listing and the read."""

        merge_on = None

        def get(self, path):
            if path == self.merge_on:
                self.merge_on = None
                body = super().get(path)
                super().put(part_path("root", DAY, bucket_of(path.rsplit("/", 1)[1])), body)
                self.delete(path)
            return super().get(path)

    alice, bob = rec("alice", T), rec("bob", T)

    def setup():
        store = MergingStore()
        _, writer = make(store, now=lambda: NOW)
        calls = flushed(writer)
        writer.add(alice)
        writer.add(bob)
        writer.flush()
        writer.add(rec("carol", "2026-11-10T14:30:00.000Z"))
        writer.flush()
        store.merge_on = segment_path("root", DAY, calls[0][1])
        return store, AuditLog(store, "root")  # its cache is empty, as after a restart

    store, log = setup()
    assert [r["user"] for r in log.rows_for_day(DAY)] == ["alice", "bob", "carol"]
    assert store.merge_on is None  # the merge ran during the read
    assert len(store.ls(f"root/audit/{DAY}/segments")) == 1
    store, log = setup()
    assert log.record(DAY, bob["request_id"]) == full(bob)
    assert store.merge_on is None
    store, log = setup()
    assert len(log.users()) == 3


def test_a_read_gives_up_listing_again_after_three_tries():
    class VanishingStore(RecordingStore):
        """A store whose listing always shows a file that is gone."""

        def ls(self, prefix):
            return sorted(super().ls(prefix) + [f"root/audit/{DAY}/segments/{STAMP}-gone.jsonl"])

    store = VanishingStore()
    _, writer = make(store)
    writer.add(rec("alice", T))
    writer.flush()
    log = AuditLog(store, "root")
    store.clear()
    assert [r["user"] for r in log.rows_for_day(DAY)] == ["alice"]
    assert store.ops("ls") == [f"root/audit/{DAY}"] * 3


def test_prompt_cache_drop_forgets_the_prompt_files_of_a_day():
    store, log = make()
    alice = rec("alice", T)
    log.add(alice)
    log.add(rec("bob", "2026-11-11T08:00:00.000Z"))
    log.flush()
    log = AuditLog(store, "root")
    bob_day = "2026-11-11"
    assert log.record(DAY, alice["request_id"]) == full(alice)
    assert log.record(bob_day, rec("bob", "2026-11-11T08:00:00.000Z")["request_id"])["prompt"]
    for path in store.ls("root/prompts"):
        store.delete(path)
    assert log.record(DAY, alice["request_id"]) == full(alice)  # still cached
    log.prompt_cache_drop(DAY)
    assert log.record(DAY, alice["request_id"]) == {**full(alice), "prompt": None, "prompt_deleted": True}
    bob = log.record(bob_day, rec("bob", "2026-11-11T08:00:00.000Z")["request_id"])
    assert bob["prompt"] is not None  # another day stays cached


class BarrierStore(CountingStore):
    """A counting store whose reads of the `paired` paths wait until both run at the same time."""

    def __init__(self) -> None:
        super().__init__()
        self.barrier = threading.Barrier(2, timeout=5)
        self.paired: set[str] = set()

    def get(self, path: str) -> bytes:
        if path in self.paired:
            self.barrier.wait()
        return super().get(path)


def test_read_many_reads_in_parallel_keeps_the_order_and_gives_empty_for_a_missing_file():
    store = BarrierStore()
    seg = "20261110T120000.000000-aa"
    paths = [segment_path("root", DAY, f"20261110T12000{n}.000000-aa") for n in range(3)]
    for n, path in enumerate(paths):
        store.put(path, json.dumps(dict(rec(f"u{n}", T), seg=seg)) + "\n")
    store.paired = {paths[0], paths[2]}
    log = AuditLog(store, "root")
    log.cache_put(paths[1], [dict(rec("cached", T), seg=seg)])
    missing = segment_path("root", DAY, "20261110T130000.000000-aa")
    chunks = log.read_many([paths[2], missing, paths[1], paths[0]])
    assert [[row["user"] for row in chunk] for chunk in chunks] == [["u2"], [], ["cached"], ["u0"]]
    assert sorted(store.gets) == sorted([paths[0], paths[2], missing])


class PausedPromptRead(MemoryBlobStore):
    """A store whose reads of prompt files wait for `release` after they got the data."""

    def __init__(self) -> None:
        super().__init__()
        self.pause = False
        self.reading = threading.Event()
        self.release = threading.Event()

    def get(self, path):
        data = super().get(path)
        if self.pause and "/prompts/" in path:
            self.reading.set()
            assert self.release.wait(5)
        return data


def test_a_prompt_read_that_overlaps_a_retention_delete_is_not_cached():
    store = PausedPromptRead()
    _, writer = make(store)
    alice = rec("alice", T)
    writer.add(alice)
    writer.flush()
    log = AuditLog(store, "root")
    (path,) = store.ls("root/prompts")
    store.pause = True
    answers = []
    reader = threading.Thread(target=lambda: answers.append(log.record(DAY, alice["request_id"])))
    reader.start()
    try:
        assert store.reading.wait(5)
        store.delete(path)
        log.prompt_cache_drop(DAY)
    finally:
        store.release.set()
        reader.join(5)
    assert not reader.is_alive()
    assert answers == [full(alice)]  # the read got the file before the delete
    store.pause = False
    assert log.record(DAY, alice["request_id"]) == {**full(alice), "prompt": None, "prompt_deleted": True}
    assert path not in log._prompts


def test_a_failed_marker_write_keeps_the_records_and_writes_no_prompt_and_no_segment():
    store, log = make(RecordingStore(), now=lambda: NOW)
    calls = flushed(log)
    alice = rec("alice", T)
    log.add(alice)
    store.fail_puts = {"prompts.marker"}
    assert log.flush() is False
    assert store.ops("put") == [prompt_marker_path("root", DAY)]
    assert store.ls("root/prompts") == [] and audit_files(store, "root/audit") == [] and calls == []
    assert [r["user"] for r in log.query()] == ["alice"]
    store.fail_puts = set()
    store.clear()
    assert log.flush() is True
    seg = calls[0][1]
    assert store.ops("put") == [
        prompt_marker_path("root", DAY),
        prompt_path("root", DAY, seg),
        segment_path("root", DAY, seg),
    ]
    assert [r["user"] for r in log.query()] == ["alice"]
    assert AuditLog(store, "root").record(DAY, alice["request_id"]) == full(alice)
