import asyncio
import json
import logging
import threading
from datetime import datetime, timezone

import pytest

from llm_service.gateway.audit import AuditLog, iso, parse_time, run_flusher, utc_now
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.tests.fakes import FlakyBlobStore

T = "2026-11-10T12:00:00.123Z"


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


def make(store=None, flush_size=100):
    store = store if store is not None else MemoryBlobStore()
    return store, AuditLog(store, "root", flush_size=flush_size)


def test_flush_writes_one_file_per_day():
    store, log = make()
    log.add(rec("alice", "2026-11-10T23:59:59.000Z"))
    log.add(rec("bob", "2026-11-11T00:00:01.000Z"))
    log.flush()
    paths = store.ls("root/audit")
    assert [p.split("/")[2] for p in paths] == ["2026-11-10", "2026-11-11"]
    assert paths[0].endswith(".jsonl")
    assert json.loads(store.get(paths[0]).decode().splitlines()[0])["user"] == "alice"
    log.flush()
    assert len(store.ls("root/audit")) == 2  # an empty buffer writes nothing


def test_add_never_writes_and_should_flush_at_size():
    store, log = make(flush_size=2)
    log.add(rec("alice", T))
    assert not log.should_flush()
    log.add(rec("bob", T))
    assert log.should_flush()
    assert store.ls("root/audit") == []  # add does no storage I/O
    assert log.flush() is True
    assert len(store.ls("root/audit")) == 1
    assert not log.should_flush()
    assert log.flush() is True  # the buffer is empty
    assert len(store.ls("root/audit")) == 1


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


def test_records_for_day():
    store, log = make()
    log.add(rec("alice", "2026-11-10T10:00:00.000Z"))
    log.flush()
    log.add(rec("bob", "2026-11-10T11:00:00.000Z"))
    log.add(rec("bob", "2026-11-11T11:00:00.000Z"))
    assert {r["user"] for r in log.records_for_day("2026-11-10")} == {"alice", "bob"}
    assert len(log.records_for_day("2026-11-10")) == 2


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
    assert len(store.ls("root/audit")) == 1
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
    assert len(store.ls("root/audit")) == 1


def test_flusher_writes_a_full_buffer_within_a_tick():
    store, log = make(flush_size=2)

    async def scenario():
        task = asyncio.create_task(run_flusher(log, 3600, tick=0.01))
        log.add(rec("alice", T))
        await asyncio.sleep(0.1)
        assert store.ls("root/audit") == []  # not full, interval not over
        log.add(rec("bob", T))
        await asyncio.sleep(0.2)
        assert len(store.ls("root/audit")) == 1
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
        calls.append(len(store.ls("root/audit")))
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
    assert len(store.ls("root/audit")) == 1
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
    """A store whose `put` waits for `release`. It writes before or after the wait."""

    def __init__(self, write_first: bool):
        super().__init__()
        self.write_first = write_first
        self.entered = threading.Event()
        self.release = threading.Event()

    def put(self, path, data):
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
        assert len(log.records_for_day("2026-11-10")) == 2
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
    assert len(store.ls("root/audit/2026-11-10")) == 1
    assert store.ls("root/audit/2026-11-09") == []
    assert [r["user"] for r in log.query()] == ["bob", "alice"]
    store.fail = False
    assert log.flush() is True
    assert len(store.ls("root/audit/2026-11-10")) == 1  # not written twice
    assert len(store.ls("root/audit/2026-11-09")) == 1
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
        for path in store.ls("root/audit")
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
    assert store.ls("root/audit") == []
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
    cold = log.records_for_day("2026-11-10")
    assert len(cold) == 62
    warm = log.records_for_day("2026-11-10")
    fresh = AuditLog(store, "root").records_for_day("2026-11-10")
    assert cold == warm == fresh
    assert [r["user"] for r in cold[-2:]] == ["same-a", "same-b"]
    assert log.query(limit=1000) == AuditLog(store, "root").query(limit=1000)
    assert [r["user"] for r in log.query(limit=2)] == ["same-b", "same-a"]


def test_mutating_a_returned_record_does_not_change_a_later_result():
    store = CountingStore()
    put_file(store, "120000.000000-a", [rec("alice", T)])
    log = AuditLog(store, "root")
    first = log.query()
    first[0]["user"] = "mallory"
    first[0]["prompt"]["messages"].append("x")
    day = log.records_for_day("2026-11-10")
    day[0]["user"] = "mallory"
    again = log.query()
    assert again[0]["user"] == "alice"
    assert again[0]["prompt"]["messages"] == rec("alice", T)["prompt"]["messages"]
    assert [u["user"] for u in log.users()] == ["alice"]


def test_cache_evicts_the_least_recently_used_file():
    store = CountingStore()
    paths = [put_file(store, f"12000{i}.000000-a", [rec(f"u{i}", T)]) for i in range(3)]
    log = AuditLog(store, "root", file_cache_size=2)
    log.records_for_day("2026-11-10")  # reads 3 files, keeps the last 2
    assert len(log._files) == 2
    store.gets.clear()
    log.records_for_day("2026-11-10")
    assert paths[0] in store.gets  # the oldest was evicted
    assert len(log._files) == 2


def test_users_and_records_for_day_use_the_cache():
    store = CountingStore()
    for i in range(3):
        put_file(store, f"12000{i}.000000-a", [rec("alice", T)])
    log = AuditLog(store, "root")
    assert log.users()[0]["requests"] == 3
    assert len(store.gets) == 3
    assert len(log.records_for_day("2026-11-10")) == 3
    assert log.users()[0]["requests"] == 3
    assert len(store.gets) == 3


def test_buffered_records_are_not_cached_and_still_appear():
    store = CountingStore()
    log = AuditLog(store, "root")
    log.add(rec("alice", T))
    assert len(log.query()) == 1
    assert len(log._files) == 0
    log.flush()
    assert len(log.query()) == 1
    assert len(log._files) == 1
