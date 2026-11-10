import asyncio
import json
import logging
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
