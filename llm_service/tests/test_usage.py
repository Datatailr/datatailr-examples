import json
import logging
import threading
from datetime import datetime, timedelta, timezone

import pytest

from llm_service.gateway.audit import AuditLog, iso
from llm_service.gateway.audit_merge import AuditMerger
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.gateway.usage import UsageTotals, is_segment_name
from llm_service.tests.fakes import FlakyBlobStore

D1 = "2026-11-09"
D2 = "2026-11-10"
T1 = "2026-11-09T08:00:00.000Z"
T2 = "2026-11-10T09:00:00.000Z"
T3 = "2026-11-10T10:00:00.000Z"
ROOT = "root"


def rec(user, ts, model="m1", prompt=10, completion=5, status=200, endpoint="/v1/chat/completions"):
    total = None if prompt is None or completion is None else prompt + completion
    return {
        "ts": ts,
        "request_id": f"{user}-{ts}",
        "user": user,
        "endpoint": endpoint,
        "model": model,
        "stream": False,
        "prompt": {},
        "status": status,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "duration_ms": 1,
    }


def make(today=D2, store=None):
    store = store or MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    state = {"today": today}
    usage = UsageTotals(store, ROOT, audit, lambda: state["today"])
    audit.listeners.append(usage.record)
    return store, audit, usage, state


def seed(audit):
    audit.add(rec("alice", T2, prompt=10, completion=5))
    audit.add(rec("alice", T3, prompt=20, completion=5, status=500))
    audit.add(rec("bob", T1, model="m2", prompt=1, completion=1))


def test_summary_by_user():
    _, audit, usage, _ = make()
    seed(audit)
    rows = usage.summary(None, None, None, None)
    assert rows[0] == {
        "user": "alice",
        "requests": 2,
        "failed_requests": 1,
        "prompt_tokens": 30,
        "completion_tokens": 10,
        "total_tokens": 40,
        "last_request": T3,
    }
    assert rows[1]["user"] == "bob"
    assert rows[1]["total_tokens"] == 2


def test_group_by_model_and_day_and_filters():
    _, audit, usage, _ = make()
    seed(audit)
    by_model = usage.summary(None, None, None, None, group_by="model")
    assert [r["model"] for r in by_model] == ["m1", "m2"]
    assert by_model[0]["requests"] == 2
    by_day = usage.summary(None, None, None, None, group_by="day")
    assert [r["day"] for r in by_day] == [D2, D1]
    assert by_day[0]["total_tokens"] == 40
    assert usage.summary(D2, None, None, None)[0]["user"] == "alice"
    assert [r["user"] for r in usage.summary(D2, None, None, None)] == ["alice"]
    assert [r["user"] for r in usage.summary(None, D1, None, None)] == ["bob"]
    assert [r["user"] for r in usage.summary(D1, D2, "bob", None)] == ["bob"]
    assert [r["user"] for r in usage.summary(None, None, None, "m2")] == ["bob"]
    assert usage.summary(None, None, "carol", None) == []


def test_ties_sort_by_key():
    _, audit, usage, _ = make()
    audit.add(rec("zed", T2))
    audit.add(rec("amy", T2))
    assert [r["user"] for r in usage.summary(None, None, None, None)] == ["amy", "zed"]


def test_group_by_invalid():
    _, _, usage, _ = make()
    with pytest.raises(ValueError):
        usage.summary(None, None, None, None, group_by="hour")


def test_null_tokens_and_admin_events_ignored():
    _, audit, usage, _ = make()
    audit.add(rec("alice", T2, prompt=None, completion=None))
    audit.add(rec("alice", T3, prompt=3, completion=4))
    audit.add(rec("admin", T3, endpoint="/admin/models/switch"))
    rows = usage.summary(None, None, None, None)
    assert len(rows) == 1
    assert rows[0]["requests"] == 2
    assert rows[0]["prompt_tokens"] == 3
    assert rows[0]["completion_tokens"] == 4
    assert rows[0]["total_tokens"] == 7


def test_user_detail_shape():
    _, audit, usage, _ = make()
    seed(audit)
    detail = usage.user_detail("alice", None, None)
    assert detail["user"] == "alice"
    assert detail["totals"]["requests"] == 2
    assert detail["totals"]["user"] == "alice"
    assert [r["model"] for r in detail["by_model"]] == ["m1"]
    assert [r["day"] for r in detail["by_day"]] == [D2]
    assert detail["by_day"][0]["total_tokens"] == 40


def test_user_detail_unknown_user():
    _, _, usage, _ = make()
    detail = usage.user_detail("nobody", None, None)
    assert detail["totals"] == {
        "user": "nobody",
        "requests": 0,
        "failed_requests": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "last_request": None,
    }
    assert detail["by_model"] == []
    assert detail["by_day"] == []


def test_load_uses_daily_file_and_rebuilds_missing_day():
    store = MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    audit.add(rec("alice", T1))  # day 1: one audit record
    audit.add(rec("bob", "2026-11-08T08:00:00.000Z"))  # day 0: no daily file
    audit.add(rec("carol", T2))  # today
    audit.flush()
    day1 = {
        "day": D1,
        "rows": [
            {
                "user": "alice",
                "model": "m1",
                "requests": 99,
                "failed_requests": 0,
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
                "last_request": T1,
            }
        ],
    }
    store.put(f"{ROOT}/usage/{D1}.json", json.dumps(day1))
    usage = UsageTotals(store, ROOT, audit, lambda: D2)
    usage.load()
    rows = {r["user"]: r for r in usage.summary(None, None, None, None)}
    assert rows["alice"]["requests"] == 99
    assert rows["bob"]["requests"] == 1
    assert rows["carol"]["requests"] == 1
    assert store.exists(f"{ROOT}/usage/2026-11-08.json")
    assert not store.exists(f"{ROOT}/usage/{D2}.json")


def test_load_treats_invalid_daily_file_as_missing(caplog):
    store = MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    audit.add(rec("alice", T1))
    audit.flush()
    store.put(f"{ROOT}/usage/{D1}.json", "{not json")
    usage = UsageTotals(store, ROOT, audit, lambda: D2)
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        usage.load()
    assert usage.summary(None, None, None, None)[0]["requests"] == 1
    assert any("llm_service.usage" == r.name for r in caplog.records)
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["day"] == D1


def test_rollover_writes_daily_file():
    store, audit, usage, state = make(today=D1)
    audit.add(rec("alice", T1))
    assert not store.exists(f"{ROOT}/usage/{D1}.json")
    state["today"] = D2
    audit.add(rec("alice", T2))
    assert not store.exists(f"{ROOT}/usage/{D1}.json")  # record does no storage I/O
    usage.close_days()
    saved = json.loads(store.get(f"{ROOT}/usage/{D1}.json"))
    assert saved["day"] == D1
    assert saved["rows"] == [
        {
            "user": "alice",
            "model": "m1",
            "requests": 1,
            "failed_requests": 0,
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "last_request": T1,
        }
    ]
    assert not store.exists(f"{ROOT}/usage/{D2}.json")


def test_failed_daily_write_is_retried_and_never_raises(caplog):
    store = FlakyBlobStore()
    audit = AuditLog(store, ROOT)
    state = {"today": D1}
    usage = UsageTotals(store, ROOT, audit, lambda: state["today"])
    audit.listeners.append(usage.record)
    audit.add(rec("alice", T1))
    store.fail_puts = True
    state["today"] = D2
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        audit.add(rec("alice", T2))
        usage.close_days()  # must not raise
    assert any(r.name == "llm_service.usage" for r in caplog.records)
    assert not store.exists(f"{ROOT}/usage/{D1}.json")
    store.fail_puts = False
    usage.close_days()
    assert store.exists(f"{ROOT}/usage/{D1}.json")


def test_late_record_for_finished_day_rewrites_file():
    store, audit, usage, state = make(today=D2)
    audit.add(rec("alice", T1))
    usage.close_days()
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["rows"][0]["requests"] == 1
    audit.add(rec("alice", T1))
    usage.close_days()
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["rows"][0]["requests"] == 2


# Checkpoint of the current day

DAY = "2026-11-10"
PARTIAL = f"{ROOT}/usage/{DAY}.partial.json"
GROUPS = ("user", "model", "day")


class Clock:
    """A clock that tests move by hand."""

    def __init__(self, text: str) -> None:
        self.t = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.t

    def advance(self, **delta) -> None:
        self.t += timedelta(**delta)

    def ts(self) -> str:
        return iso(self.t)


class CountingStore(MemoryBlobStore):
    """A memory store that records the paths it reads and can fail its deletes."""

    def __init__(self) -> None:
        super().__init__()
        self.gets: list[str] = []
        self.puts: list[str] = []
        self.fail_deletes = False
        self._count_lock = threading.Lock()

    def get(self, path: str) -> bytes:
        with self._count_lock:
            self.gets.append(path)
        return super().get(path)

    def put(self, path, data) -> None:
        with self._count_lock:
            self.puts.append(path)
        super().put(path, data)

    def delete(self, path: str) -> None:
        if self.fail_deletes:
            raise OSError("simulated delete failure")
        super().delete(path)


def start(store, clock, today=None):
    """Build an audit log and usage totals on `store`, load them, and connect them like the app."""
    audit = AuditLog(store, ROOT, now=clock)
    today = today or (lambda: clock().strftime("%Y-%m-%d"))
    usage = UsageTotals(store, ROOT, audit, today, now=clock)
    usage.load()
    audit.listeners.append(usage.record)
    audit.flushed_listeners.append(usage.flushed)
    return audit, usage


def totals(usage):
    return {group: usage.summary(None, None, None, None, group) for group in GROUPS}


def copy_store(store, skip=()):
    """Return a new memory store with the files of `store`, without the paths in `skip`."""
    copy = MemoryBlobStore()
    for path in store.ls(ROOT):
        if path not in skip:
            copy.put(path, MemoryBlobStore.get(store, path))
    return copy


def rebuilt(store, clock, today=None):
    """Return the totals of a start on a copy of `store` without any partial file."""
    partials = [p for p in store.ls(f"{ROOT}/usage") if p.endswith(".partial.json")]
    _, usage = start(copy_store(store, partials), clock, today)
    return totals(usage)


def partial(store, day=DAY):
    return json.loads(store.get(f"{ROOT}/usage/{day}.partial.json"))


def flush_one(audit, clock, user, **kwargs):
    """Add one record at the clock time, flush it, and return the segment name."""
    calls = []
    audit.flushed_listeners.append(lambda day, seg, rows: calls.append(seg))
    audit.add(rec(user, clock.ts(), **kwargs))
    audit.flush()
    audit.flushed_listeners.pop()
    return calls[0]


def old_file(store, name, records, day=DAY):
    """Write an old layout file: full records directly in the day folder."""
    path = f"{ROOT}/audit/{day}/{name}.jsonl"
    store.put(path, "".join(json.dumps(r) + "\n" for r in records))
    return path


def test_segment_name_check_tells_new_names_from_old_names():
    assert is_segment_name("20261111T140359.123456-ab12")
    assert not is_segment_name("235959.000000-abc")
    assert not is_segment_name("")
    assert not is_segment_name(None)
    assert "235959.000000-abc" > "20261111T140359.123456-ab12"  # why the check is needed


def test_flushed_rows_reach_the_checkpoint_and_buffered_rows_do_not():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock)
    seg = flush_one(audit, clock, "alice", prompt=10, completion=5)
    audit.add(rec("bob", clock.ts()))  # stays in the buffer
    usage.checkpoint()
    saved = partial(store)
    assert saved["day"] == DAY and saved["through"] == seg
    assert saved["rows"] == [
        {
            "user": "alice",
            "model": "m1",
            "requests": 1,
            "failed_requests": 0,
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "last_request": iso(clock.t),
        }
    ]
    assert [r["user"] for r in usage.summary(None, None, None, None)] == ["alice", "bob"]


def test_flushed_does_no_storage_io_and_never_raises(caplog):
    clock = Clock("2026-11-10T14:00:00")
    store = CountingStore()
    _, usage = start(store, clock)
    store.gets.clear()
    store.puts.clear()
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        usage.flushed(DAY, "20261110T140000.000000-aa", [{"endpoint": "/v1/chat/completions"}])
        usage.flushed(DAY, "20261110T140001.000000-bb", None)
    assert store.gets == [] and store.puts == []
    assert any(r.name == "llm_service.usage" for r in caplog.records)


def test_checkpoint_writes_at_most_once_each_60_seconds_and_only_after_a_change():
    clock = Clock("2026-11-10T14:00:00")
    store = CountingStore()
    audit, usage = start(store, clock)
    usage.checkpoint()
    assert not store.exists(PARTIAL)  # no flushed row yet
    first = flush_one(audit, clock, "alice")
    usage.checkpoint()
    assert partial(store)["through"] == first
    clock.advance(seconds=30)
    second = flush_one(audit, clock, "bob")
    usage.checkpoint()
    assert partial(store)["through"] == first  # 30 seconds after the last write
    clock.advance(seconds=30)
    usage.checkpoint()
    assert partial(store)["through"] == second
    clock.advance(seconds=120)
    store.puts.clear()
    usage.checkpoint()
    assert store.puts == []  # nothing changed


def test_forced_checkpoint_ignores_the_60_second_limit_and_still_needs_a_change():
    clock = Clock("2026-11-10T14:00:00")
    store = CountingStore()
    audit, usage = start(store, clock)
    flush_one(audit, clock, "alice")
    usage.checkpoint()
    clock.advance(seconds=10)
    second = flush_one(audit, clock, "bob")
    usage.checkpoint()
    assert partial(store)["through"] != second  # held back by the limit
    usage.checkpoint(force=True)
    assert partial(store)["through"] == second
    store.puts.clear()
    usage.checkpoint(force=True)
    assert store.puts == []  # nothing changed


def test_failed_checkpoint_is_logged_and_tried_at_the_next_call(caplog):
    clock = Clock("2026-11-10T14:00:00")
    store = FlakyBlobStore()
    audit, usage = start(store, clock)
    seg = flush_one(audit, clock, "alice")
    store.fail_puts = True
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        usage.checkpoint()  # must not raise
    assert any(r.name == "llm_service.usage" for r in caplog.records)
    assert not store.exists(PARTIAL)
    store.fail_puts = False
    clock.advance(seconds=5)
    usage.checkpoint()
    assert partial(store)["through"] == seg


def test_start_from_a_checkpoint_reads_only_later_rows():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock)
    flush_one(audit, clock, "alice", prompt=7)
    clock.advance(minutes=3)
    through = flush_one(audit, clock, "bob", model="m2")
    usage.checkpoint()
    clock.advance(minutes=3)
    flush_one(audit, clock, "carol", status=500)
    clock.advance(minutes=3)
    flush_one(audit, clock, "alice", prompt=3)
    before = totals(usage)

    reader = AuditLog(store, ROOT, now=clock)
    calls = []
    real = reader.rows_for_day

    def spy(day, after=None):
        calls.append((day, after))
        return real(day, after)

    reader.rows_for_day = spy
    restarted = UsageTotals(store, ROOT, reader, lambda: DAY, now=clock)
    restarted.load()
    assert calls == [(DAY, through)]
    assert totals(restarted) == before == rebuilt(store, clock)


def test_three_start_cases():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock)
    flush_one(audit, clock, "alice")
    usage.checkpoint()
    clock.advance(minutes=1)
    flush_one(audit, clock, "bob")
    audit.add(rec("carol", "2026-11-09T08:00:00.000Z"))  # a day with a daily file
    audit.add(rec("bob", "2026-11-08T08:00:00.000Z"))  # a day with nothing
    audit.flush()
    store.put(
        f"{ROOT}/usage/{D1}.json",
        json.dumps({"day": D1, "rows": [{"user": "carol", "model": "m1", **_counts(99)}]}),
    )
    restarted = start(store, clock, lambda: DAY)[1]
    rows = {r["day"]: r for r in restarted.summary(None, None, None, None, "day")}
    assert rows[D1]["requests"] == 99  # case 1: the daily file
    assert rows[DAY]["requests"] == 2  # case 2: the partial file and the later row
    assert rows["2026-11-08"]["requests"] == 1  # case 3: all metadata of the day
    assert not store.exists(f"{ROOT}/usage/2026-11-08.partial.json")


def _counts(requests):
    return {
        "requests": requests,
        "failed_requests": 0,
        "prompt_tokens": 1,
        "completion_tokens": 1,
        "total_tokens": 2,
        "last_request": T1,
    }


def test_partial_file_without_a_segment_reads_the_whole_day():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, _ = start(store, clock)
    flush_one(audit, clock, "alice")
    store.put(PARTIAL, json.dumps({"day": DAY, "through": None, "rows": []}))
    reader = AuditLog(store, ROOT, now=clock)
    calls = []
    real = reader.rows_for_day
    reader.rows_for_day = lambda day, after=None: calls.append(after) or real(day, after)
    usage = UsageTotals(store, ROOT, reader, lambda: DAY, now=clock)
    usage.load()
    assert calls == [None]
    assert usage.summary(None, None, None, None)[0]["requests"] == 1


@pytest.mark.parametrize(
    "body",
    [
        "{not json",
        json.dumps({"day": D1, "through": None, "rows": []}),
        json.dumps({"day": DAY, "through": "235959.000000-abc", "rows": []}),
        json.dumps({"day": DAY, "through": 5, "rows": []}),
        json.dumps({"day": DAY, "rows": []}),
        json.dumps({"day": DAY, "through": None, "rows": [{"user": "alice"}]}),
    ],
)
def test_invalid_partial_file_is_ignored_and_the_day_is_rebuilt(caplog, body):
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, _ = start(store, clock)
    flush_one(audit, clock, "alice")
    flush_one(audit, clock, "bob")
    store.put(PARTIAL, body)
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        _, usage = start(store, clock, lambda: DAY)
    warnings = [r for r in caplog.records if r.name == "llm_service.usage"]
    assert len(warnings) == 1 and "partial" in warnings[0].getMessage()
    assert usage.summary(None, None, None, None, "day")[0]["requests"] == 2


def test_partial_file_is_deleted_when_the_day_closes():
    clock = Clock("2026-11-10T23:58:00")
    store = CountingStore()
    audit, usage = start(store, clock)
    flush_one(audit, clock, "alice")
    usage.checkpoint()
    assert store.exists(PARTIAL)
    clock.advance(minutes=5)
    store.fail_deletes = True
    usage.close_days()
    assert store.exists(f"{ROOT}/usage/{DAY}.json")
    assert store.exists(PARTIAL)  # the delete failed
    clock.advance(minutes=5)
    usage.checkpoint()
    assert json.loads(store.get(PARTIAL))["through"] is not None
    store.fail_deletes = False
    usage.close_days()
    assert not store.exists(PARTIAL)
    clock.advance(minutes=5)
    usage.flushed(DAY, "20261111T000500.000000-aa", [])  # a closed day keeps no flushed totals
    usage.checkpoint()
    assert not store.exists(PARTIAL)


def test_record_before_midnight_flushed_after_midnight_counts_once_after_a_restart():
    clock = Clock("2026-11-10T23:59:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock)
    flush_one(audit, clock, "alice")
    usage.checkpoint()
    audit.add(rec("bob", "2026-11-10T23:59:59.500Z"))
    clock.advance(minutes=1, seconds=2)  # 00:00:02 of the next day
    audit.flush()  # a segment of the old day with a stamp of the new day
    # The process stops here, before any checkpoint or close of the old day.
    _, restarted = start(store, clock)
    assert restarted.summary(DAY, DAY, None, None, "day")[0]["requests"] == 2
    assert json.loads(store.get(f"{ROOT}/usage/{DAY}.json"))["rows"]
    assert not store.exists(PARTIAL)
    _, again = start(store, clock)
    assert again.summary(DAY, DAY, None, None, "day")[0]["requests"] == 2


def test_late_flush_of_the_old_day_reaches_its_checkpoint():
    clock = Clock("2026-11-10T23:59:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock, lambda: DAY)
    flush_one(audit, clock, "alice")
    usage.checkpoint()
    audit.add(rec("bob", "2026-11-10T23:59:59.500Z"))
    clock.advance(minutes=1, seconds=2)
    audit.flush()
    usage.checkpoint()
    assert [(r["user"], r["requests"]) for r in partial(store)["rows"]] == [("alice", 1), ("bob", 1)]
    _, restarted = start(store, clock, lambda: DAY)
    assert restarted.summary(DAY, DAY, None, None, "day")[0]["requests"] == 2


def test_restart_from_a_checkpoint_equals_a_full_rebuild():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock)
    for index, user in enumerate(["alice", "bob", "carol", "alice"]):
        status = 200 + 100 * index
        flush_one(audit, clock, user, model=f"m{index % 2}", prompt=index + 1, status=status)
        clock.advance(minutes=4)
    clock.advance(seconds=60)
    usage.checkpoint()
    for user in ["bob", "carol"]:
        clock.advance(minutes=4)
        flush_one(audit, clock, user, completion=9)
    audit.add(rec("alice", clock.ts(), endpoint="/admin/models/switch"))  # not counted
    audit.flush()
    expected = totals(usage)
    _, restarted = start(store, clock)
    assert totals(restarted) == expected == rebuilt(store, clock)
    clock.advance(seconds=60)
    restarted.checkpoint()  # the load moved `through` to the last segment
    _, again = start(store, clock)
    assert totals(again) == expected


def test_restart_with_an_old_layout_file_equals_a_full_rebuild():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    old_file(
        store,
        "235959.000000-abc",
        [
            rec("carol", "2026-11-10T09:00:00.000Z", prompt=4),
            rec("bob", "2026-11-10T09:30:00.000Z"),
        ],
    )
    audit, usage = start(store, clock)
    first = flush_one(audit, clock, "alice")
    usage.checkpoint()
    assert partial(store)["through"] == first
    clock.advance(minutes=2)
    flush_one(audit, clock, "bob", prompt=8)
    expected = totals(usage)
    assert expected["day"][0]["requests"] == 4
    _, restarted = start(store, clock)
    assert totals(restarted) == expected == rebuilt(store, clock)


def test_start_from_a_checkpoint_reads_no_segment_up_to_through():
    clock = Clock("2026-11-10T14:00:00")
    store = CountingStore()
    audit, usage = start(store, clock)
    early = []
    for user in ["alice", "bob", "carol"]:
        early.append(flush_one(audit, clock, user))
        clock.advance(seconds=20)
    clock.advance(seconds=60)
    usage.checkpoint()
    through = partial(store)["through"]
    assert through == early[-1]
    later = flush_one(audit, clock, "alice")
    store.gets.clear()
    _, restarted = start(store, clock)
    segments = [p for p in store.gets if "/segments/" in p]
    assert segments == [f"{ROOT}/audit/{DAY}/segments/{later}.jsonl"]
    assert not any("/prompts/" in p for p in store.gets)
    assert restarted.summary(None, None, None, None, "day")[0]["requests"] == 4


def test_totals_are_equal_before_and_after_a_merge():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit, usage = start(store, clock, lambda: DAY)
    merger = AuditMerger(store, ROOT, audit, now=clock)
    flush_one(audit, clock, "alice")
    clock.advance(minutes=5)
    flush_one(audit, clock, "bob")
    usage.checkpoint()
    clock.advance(minutes=7)
    flush_one(audit, clock, "carol")
    expected = totals(usage)
    clock.advance(minutes=20)
    merger.merge()
    assert store.ls(f"{ROOT}/audit/{DAY}/parts")
    assert not store.ls(f"{ROOT}/audit/{DAY}/segments")
    assert totals(start(store, clock, lambda: DAY)[1]) == expected
    assert rebuilt(store, clock, lambda: DAY) == expected
    clock.advance(hours=10)
    merger.merge()
    assert store.exists(f"{ROOT}/audit/{DAY}/day.jsonl")
    assert totals(start(store, clock, lambda: DAY)[1]) == expected
    assert rebuilt(store, clock, lambda: DAY) == expected


def test_buffered_rows_at_load_count_once_and_stay_out_of_the_checkpoint():
    clock = Clock("2026-11-10T14:00:00")
    store = MemoryBlobStore()
    audit = AuditLog(store, ROOT, now=clock)
    usage = UsageTotals(store, ROOT, audit, lambda: DAY, now=clock)
    audit.listeners.append(usage.record)
    audit.flushed_listeners.append(usage.flushed)
    seg = flush_one(audit, clock, "alice")
    audit.add(rec("bob", clock.ts()))  # counted by `record`, still buffered
    usage.load()
    assert [r["requests"] for r in usage.summary(None, None, None, None)] == [1, 1]
    usage.checkpoint()
    assert partial(store)["through"] == seg
    assert [r["user"] for r in partial(store)["rows"]] == ["alice"]


def test_default_clock_is_utc():
    usage = UsageTotals(MemoryBlobStore(), ROOT, AuditLog(MemoryBlobStore(), ROOT), lambda: DAY)
    assert usage._now().tzinfo is not None


class CallStore(MemoryBlobStore):
    """A memory store that records each `ls`, `exists` and `get` call."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []
        self._calls_lock = threading.Lock()

    def _note(self, kind: str, path: str) -> None:
        with self._calls_lock:
            self.calls.append((kind, path))

    def ls(self, prefix):
        self._note("ls", prefix)
        return super().ls(prefix)

    def exists(self, path):
        self._note("exists", path)
        return super().exists(path)

    def get(self, path):
        self._note("get", path)
        return super().get(path)


def test_load_lists_usage_once_and_reads_each_daily_file_without_exists():
    store = CallStore()
    audit = AuditLog(store, ROOT)
    first = datetime(2026, 10, 1, tzinfo=timezone.utc)
    days = [(first + timedelta(days=n)).strftime("%Y-%m-%d") for n in range(40)]
    for n, day in enumerate(days):
        audit.add(rec("alice", f"{day}T08:00:00.000Z"))
        row = {
            "user": "alice",
            "model": "m1",
            "requests": n + 1,
            "failed_requests": 0,
            "prompt_tokens": n,
            "completion_tokens": 1,
            "total_tokens": n + 1,
            "last_request": f"{day}T08:00:00.000Z",
        }
        store.put(f"{ROOT}/usage/{day}.json", json.dumps({"day": day, "rows": [row]}))
    audit.flush()
    store.calls.clear()
    usage = UsageTotals(store, ROOT, audit, lambda: "2026-11-10")
    assert usage.load() == 40
    usage_calls = [(kind, path) for kind, path in store.calls if path.startswith(f"{ROOT}/usage")]
    assert [call for call in usage_calls if call[0] == "ls"] == [("ls", f"{ROOT}/usage")]
    assert not any(kind == "exists" for kind, _ in store.calls)
    assert sorted(path for kind, path in usage_calls if kind == "get") == [
        f"{ROOT}/usage/{day}.json" for day in days
    ]
    by_day = {row["day"]: row for row in usage.summary(None, None, None, None, "day")}
    assert {day: by_day[day]["requests"] for day in days} == {day: n + 1 for n, day in enumerate(days)}
    assert usage.summary(None, None, None, None)[0]["requests"] == sum(range(1, 41))
