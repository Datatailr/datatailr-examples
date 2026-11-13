"""Tests for the metrics store: one file for each UTC day and the history read."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone

import pytest

from llm_service.gateway.metrics_store import (
    DROP_AFTER_SECONDS,
    MAX_DAYS,
    MAX_POINTS,
    WRITE_EVERY_SECONDS,
    MetricsStore,
    merge_points,
)
from llm_service.gateway.storage import MemoryBlobStore

ROOT = "llm"
UTC = timezone.utc
DAY1 = "2026-11-12"
DAY2 = "2026-11-13"


def path(day):
    return f"{ROOT}/metrics/{day}.jsonl"


class Clock:
    def __init__(self, start="2026-11-12T10:00:00"):
        self.value = datetime.fromisoformat(start).replace(tzinfo=UTC)

    def __call__(self):
        return self.value

    def move(self, **kwargs):
        self.value += timedelta(**kwargs)


def point(ts, **over):
    base = {
        "ts": ts,
        "requests": 1,
        "failed_requests": 0,
        "in_flight_max": 1,
        "waiting_max": 0,
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "ttft_ms_p50": 100,
        "ttft_ms_p95": 200,
        "duration_ms_p50": 1000,
        "duration_ms_p95": 2000,
        "gpu_memory_used_max": 500,
        "gpu_memory_total": 1000,
    }
    base.update(over)
    return base


def minute(day, hour, mnt, **over):
    return point(f"{day}T{hour:02d}:{mnt:02d}:00Z", **over)


def lines(store, day):
    return [json.loads(x) for x in store.get(path(day)).decode().splitlines()]


class FailingPut(MemoryBlobStore):
    def __init__(self):
        super().__init__()
        self.fail = True

    def put(self, p, data):
        if self.fail:
            raise OSError("disk full")
        super().put(p, data)


class FailingGet(MemoryBlobStore):
    def __init__(self):
        super().__init__()
        self.fail = True
        self.gets = 0

    def get(self, p):
        self.gets += 1
        if self.fail:
            raise OSError("storage down")
        return super().get(p)


class Counting(MemoryBlobStore):
    def __init__(self):
        super().__init__()
        self.lists = 0
        self.puts = 0
        self.fail_delete = False

    def ls(self, prefix):
        self.lists += 1
        return super().ls(prefix)

    def put(self, p, data):
        self.puts += 1
        super().put(p, data)

    def delete(self, p):
        if self.fail_delete:
            raise OSError("no delete")
        super().delete(p)


def make(store=None, clock=None, retention_days=30):
    store = store if store is not None else MemoryBlobStore()
    clock = clock or Clock()
    return MetricsStore(store, ROOT, clock, retention_days), store, clock


# merge_points


def many(n):
    base = datetime(2026, 11, 12, tzinfo=UTC)
    return [point((base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")) for i in range(n)]


S = datetime(2026, 11, 12, tzinfo=UTC)


def test_merge_fewer_than_limit_keeps_points():
    pts = many(10)
    out, step = merge_points(pts, S, S + timedelta(minutes=9))
    assert out == pts
    assert step == 60


def test_merge_exactly_limit_keeps_points():
    pts = many(MAX_POINTS)
    out, step = merge_points(pts, S, S + timedelta(minutes=MAX_POINTS - 1))
    assert len(out) == MAX_POINTS
    assert step == 60


def test_merge_more_than_limit_groups_points():
    pts = many(MAX_POINTS + 1)
    out, step = merge_points(pts, S, S + timedelta(minutes=MAX_POINTS))
    assert step == 120
    assert len(out) <= MAX_POINTS
    assert out[0]["ts"] == pts[0]["ts"]
    assert out[1]["ts"] == pts[2]["ts"]
    assert out[0]["requests"] == 2
    assert sum(p["requests"] for p in out) == MAX_POINTS + 1


def test_merge_large_group_size():
    out, step = merge_points(many(1440), S, S + timedelta(minutes=1439), limit=100)
    assert step == 60 * 15
    assert len(out) <= 100


def test_merge_step_comes_from_the_range_not_from_the_points():
    # Two points in a range of 1000 minutes: the step is 2 minutes, though no point merges.
    pts = [point("2026-11-12T00:01:00Z"), point("2026-11-12T10:00:00Z")]
    out, step = merge_points(pts, S, S + timedelta(minutes=999))
    assert step == 120
    assert [p["ts"] for p in out] == ["2026-11-12T00:00:00Z", "2026-11-12T10:00:00Z"]


def test_merge_groups_by_time_across_a_gap():
    pts = [point(f"2026-11-12T00:0{m}:00Z", requests=m + 1) for m in (0, 1, 7, 8)]
    out, step = merge_points(pts, S, S + timedelta(minutes=9), limit=5)
    assert step == 120
    # The minutes 2 to 6 have no point, so their buckets give no point.
    assert [(p["ts"], p["requests"]) for p in out] == [
        ("2026-11-12T00:00:00Z", 3),
        ("2026-11-12T00:06:00Z", 8),
        ("2026-11-12T00:08:00Z", 9),
    ]


def test_merge_buckets_start_at_the_minute_of_since():
    pts = [point("2026-11-12T00:01:00Z"), point("2026-11-12T00:02:00Z")]
    out, step = merge_points(pts, S + timedelta(seconds=30), S + timedelta(minutes=3), limit=2)
    assert step == 120
    assert [p["ts"] for p in out] == ["2026-11-12T00:00:00Z", "2026-11-12T00:02:00Z"]


def test_merge_field_rules():
    a = point(
        "2026-11-12T10:00:00Z", requests=2, failed_requests=1, prompt_tokens=10, completion_tokens=4,
        in_flight_max=3, waiting_max=1, ttft_ms_p50=100, ttft_ms_p95=300, duration_ms_p50=900,
        duration_ms_p95=1500, gpu_memory_used_max=400, gpu_memory_total=1000,
    )
    b = point(
        "2026-11-12T10:01:00Z", requests=5, failed_requests=2, prompt_tokens=20, completion_tokens=6,
        in_flight_max=2, waiting_max=4, ttft_ms_p50=250, ttft_ms_p95=200, duration_ms_p50=800,
        duration_ms_p95=2500, gpu_memory_used_max=600, gpu_memory_total=2000,
    )
    ten = S + timedelta(hours=10)
    out, step = merge_points([a, b], ten, ten + timedelta(minutes=1), limit=1)
    assert step == 120
    assert out == [
        {
            "ts": "2026-11-12T10:00:00Z", "requests": 7, "failed_requests": 3,
            "in_flight_max": 3, "waiting_max": 4, "prompt_tokens": 30, "completion_tokens": 10,
            "ttft_ms_p50": 250, "ttft_ms_p95": 300, "duration_ms_p50": 900, "duration_ms_p95": 2500,
            "gpu_memory_used_max": 600, "gpu_memory_total": 2000,
        }
    ]


def test_merge_null_handling():
    nulls = dict.fromkeys(
        ("in_flight_max", "waiting_max", "ttft_ms_p50", "ttft_ms_p95", "duration_ms_p50",
         "duration_ms_p95", "gpu_memory_used_max", "gpu_memory_total")
    )
    a = point("2026-11-12T10:00:00Z", **nulls)
    b = point("2026-11-12T10:01:00Z", **nulls)
    c = point("2026-11-12T10:02:00Z", ttft_ms_p95=700, gpu_memory_total=1500)
    d = point("2026-11-12T10:03:00Z", **nulls)
    ten = S + timedelta(hours=10)
    out, _ = merge_points([a, b], ten, ten + timedelta(minutes=1), limit=1)
    assert all(out[0][name] is None for name in nulls)
    out, _ = merge_points([a, c, d], ten, ten + timedelta(minutes=3), limit=1)
    assert out[0]["ttft_ms_p95"] == 700
    assert out[0]["ttft_ms_p50"] == 100
    assert out[0]["gpu_memory_total"] == 1500
    assert out[0]["in_flight_max"] == 1


def test_merge_empty():
    assert merge_points([], S, S) == ([], 60)


# load_today


def test_load_today_missing_file_means_no_points():
    ms, store, _ = make()
    ms.load_today()
    assert ms.history(datetime(2026, 11, 12, 9, tzinfo=UTC), datetime(2026, 11, 12, 10, tzinfo=UTC)) == ([], 60, [])


def test_load_today_reads_good_file():
    store = MemoryBlobStore()
    pts = [minute(DAY1, 9, 0), minute(DAY1, 9, 1)]
    store.put(path(DAY1), "\n".join(json.dumps(p) for p in pts) + "\n")
    ms, _, _ = make(store)
    ms.load_today()
    got, _, _ = ms.history(datetime(2026, 11, 12, 9, tzinfo=UTC), datetime(2026, 11, 12, 10, tzinfo=UTC))
    assert got == pts


def test_load_today_skips_broken_lines():
    store = MemoryBlobStore()
    good = minute(DAY1, 9, 0)
    store.put(path(DAY1), json.dumps(good) + "\n{broken\n[1, 2]\n" + json.dumps({"ts": 5}) + "\n\n")
    ms, _, _ = make(store)
    ms.load_today()
    got, _, _ = ms.history(datetime(2026, 11, 12, 9, tzinfo=UTC), datetime(2026, 11, 12, 10, tzinfo=UTC))
    assert got == [good]


# add and write


def test_add_then_write_gives_sorted_file():
    ms, store, _ = make()
    ms.load_today()
    ms.add([minute(DAY1, 9, 5), minute(DAY1, 9, 3)])
    ms.write(force=True)
    assert [p["ts"] for p in lines(store, DAY1)] == ["2026-11-12T09:03:00Z", "2026-11-12T09:05:00Z"]


def test_add_alone_writes_nothing():
    ms, store, _ = make()
    ms.add([minute(DAY1, 9, 5)])
    assert store.ls(f"{ROOT}/metrics") == []


def test_same_ts_replaces_point():
    ms, store, _ = make()
    ms.load_today()
    ms.add([minute(DAY1, 9, 5, requests=1)])
    ms.add([minute(DAY1, 9, 5, requests=7)])
    ms.write(force=True)
    assert [p["requests"] for p in lines(store, DAY1)] == [7]


def test_second_write_inside_five_minutes_writes_nothing():
    store = Counting()
    ms, _, clock = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write()
    assert store.puts == 1
    ms.add([minute(DAY1, 9, 1)])
    clock.move(seconds=WRITE_EVERY_SECONDS - 1)
    ms.write()
    assert store.puts == 1
    clock.move(seconds=1)
    ms.write()
    assert store.puts == 2
    assert len(lines(store, DAY1)) == 2


def test_force_writes_inside_five_minutes():
    store = Counting()
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write()
    ms.add([minute(DAY1, 9, 1)])
    ms.write(force=True)
    assert store.puts == 2


def test_write_without_new_points_writes_nothing():
    store = Counting()
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write(force=True)
    ms.write(force=True)
    assert store.puts == 1


def test_write_at_midnight_gives_two_files():
    ms, store, clock = make(clock=Clock("2026-11-12T23:59:30"))
    ms.load_today()
    ms.add([minute(DAY1, 23, 59), minute(DAY2, 0, 0)])
    clock.move(minutes=1)
    ms.write(force=True)
    assert [p["ts"] for p in lines(store, DAY1)] == ["2026-11-12T23:59:00Z"]
    assert [p["ts"] for p in lines(store, DAY2)] == ["2026-11-13T00:00:00Z"]


def test_new_store_continues_the_day():
    ms, store, _ = make()
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write(force=True)
    again, _, _ = make(store)
    again.load_today()
    again.add([minute(DAY1, 9, 1)])
    again.write(force=True)
    assert [p["ts"] for p in lines(store, DAY1)] == ["2026-11-12T09:00:00Z", "2026-11-12T09:01:00Z"]


def test_write_without_load_today_keeps_stored_points():
    ms, store, _ = make()
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write(force=True)
    fresh, _, _ = make(store)
    fresh.add([minute(DAY1, 9, 1)])
    fresh.write(force=True)
    assert len(lines(store, DAY1)) == 2


def test_failed_put_keeps_points_for_next_write(caplog):
    store = FailingPut()
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    with caplog.at_level(logging.ERROR):
        ms.write(force=True)
    assert store.ls(f"{ROOT}/metrics") == []
    store.fail = False
    ms.add([minute(DAY1, 9, 1)])
    ms.write(force=True)
    assert len(lines(store, DAY1)) == 2


def test_failed_read_in_load_today_does_not_erase_stored_points(caplog):
    store = FailingGet()
    store.fail = False
    ms, _, _ = make(store)
    ms.add([minute(DAY1, 9, 0)])
    ms.write(force=True)
    store.fail = True
    again, _, _ = make(store)
    with caplog.at_level(logging.ERROR):
        again.load_today()
    assert "storage down" in caplog.text
    again.add([minute(DAY1, 9, 1)])
    again.write(force=True)
    store.fail = False
    assert len(lines(store, DAY1)) == 1  # skipped while the read fails
    again.write(force=True)
    assert [p["ts"] for p in lines(store, DAY1)] == ["2026-11-12T09:00:00Z", "2026-11-12T09:01:00Z"]


def test_history_after_failed_load_today_reads_file_and_merges_memory():
    store = FailingGet()
    store.fail = False
    first, _, _ = make(store)
    first.add([minute(DAY1, 9, 0)])
    first.write(force=True)
    store.fail = True
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 1)])
    store.fail = False
    got, _, _ = ms.history(datetime(2026, 11, 12, 9, tzinfo=UTC), datetime(2026, 11, 12, 10, tzinfo=UTC))
    assert [p["ts"] for p in got] == ["2026-11-12T09:00:00Z", "2026-11-12T09:01:00Z"]


def test_history_uses_memory_only_after_good_load_today():
    store = FailingGet()
    store.fail = False
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    before = store.gets
    ms.history(datetime(2026, 11, 12, tzinfo=UTC), datetime(2026, 11, 12, 23, tzinfo=UTC))
    assert store.gets == before


def test_past_day_dropped_after_one_hour_without_good_write(caplog):
    store = FailingPut()
    ms, _, clock = make(store, Clock("2026-11-12T23:58:00"))
    ms.load_today()
    ms.add([minute(DAY1, 23, 58), minute(DAY2, 0, 1)])
    clock.move(minutes=5)  # now on day 2
    ms.write(force=True)
    clock.value = datetime(2026, 11, 13, tzinfo=UTC) + timedelta(seconds=DROP_AFTER_SECONDS - 1)
    with caplog.at_level(logging.ERROR):
        ms.write(force=True)
    assert "dropping" not in caplog.text
    clock.move(seconds=1)
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        ms.write(force=True)
        ms.write(force=True)
    assert sum("dropping" in r.getMessage() for r in caplog.records) == 1
    store.fail = False
    ms.write(force=True)
    assert store.ls(f"{ROOT}/metrics") == [path(DAY2)]  # the past day is gone, the current day is kept
    assert [p["ts"] for p in lines(store, DAY2)] == ["2026-11-13T00:01:00Z"]


def test_past_day_leaves_memory_after_good_write():
    store = FailingGet()
    store.fail = False
    ms, _, clock = make(store, Clock("2026-11-12T23:58:00"))
    ms.load_today()
    ms.add([minute(DAY1, 23, 58)])
    clock.move(minutes=5)
    ms.write(force=True)
    before = store.gets
    got, _, _ = ms.history(datetime(2026, 11, 12, 23, tzinfo=UTC), datetime(2026, 11, 12, 23, 59, tzinfo=UTC))
    assert store.gets == before + 1  # read from the file, not from memory
    assert [p["ts"] for p in got] == ["2026-11-12T23:58:00Z"]


# history


def test_history_over_two_days_with_memory_only_point():
    ms, store, clock = make(clock=Clock("2026-11-13T10:00:00"))
    store.put(path(DAY1), json.dumps(minute(DAY1, 23, 0)) + "\n" + json.dumps(minute(DAY1, 23, 1)) + "\n")
    ms.load_today()
    ms.add([minute(DAY2, 0, 5)])
    got, step, _ = ms.history(datetime(2026, 11, 12, 22, tzinfo=UTC), datetime(2026, 11, 13, 2, tzinfo=UTC))
    assert [p["ts"] for p in got] == ["2026-11-12T23:00:00Z", "2026-11-12T23:01:00Z", "2026-11-13T00:05:00Z"]
    assert step == 60
    # Two days have 2881 minutes, so the step is 6 minutes and the two points of 23:00 merge.
    got, step, _ = ms.history(datetime(2026, 11, 12, tzinfo=UTC), datetime(2026, 11, 14, tzinfo=UTC))
    assert [(p["ts"], p["requests"]) for p in got] == [("2026-11-12T23:00:00Z", 2), ("2026-11-13T00:00:00Z", 1)]
    assert step == 360


def test_history_bounds_are_inclusive():
    ms, store, _ = make()
    ms.load_today()
    ms.add([minute(DAY1, 9, m) for m in range(5)])
    got, _, _ = ms.history(
        datetime(2026, 11, 12, 9, 1, tzinfo=UTC), datetime(2026, 11, 12, 9, 3, tzinfo=UTC)
    )
    assert [p["ts"] for p in got] == [f"2026-11-12T09:0{m}:00Z" for m in (1, 2, 3)]


def test_history_missing_day_file_is_empty():
    ms, _, _ = make()
    ms.load_today()
    got, step, _ = ms.history(datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 11, 5, tzinfo=UTC))
    assert (got, step) == ([], 60 * 12)


def test_history_broken_line_skipped_in_past_day():
    ms, store, _ = make()
    store.put(path("2026-11-05"), json.dumps(minute("2026-11-05", 1, 0)) + "\nnot json\n")
    got, _, _ = ms.history(datetime(2026, 11, 5, tzinfo=UTC), datetime(2026, 11, 5, 23, tzinfo=UTC))
    assert len(got) == 1


def test_history_merges_to_limit():
    ms, store, _ = make()
    pts = [minute("2026-11-05", h, m) for h in range(24) for m in range(60)]
    store.put(path("2026-11-05"), "\n".join(json.dumps(p) for p in pts))
    got, step, _ = ms.history(datetime(2026, 11, 5, tzinfo=UTC), datetime(2026, 11, 6, tzinfo=UTC))
    assert len(got) <= MAX_POINTS
    assert step == 180
    assert sum(p["requests"] for p in got) == 1440


def test_history_accepts_31_days_and_rejects_32():
    ms, _, _ = make()
    start = datetime(2026, 10, 1, tzinfo=UTC)
    start = start + timedelta(hours=10)
    ms.history(start, start + timedelta(days=MAX_DAYS))
    with pytest.raises(ValueError):
        ms.history(start, start + timedelta(days=MAX_DAYS, seconds=1))


def test_history_rejects_since_after_until():
    ms, _, _ = make()
    with pytest.raises(ValueError):
        ms.history(datetime(2026, 11, 5, tzinfo=UTC), datetime(2026, 11, 4, tzinfo=UTC))


def test_history_reads_days_in_parallel_threads():
    barrier = threading.Barrier(2, timeout=5)
    names = set()

    class Spy(MemoryBlobStore):
        def get(self, p):
            barrier.wait()  # two reads must run at the same time
            names.add(threading.current_thread().name)
            return super().get(p)

    ms, _, _ = make(Spy())
    ms.history(datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC))
    assert len(names) == 2


def test_history_reports_failed_days_with_one_error(caplog):
    store = FailingGet()
    ms, _, _ = make(store)
    with caplog.at_level(logging.ERROR):
        got, _, failed = ms.history(datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC))
    assert got == []
    assert failed == ["2026-10-01", "2026-10-02", "2026-10-03"]
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1


def test_history_missing_day_is_not_failed():
    ms, store, _ = make()
    store.put(path("2026-10-02"), json.dumps(minute("2026-10-02", 1, 0)))
    got, _, failed = ms.history(datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC))
    assert len(got) == 1
    assert failed == []


def test_clean_past_day_read_by_load_today_leaves_memory():
    store = FailingGet()
    store.fail = False
    store.put(path(DAY1), json.dumps(minute(DAY1, 9, 0)) + "\n")
    ms, _, clock = make(store)
    ms.load_today()
    clock.move(days=1)
    ms.write(force=True)
    before = store.gets
    got, _, _ = ms.history(datetime(2026, 11, 12, tzinfo=UTC), datetime(2026, 11, 12, 23, tzinfo=UTC))
    assert store.gets == before + 1
    assert len(got) == 1


def test_point_added_during_put_of_past_day_stays(caplog):
    holder = {}

    class AddDuring(MemoryBlobStore):
        def put(self, p, data):
            if holder.get("go"):
                holder["go"] = False
                holder["ms"].add([minute(DAY1, 23, 59)])
            super().put(p, data)

    store = AddDuring()
    ms, _, clock = make(store, Clock("2026-11-12T23:58:00"))
    holder["ms"] = ms
    ms.load_today()
    ms.add([minute(DAY1, 23, 58)])
    clock.move(minutes=5)
    holder["go"] = True
    ms.write(force=True)
    assert len(lines(store, DAY1)) == 1
    ms.write(force=True)
    assert [p["ts"] for p in lines(store, DAY1)] == ["2026-11-12T23:58:00Z", "2026-11-12T23:59:00Z"]


# retain


def test_retain_removes_old_files_and_keeps_new():
    store = Counting()
    ms, _, _ = make(store, Clock("2026-11-12T10:00:00"), retention_days=30)
    for day in ("2026-10-01", "2026-10-12", "2026-10-13", DAY1):
        store.put(path(day), "")
    store.put(f"{ROOT}/metrics/readme.txt", "x")
    ms.retain()
    assert store.ls(f"{ROOT}/metrics") == [
        path("2026-10-13"), path(DAY1), f"{ROOT}/metrics/readme.txt",
    ]


def test_retain_lists_once_an_hour():
    store = Counting()
    ms, _, clock = make(store)
    ms.retain()
    ms.retain()
    assert store.lists == 1
    clock.move(minutes=59)
    ms.retain()
    assert store.lists == 1
    clock.move(minutes=1)
    ms.retain()
    assert store.lists == 2


def test_retain_survives_failed_delete(caplog):
    store = Counting()
    store.put(path("2026-01-01"), "")
    store.put(path("2026-01-02"), "")
    store.fail_delete = True
    ms, _, _ = make(store)
    with caplog.at_level(logging.ERROR):
        ms.retain()
    assert len(store.ls(f"{ROOT}/metrics")) == 2
    assert "no delete" in caplog.text


def test_retain_survives_failed_listing():
    class BadLs(MemoryBlobStore):
        def ls(self, prefix):
            raise OSError("no listing")

    ms, _, _ = make(BadLs())
    ms.retain()


def test_write_never_raises_on_read_error():
    store = FailingGet()
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    ms.write(force=True)
    store.fail = False
    assert store.ls(f"{ROOT}/metrics") == []


def test_write_never_raises_on_unexpected_error(caplog, monkeypatch):
    ms, store, _ = make()
    ms.add([minute(DAY1, 9, 0)])

    def boom(day):
        raise RuntimeError("surprise")

    monkeypatch.setattr(ms, "_read_again", boom)
    with caplog.at_level(logging.ERROR):
        ms.write(force=True)
    assert "surprise" in caplog.text
    assert store.ls(f"{ROOT}/metrics") == []


def test_write_keeps_a_point_that_another_writer_put_after_load_today():
    ms, store, clock = make()
    ms.load_today()
    # An old container writes its last point after this one read the file.
    store.put(path(DAY1), json.dumps(minute(DAY1, 9, 58, requests=4)) + "\n")
    ms.add([minute(DAY1, 9, 59)])
    ms.write(force=True)
    lines = [json.loads(line) for line in store.get(path(DAY1)).decode().splitlines()]
    assert [(p["ts"], p["requests"]) for p in lines] == [("2026-11-12T09:58:00Z", 4), ("2026-11-12T09:59:00Z", 1)]


def test_write_memory_wins_over_the_file_for_the_same_ts():
    ms, store, clock = make()
    ms.load_today()
    store.put(path(DAY1), json.dumps(minute(DAY1, 9, 59, requests=4)) + "\n")
    ms.add([minute(DAY1, 9, 59, requests=7)])
    ms.write(force=True)
    lines = [json.loads(line) for line in store.get(path(DAY1)).decode().splitlines()]
    assert [(p["ts"], p["requests"]) for p in lines] == [("2026-11-12T09:59:00Z", 7)]


def test_write_skips_the_put_when_the_read_before_it_fails():
    store = FailingGet()
    store.fail = False
    ms, _, _ = make(store)
    ms.load_today()
    ms.add([minute(DAY1, 9, 0)])
    store.fail = True
    ms.write(force=True)
    assert path(DAY1) not in store.ls(f"{ROOT}/metrics")
    store.fail = False
    ms.write(force=True)
    assert [json.loads(line)["ts"] for line in store.get(path(DAY1)).decode().splitlines()] == ["2026-11-12T09:00:00Z"]
