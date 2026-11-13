from datetime import datetime, timedelta, timezone

import pytest

from llm_service.gateway import load as load_module
from llm_service.gateway.load import POINTS_KEPT, SAMPLES_KEPT, LoadMonitor, percentile

START = datetime(2026, 11, 12, 10, 15, 0, tzinfo=timezone.utc)

POINT_FIELDS = {
    "ts", "requests", "failed_requests", "in_flight_max", "waiting_max",
    "prompt_tokens", "completion_tokens", "ttft_ms_p50", "ttft_ms_p95",
    "duration_ms_p50", "duration_ms_p95", "gpu_memory_used_max", "gpu_memory_total",
}


class Env:
    def __init__(self):
        self.now = START
        self.monitor = LoadMonitor(lambda: self.now)

    def advance(self, seconds):
        self.now = self.now + timedelta(seconds=seconds)

    def sample(self, running=0.0, waiting=0.0, prompt=0.0, completion=0.0, gpu=None):
        engine = {
            "running": running,
            "waiting": waiting,
            "prompt_tokens_total": prompt,
            "completion_tokens_total": completion,
        }
        return self.monitor.add_sample(engine, gpu)

    def record(self, **kw):
        rec = {
            "endpoint": "/v1/chat/completions",
            "status": 200,
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "duration_ms": 1000,
            "ttft_ms": 100,
        }
        rec.update(kw)
        self.monitor.observe(rec)


@pytest.fixture
def env():
    return Env()


def test_constants():
    assert POINTS_KEPT == 60
    assert SAMPLES_KEPT == 12


def test_percentile_empty():
    assert percentile([], 0.5) is None


def test_percentile_one_value():
    assert percentile([7], 0.5) == 7
    assert percentile([7], 0.95) == 7


def test_percentile_two_values():
    assert percentile([9, 3], 0.5) == 3
    assert percentile([9, 3], 0.95) == 9


def test_percentile_hundred_values():
    values = list(range(100, 0, -1))
    assert percentile(values, 0.5) == 50
    assert percentile(values, 0.95) == 95


def test_begin_and_end(env):
    assert env.monitor.in_flight() == 0
    env.monitor.begin()
    env.monitor.begin()
    assert env.monitor.in_flight() == 2
    env.monitor.end()
    assert env.monitor.in_flight() == 1


def test_end_at_zero_stays_zero(env):
    env.monitor.end()
    assert env.monitor.in_flight() == 0
    env.monitor.begin()
    assert env.monitor.in_flight() == 1


def test_sample_fields_and_ts(env):
    env.now = START + timedelta(seconds=7, microseconds=900000)
    env.monitor.begin()
    env.sample(running=2.0, waiting=1.0, gpu=(100, 200))
    now = env.monitor.live()["now"]
    assert now == {
        "ts": "2026-11-12T10:15:07Z",
        "in_flight": 1,
        "running": 2.0,
        "waiting": 1.0,
        "prompt_tokens_per_s": None,
        "completion_tokens_per_s": None,
        "gpu_memory_used": 100,
        "gpu_memory_total": 200,
    }


def test_sample_ts_converts_to_utc(env):
    env.now = datetime(2026, 11, 12, 12, 0, 5, tzinfo=timezone(timedelta(hours=2)))
    env.sample()
    assert env.monitor.live()["now"]["ts"] == "2026-11-12T10:00:05Z"


def test_rates_between_samples(env):
    env.sample(prompt=100.0, completion=50.0)
    env.advance(5)
    env.sample(prompt=200.0, completion=75.0)
    now = env.monitor.live()["now"]
    assert now["prompt_tokens_per_s"] == pytest.approx(20.0)
    assert now["completion_tokens_per_s"] == pytest.approx(5.0)


def test_rate_null_for_null_counter_then_recovers_after_next(env):
    env.sample(prompt=100.0, completion=50.0)
    env.advance(5)
    env.monitor.add_sample(
        {"running": 1.0, "waiting": 0.0, "prompt_tokens_total": None,
         "completion_tokens_total": 60.0}, None)
    now = env.monitor.live()["now"]
    assert now["prompt_tokens_per_s"] is None
    assert now["completion_tokens_per_s"] == pytest.approx(2.0)
    env.advance(5)
    env.sample(prompt=300.0, completion=70.0)
    now = env.monitor.live()["now"]
    assert now["prompt_tokens_per_s"] is None
    assert now["completion_tokens_per_s"] == pytest.approx(2.0)
    env.advance(5)
    env.sample(prompt=350.0, completion=80.0)
    assert env.monitor.live()["now"]["prompt_tokens_per_s"] == pytest.approx(10.0)


def test_rate_null_when_counter_went_down(env):
    env.sample(prompt=1000.0, completion=500.0)
    env.advance(5)
    env.sample(prompt=10.0, completion=600.0)
    now = env.monitor.live()["now"]
    assert now["prompt_tokens_per_s"] is None
    assert now["completion_tokens_per_s"] == pytest.approx(20.0)
    env.advance(5)
    env.sample(prompt=60.0, completion=600.0)
    assert env.monitor.live()["now"]["prompt_tokens_per_s"] == pytest.approx(10.0)


def test_null_engine_values_and_gpu(env):
    env.monitor.add_sample(
        {"running": None, "waiting": None, "prompt_tokens_total": None,
         "completion_tokens_total": None}, None)
    now = env.monitor.live()["now"]
    assert now["running"] is None
    assert now["waiting"] is None
    assert now["gpu_memory_used"] is None
    assert now["gpu_memory_total"] is None


def test_point_for_minute_with_records_and_samples(env):
    env.monitor.begin()
    env.sample(running=1.0, waiting=2.0, gpu=(1000, 2000))
    env.advance(5)
    env.monitor.begin()
    env.monitor.begin()
    env.sample(running=3.0, waiting=5.0, gpu=(1500, 2000))
    env.advance(5)
    env.monitor.end()
    env.sample(running=2.0, waiting=0.0, gpu=(1200, None))
    for ttft, dur in [(100, 1000), (200, 2000), (300, 3000), (400, 4000)]:
        env.record(ttft_ms=ttft, duration_ms=dur, prompt_tokens=10, completion_tokens=5)
    env.record(status=500, ttft_ms=None, duration_ms=5000, prompt_tokens=7, completion_tokens=None)
    env.now = START + timedelta(seconds=60)
    points = env.sample(running=0.0)
    assert points == [{
        "ts": "2026-11-12T10:15:00Z",
        "requests": 5,
        "failed_requests": 1,
        "in_flight_max": 3,
        "waiting_max": 5.0,
        "prompt_tokens": 47,
        "completion_tokens": 20,
        "ttft_ms_p50": 200,
        "ttft_ms_p95": 400,
        "duration_ms_p50": 2000,
        "duration_ms_p95": 4000,
        "gpu_memory_used_max": 1500,
        "gpu_memory_total": 2000,
    }]
    assert set(points[0]) == POINT_FIELDS


def test_minute_with_one_request(env):
    env.sample()
    env.record(ttft_ms=50, duration_ms=900)
    env.now = START + timedelta(seconds=65)
    (point,) = env.sample()
    assert point["requests"] == 1
    assert point["ttft_ms_p50"] == 50
    assert point["ttft_ms_p95"] == 50
    assert point["duration_ms_p50"] == 900
    assert point["duration_ms_p95"] == 900


def test_minute_with_samples_and_no_request(env):
    env.sample(waiting=1.0)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 0
    assert point["failed_requests"] == 0
    assert point["prompt_tokens"] == 0
    assert point["completion_tokens"] == 0
    assert point["ttft_ms_p50"] is None
    assert point["ttft_ms_p95"] is None
    assert point["duration_ms_p50"] is None
    assert point["duration_ms_p95"] is None
    assert point["waiting_max"] == 1.0
    assert point["in_flight_max"] == 0
    assert point["gpu_memory_used_max"] is None
    assert point["gpu_memory_total"] is None


def test_minute_with_records_and_no_sample_gives_point(env):
    env.record()
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 1
    assert point["in_flight_max"] is None
    assert point["waiting_max"] is None


def test_failed_request_threshold(env):
    env.record(status=399)
    env.record(status=400)
    env.record(status=429)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 3
    assert point["failed_requests"] == 2


def test_record_not_from_v1_is_ignored(env):
    env.record(endpoint="/admin/model")
    env.monitor.observe({"endpoint": "/v1/models"})
    env.now = START + timedelta(seconds=60)
    assert env.sample() == []


def test_bad_records_do_not_raise(env):
    env.monitor.observe({})
    env.monitor.observe({"endpoint": None, "status": 200})
    env.monitor.observe({"endpoint": "/v1/x", "status": "bad"})
    env.monitor.observe({"endpoint": "/v1/x", "status": 200, "prompt_tokens": "x",
                         "duration_ms": "y", "ttft_ms": object()})
    env.monitor.observe(None)
    env.monitor.observe("text")
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 1
    assert point["failed_requests"] == 0
    assert point["prompt_tokens"] == 0
    assert point["ttft_ms_p50"] is None
    assert point["duration_ms_p50"] is None


def test_missing_ttft_key_and_null_values(env):
    env.monitor.observe({"endpoint": "/v1/completions", "status": 200,
                         "prompt_tokens": None, "completion_tokens": None,
                         "duration_ms": None})
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 1
    assert point["prompt_tokens"] == 0
    assert point["ttft_ms_p50"] is None
    assert point["duration_ms_p50"] is None


def test_point_comes_once_at_first_sample_of_later_minute(env):
    env.sample()
    env.advance(5)
    assert env.sample() == []
    env.now = START + timedelta(seconds=60)
    assert len(env.sample()) == 1
    env.advance(5)
    assert env.sample() == []
    assert len(env.monitor.live()["points"]) == 1


def test_gap_of_several_minutes_gives_no_points_for_gap(env):
    env.sample()
    env.now = START + timedelta(minutes=5)
    points = env.sample()
    assert [p["ts"] for p in points] == ["2026-11-12T10:15:00Z"]
    env.now = START + timedelta(minutes=6)
    points = env.sample()
    assert [p["ts"] for p in points] == ["2026-11-12T10:20:00Z"]


def test_several_minutes_returned_oldest_first(env):
    env.sample()
    env.record()
    env.now = START + timedelta(minutes=2, seconds=30)
    env.record()
    env.now = START + timedelta(minutes=3)
    points = env.sample()
    assert [p["ts"] for p in points] == ["2026-11-12T10:15:00Z", "2026-11-12T10:17:00Z"]


def test_record_counts_for_minute_of_observe_call(env):
    env.now = START + timedelta(seconds=59)
    env.record()
    env.now = START + timedelta(seconds=60)
    env.record()
    env.now = START + timedelta(seconds=120)
    points = env.sample()
    assert [(p["ts"], p["requests"]) for p in points] == [
        ("2026-11-12T10:15:00Z", 1), ("2026-11-12T10:16:00Z", 1)]


def test_gpu_total_is_last_non_null_of_minute(env):
    env.sample(gpu=(1, 10))
    env.advance(5)
    env.sample(gpu=(2, 20))
    env.advance(5)
    env.sample(gpu=None)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["gpu_memory_total"] == 20
    assert point["gpu_memory_used_max"] == 2


def test_keeps_at_most_sixty_points(env):
    for minute in range(70):
        env.now = START + timedelta(minutes=minute)
        env.sample()
    env.now = START + timedelta(minutes=70)
    env.sample()
    points = env.monitor.live()["points"]
    assert len(points) == POINTS_KEPT
    assert points[-1]["ts"] == "2026-11-12T11:24:00Z"
    assert points[0]["ts"] == "2026-11-12T10:25:00Z"


def test_keeps_at_most_twelve_samples(env):
    for _ in range(30):
        env.sample()
        env.advance(1)
    assert len(env.monitor._samples) == SAMPLES_KEPT


def test_live_before_first_sample(env):
    env.monitor.begin()
    live = env.monitor.live()
    assert live["points"] == []
    assert live["now"] == {
        "ts": None, "in_flight": None, "running": None, "waiting": None,
        "prompt_tokens_per_s": None, "completion_tokens_per_s": None,
        "gpu_memory_used": None, "gpu_memory_total": None,
    }


def test_live_after_several_minutes(env):
    for minute in range(3):
        env.now = START + timedelta(minutes=minute)
        env.sample(running=float(minute))
    live = env.monitor.live()
    assert live["now"]["ts"] == "2026-11-12T10:17:00Z"
    assert live["now"]["running"] == 2.0
    assert [p["ts"] for p in live["points"]] == ["2026-11-12T10:15:00Z", "2026-11-12T10:16:00Z"]


def test_late_record_for_emitted_minute_counts_for_open_minute(env):
    env.sample()
    env.now = START + timedelta(seconds=60)
    env.sample()
    # The clock goes back into the emitted minute 10:15 while 10:16 is open.
    env.now = START + timedelta(seconds=59)
    env.record()
    env.now = START + timedelta(seconds=120)
    points = env.sample()
    assert [(p["ts"], p["requests"]) for p in points] == [("2026-11-12T10:16:00Z", 1)]
    stamps = [p["ts"] for p in env.monitor.live()["points"]]
    assert len(stamps) == len(set(stamps))


def test_clock_error_in_observe_does_not_raise():
    def broken():
        raise RuntimeError("clock")

    LoadMonitor(broken).observe({"endpoint": "/v1/x", "status": 200})


def test_pending_minutes_are_bounded(env):
    for minute in range(200):
        env.now = START + timedelta(minutes=minute)
        env.record()
    assert len(env.monitor._minutes) == load_module.MINUTES_PENDING == 61
    assert min(env.monitor._minutes) == START + timedelta(minutes=139)


def test_values_of_one_minute_are_bounded(env, monkeypatch):
    monkeypatch.setattr(load_module, "VALUES_KEPT", 3)
    for i in range(5):
        env.record(ttft_ms=i, duration_ms=i)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 5
    assert point["prompt_tokens"] == 50
    assert point["ttft_ms_p95"] == 2
    assert point["duration_ms_p50"] == 1


def test_values_limit_is_one_hundred_thousand():
    assert load_module.VALUES_KEPT == 100_000


def test_percentiles_use_only_good_requests(env):
    env.record(ttft_ms=300, duration_ms=2000)
    for _ in range(9):
        env.record(status=429, ttft_ms=None, duration_ms=1)
    env.record(status=502, ttft_ms=5, duration_ms=1)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 11
    assert point["failed_requests"] == 10
    assert point["duration_ms_p50"] == 2000
    assert point["duration_ms_p95"] == 2000
    assert point["ttft_ms_p50"] == 300


def test_percentiles_are_null_for_a_minute_of_failed_requests(env):
    env.record(status=500, ttft_ms=7, duration_ms=9)
    env.now = START + timedelta(seconds=60)
    (point,) = env.sample()
    assert point["requests"] == 1 and point["failed_requests"] == 1
    assert point["ttft_ms_p50"] is None and point["duration_ms_p95"] is None


def test_samples_of_one_minute_are_bounded_after_a_backward_clock_step(env):
    env.sample()
    env.now = START + timedelta(seconds=60)
    env.sample()
    # The clock steps back into the emitted minute 10:15, so each sample goes to the open minute 10:16.
    env.now = START + timedelta(seconds=1)
    env.sample(gpu=(9000, 10000))
    for _ in range(40):
        env.advance(1)
        env.sample(gpu=(1, 10000))
    (minute,) = env.monitor._minutes.values()
    assert len(minute.samples) == load_module.SAMPLES_PER_MINUTE == 24
    env.now = START + timedelta(seconds=120)
    (point,) = env.sample()
    assert point["ts"] == "2026-11-12T10:16:00Z"
    assert point["gpu_memory_used_max"] == 1  # the oldest sample, with 9000, is dropped
