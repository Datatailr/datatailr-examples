"""Live load numbers: a sample each 5 seconds and one point for each minute.

The monitor keeps its state in memory. One lock guards the state, and no method does I/O.
The methods run on the event loop and in threads.
"""

import math
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Callable

POINTS_KEPT = 60
SAMPLES_KEPT = 12
MINUTES_PENDING = 61
VALUES_KEPT = 100_000

_NO_SAMPLE = {
    "ts": None,
    "in_flight": None,
    "running": None,
    "waiting": None,
    "prompt_tokens_per_s": None,
    "completion_tokens_per_s": None,
    "gpu_memory_used": None,
    "gpu_memory_total": None,
}


def percentile(values: list[int], fraction: float) -> int | None:
    """Nearest rank on the sorted values. None for an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    index = max(math.ceil(fraction * len(ordered)) - 1, 0)
    return ordered[min(index, len(ordered) - 1)]


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _minute_start(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _whole(value) -> int | None:
    """An int from a record field, or None for a missing or bad value."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _maximum(values: list):
    present = [v for v in values if v is not None]
    return max(present) if present else None


class _Minute:
    """The records and samples of one minute."""

    def __init__(self) -> None:
        self.requests = 0
        self.failed = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.ttft: list[int] = []
        self.duration: list[int] = []
        self.samples: list[dict] = []

    def point(self, start: datetime) -> dict:
        totals = [s["gpu_memory_total"] for s in self.samples if s["gpu_memory_total"] is not None]
        return {
            "ts": _iso(start),
            "requests": self.requests,
            "failed_requests": self.failed,
            "in_flight_max": _maximum([s["in_flight"] for s in self.samples]),
            "waiting_max": _maximum([s["waiting"] for s in self.samples]),
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "ttft_ms_p50": percentile(self.ttft, 0.5),
            "ttft_ms_p95": percentile(self.ttft, 0.95),
            "duration_ms_p50": percentile(self.duration, 0.5),
            "duration_ms_p95": percentile(self.duration, 0.95),
            "gpu_memory_used_max": _maximum([s["gpu_memory_used"] for s in self.samples]),
            "gpu_memory_total": totals[-1] if totals else None,
        }


class LoadMonitor:
    """Builds samples and one point for each minute from engine numbers and audit records."""

    def __init__(self, now: Callable[[], datetime]) -> None:
        self._now = now
        self._lock = threading.Lock()
        self._in_flight = 0
        self._minutes: dict[datetime, _Minute] = {}
        self._samples: deque[dict] = deque(maxlen=SAMPLES_KEPT)
        self._points: deque[dict] = deque(maxlen=POINTS_KEPT)
        self._last_time: datetime | None = None
        self._emitted: datetime | None = None
        self._last_counters: tuple[float | None, float | None] = (None, None)

    def begin(self) -> None:
        """A request goes to the engine."""
        with self._lock:
            self._in_flight += 1

    def end(self) -> None:
        """That request ended. The count never goes below 0."""
        with self._lock:
            self._in_flight = max(self._in_flight - 1, 0)

    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    def observe(self, record: dict) -> None:
        """Count an audit record of a /v1/ request. Never raises."""
        try:
            endpoint = record.get("endpoint")
            status = _whole(record.get("status"))
            if not isinstance(endpoint, str) or not endpoint.startswith("/v1/") or status is None:
                return
            prompt = _whole(record.get("prompt_tokens"))
            completion = _whole(record.get("completion_tokens"))
            ttft = _whole(record.get("ttft_ms"))
            duration = _whole(record.get("duration_ms"))
            with self._lock:
                minute = self._open_minute(self._now())
                minute.requests += 1
                minute.prompt_tokens += prompt or 0
                minute.completion_tokens += completion or 0
                if status >= 400:
                    # A refused or failed request ends fast. It stays out of the latency percentiles.
                    minute.failed += 1
                    return
                if ttft is not None and len(minute.ttft) < VALUES_KEPT:
                    minute.ttft.append(ttft)
                if duration is not None and len(minute.duration) < VALUES_KEPT:
                    minute.duration.append(duration)
        except Exception:
            return

    def _open_minute(self, moment: datetime) -> _Minute:
        """The pending minute for a moment. Call it with the lock held.

        A moment in a minute that was emitted already counts for the current open minute.
        The number of pending minutes stays at MINUTES_PENDING or below.
        """
        start = _minute_start(moment)
        if self._emitted is not None and start <= self._emitted:
            start = _minute_start(self._last_time) if self._last_time else self._emitted + timedelta(minutes=1)
            start = max(start, self._emitted + timedelta(minutes=1))
        if start not in self._minutes:
            self._minutes[start] = _Minute()
            while len(self._minutes) > MINUTES_PENDING:
                del self._minutes[min(self._minutes)]
        return self._minutes[start]

    @staticmethod
    def _rate(previous: float | None, current: float | None, seconds: float) -> float | None:
        if previous is None or current is None or seconds <= 0 or current < previous:
            return None
        return (current - previous) / seconds

    def add_sample(self, engine: dict[str, float | None], gpu: tuple[int, int] | None) -> list[dict]:
        """Take a sample. Return the points of the minutes that ended since the last call."""
        counters = (engine.get("prompt_tokens_total"), engine.get("completion_tokens_total"))
        with self._lock:
            moment = self._now()
            if self._last_time is None:
                rates: tuple[float | None, float | None] = (None, None)
            else:
                seconds = (moment - self._last_time).total_seconds()
                rates = (
                    self._rate(self._last_counters[0], counters[0], seconds),
                    self._rate(self._last_counters[1], counters[1], seconds),
                )
            self._last_time = moment
            self._last_counters = counters
            sample = {
                "ts": _iso(moment),
                "in_flight": self._in_flight,
                "running": engine.get("running"),
                "waiting": engine.get("waiting"),
                "prompt_tokens_per_s": rates[0],
                "completion_tokens_per_s": rates[1],
                "gpu_memory_used": gpu[0] if gpu else None,
                "gpu_memory_total": gpu[1] if gpu else None,
            }
            self._samples.append(sample)
            current = _minute_start(moment)
            self._open_minute(moment).samples.append(sample)
            finished = []
            for start in sorted(s for s in self._minutes if s < current):
                point = self._minutes.pop(start).point(start)
                self._emitted = start
                self._points.append(point)
                finished.append(point)
            return finished

    def live(self) -> dict:
        """The last sample and the last points."""
        with self._lock:
            now = dict(self._samples[-1]) if self._samples else dict(_NO_SAMPLE)
            return {"now": now, "points": [dict(p) for p in self._points]}
