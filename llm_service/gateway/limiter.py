"""Per-user limits for requests, tokens and parallel requests.

The limiter keeps its state in memory. It runs on the event loop only and needs no lock.
"""

import math
import time
from collections import deque
from typing import Callable

WINDOW_SECONDS = 60.0
SWEEP_SECONDS = 1.0


class LimitExceeded(Exception):
    """A request reached a limit. The code is the name of the limit."""

    def __init__(self, code: str, limit: int, retry_after: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.limit = limit
        self.retry_after = retry_after
        self.message = message


class _UserState:
    def __init__(self) -> None:
        self.window: deque[float] = deque()
        self.in_flight = 0


def _seconds_up(value: float) -> int:
    return max(1, math.ceil(value))


class Limiter:
    def __init__(
        self,
        tokens_today: Callable[[str], int],
        seconds_to_midnight: Callable[[], float],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._tokens_today = tokens_today
        self._seconds_to_midnight = seconds_to_midnight
        self._clock = clock
        self._users: dict[str, _UserState] = {}
        self._last_sweep: float | None = None

    def _trim(self, user: str, now: float) -> _UserState | None:
        """Drop old entries of the window. Forget the user when nothing is left."""
        state = self._users.get(user)
        if state is None:
            return None
        edge = now - WINDOW_SECONDS
        while state.window and state.window[0] <= edge:
            state.window.popleft()
        if not state.window and state.in_flight == 0:
            del self._users[user]
            return None
        return state

    def _sweep(self, now: float) -> None:
        self._last_sweep = now
        for name in list(self._users):
            self._trim(name, now)

    def acquire(self, user: str, limits: dict) -> None:
        now = self._clock()
        if self._last_sweep is None or now - self._last_sweep >= SWEEP_SECONDS:
            self._sweep(now)
        state = self._trim(user, now)
        in_flight = state.in_flight if state else 0
        recent = len(state.window) if state else 0

        limit = limits.get("parallel_requests")
        if limit is not None and in_flight >= limit:
            raise LimitExceeded(
                "parallel_requests", limit, 1, f"You reached the limit of {limit} parallel requests."
            )
        limit = limits.get("requests_per_minute")
        if limit is not None and recent >= limit:
            if state is not None and state.window:
                wait = _seconds_up(state.window[0] + WINDOW_SECONDS - now)
            else:
                wait = int(WINDOW_SECONDS)
            raise LimitExceeded(
                "requests_per_minute", limit, wait, f"You reached the limit of {limit} requests in one minute."
            )
        limit = limits.get("tokens_per_day")
        if limit is not None and self._tokens_today(user) >= limit:
            raise LimitExceeded(
                "tokens_per_day",
                limit,
                _seconds_up(self._seconds_to_midnight()),
                f"You reached the limit of {limit} tokens in one day.",
            )

        if state is None:
            state = self._users[user] = _UserState()
        state.window.append(now)
        state.in_flight += 1

    def release(self, user: str) -> None:
        state = self._users.get(user)
        if state is None:
            return
        state.in_flight = max(0, state.in_flight - 1)
        self._trim(user, self._clock())

    def use(self, user: str) -> dict:
        state = self._trim(user, self._clock())
        return {
            "requests_last_minute": len(state.window) if state else 0,
            "tokens_today": self._tokens_today(user),
            "parallel_requests": state.in_flight if state else 0,
        }

    def active_users(self) -> list[str]:
        self._sweep(self._clock())
        return sorted(self._users)
