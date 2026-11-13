"""Readers for the engine metrics and the GPU memory.

`EngineReader` reads the Prometheus text of the vLLM metrics endpoint.
`GpuReader` reads the memory numbers of `nvidia-smi`. A failure of a reader
never raises. The number is None then.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import subprocess
from collections.abc import Callable

import httpx

logger = logging.getLogger(__name__)

# Known names for each number. The first name that the text has wins.
METRIC_NAMES = {
    "running": ("vllm:num_requests_running",),
    "waiting": ("vllm:num_requests_waiting",),
    "prompt_tokens_total": ("vllm:prompt_tokens_total", "vllm:prompt_tokens"),
    "completion_tokens_total": ("vllm:generation_tokens_total", "vllm:generation_tokens"),
    "kv_cache_use": ("vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc"),
}

# A number of these keys is a share. It takes the maximum over the label sets, not the sum.
SHARE_KEYS = frozenset({"kv_cache_use"})
# A share above this value is a percent. Some vLLM versions report 0 to 100.
PERCENT_ABOVE = 1.5

ENGINE_PREFIX = "vllm:"

GPU_COMMAND = [
    "nvidia-smi",
    "--query-gpu=memory.used,memory.total",
    "--format=csv,noheader,nounits",
]

# A sample line: name, optional label set, value, optional timestamp.
# A quoted label value can hold spaces, commas, braces and escaped characters.
_SAMPLE = re.compile(
    r"""^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)
        (?:\{(?:[^"}]|"(?:[^"\\]|\\.)*")*\})?
        [ \t]+(?P<value>\S+)
        (?:[ \t]+-?\d+)?
        [ \t]*$""",
    re.VERBOSE,
)

_GPU_LINE = re.compile(r"^\s*(\d+)\s*,\s*(\d+)\s*$")


def _finite(token: str) -> float | None:
    if "_" in token:
        return None
    try:
        value = float(token)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def parse_metrics(text: str) -> tuple[dict[str, float | None], list[str]]:
    """Return the five numbers and the sorted `vllm:` names of the text.

    A number is the sum of the samples of one name over its label sets. A share
    takes the maximum instead, as a fraction from 0 to 1. A number is None when
    no known name has a finite sample. Comment lines and bad lines are ignored.
    """
    sums: dict[str, float] = {}
    maxima: dict[str, float] = {}
    seen: set[str] = set()
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE.match(line)
        if match is None:
            continue
        name = match.group("name")
        if name.startswith(ENGINE_PREFIX):
            seen.add(name)
        value = _finite(match.group("value"))
        if value is not None:
            sums[name] = sums.get(name, 0.0) + value
            maxima[name] = max(maxima.get(name, value), value)
    numbers: dict[str, float | None] = {}
    for key, names in METRIC_NAMES.items():
        if key in SHARE_KEYS:
            share = next((maxima[n] for n in names if n in maxima), None)
            numbers[key] = None if share is None else _fraction(share)
            continue
        total = next((sums[n] for n in names if n in sums), None)
        numbers[key] = total if total is not None and math.isfinite(total) else None
    return numbers, sorted(seen)


def _fraction(value: float) -> float:
    """A share as a fraction from 0 to 1. A value above PERCENT_ABOVE is a percent."""
    if value > PERCENT_ABOVE:
        value = value / 100.0
    return min(max(value, 0.0), 1.0)


def answer_summary(text: str, limit: int = 10) -> tuple[int, list[str]]:
    """Return the count of sample lines of the text and its first `limit` metric names, sorted."""
    count = 0
    names: set[str] = set()
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE.match(line)
        if match is None:
            continue
        count += 1
        names.add(match.group("name"))
    return count, sorted(names)[:limit]


def parse_gpu_memory(text: str) -> tuple[int, int] | None:
    """Return (used, total) in MiB, summed over the GPUs.

    The text is the output of `nvidia-smi` with one line for each GPU. An empty
    text or a bad line gives None.
    """
    used = 0
    total = 0
    count = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        match = _GPU_LINE.match(line)
        if match is None:
            return None
        used += int(match.group(1))
        total += int(match.group(2))
        count += 1
    return (used, total) if count else None


def metrics_client(timeout: float = 2.0) -> httpx.AsyncClient:
    """Return a client for the metrics endpoint with its own small pool.

    The proxy client can have each connection busy with a stream. A read with its own
    pool does not wait for a free connection of the proxy.
    """
    return httpx.AsyncClient(timeout=timeout, limits=httpx.Limits(max_connections=2))


class EngineReader:
    """Read the five load numbers from the metrics endpoint of the engine.

    Give it a client of its own, not the client of the proxy. `metrics_client` builds one.
    """

    def __init__(self, client: httpx.AsyncClient, base_url: str, timeout: float = 2.0) -> None:
        self._client = client
        self._url = base_url.rstrip("/") + "/metrics"
        self._timeout = timeout
        self._failing = False
        self._missing: frozenset[str] = frozenset()

    async def read(self) -> dict[str, float | None]:
        """Return the five numbers. Each one is None on a failure."""
        try:
            # The httpx timeout is a limit for each phase. The outer limit covers the total.
            response = await asyncio.wait_for(
                self._client.get(self._url, timeout=self._timeout, follow_redirects=True), self._timeout
            )
            response.raise_for_status()
            text = response.text
        except Exception as exc:
            if not self._failing:
                self._failing = True
                logger.warning("The engine metrics endpoint failed: %r", exc)
            return {key: None for key in METRIC_NAMES}
        if self._failing:
            self._failing = False
            logger.info("The engine metrics endpoint works again.")
        numbers, names = parse_metrics(text)
        missing = frozenset(key for key, value in numbers.items() if value is None)
        if missing != self._missing:
            self._missing = missing
            if missing and names:
                logger.warning(
                    "No known metric name for: %s. The endpoint saw these %s names: %s",
                    ", ".join(sorted(missing)),
                    ENGINE_PREFIX,
                    ", ".join(names),
                )
            elif missing:
                count, some = answer_summary(text)
                logger.warning(
                    "No known metric name for: %s. The endpoint saw no %s name in %d sample lines. "
                    "Some names of the answer: %s",
                    ", ".join(sorted(missing)),
                    ENGINE_PREFIX,
                    count,
                    ", ".join(some) or "no names",
                )
        return numbers


class GpuReader:
    """Read the GPU memory with `nvidia-smi`. The call blocks, so use a thread."""

    def __init__(
        self,
        run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
        timeout: float = 5.0,
    ) -> None:
        self._run = run
        self._timeout = timeout
        self._failing = False
        self._no_binary = False

    def read(self) -> tuple[int, int] | None:
        """Return (used, total) in MiB summed over the GPUs, or None."""
        if self._no_binary:
            return None
        try:
            done = self._run(list(GPU_COMMAND), capture_output=True, text=True, timeout=self._timeout)
        except FileNotFoundError:
            self._no_binary = True
            logger.warning("The nvidia-smi binary is missing. GPU memory is not read.")
            return None
        except Exception as exc:
            return self._fail(f"nvidia-smi did not run: {exc!r}")
        if done.returncode != 0:
            return self._fail(f"nvidia-smi exited with code {done.returncode}")
        result = parse_gpu_memory(done.stdout or "")
        if result is None:
            return self._fail("nvidia-smi gave no memory numbers")
        if self._failing:
            self._failing = False
            logger.info("nvidia-smi works again.")
        return result

    def _fail(self, reason: str) -> None:
        if not self._failing:
            self._failing = True
            logger.warning("The GPU memory read failed: %s", reason)
        return None
