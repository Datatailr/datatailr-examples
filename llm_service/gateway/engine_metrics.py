"""Readers for the engine metrics and the GPU memory.

`EngineReader` reads the Prometheus text of the vLLM metrics endpoint.
`GpuReader` reads the memory numbers of `nvidia-smi`. A failure of a reader
never raises. The number is None then.
"""

from __future__ import annotations

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
}

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
    """Return the four numbers and the sorted `vllm:` names of the text.

    A number is the sum of the samples of one name over its label sets. It is
    None when no known name has a finite sample. Comment lines and bad lines
    are ignored.
    """
    sums: dict[str, float] = {}
    seen: set[str] = set()
    for raw in text.splitlines():
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
    numbers: dict[str, float | None] = {}
    for key, names in METRIC_NAMES.items():
        numbers[key] = next((sums[n] for n in names if n in sums), None)
    return numbers, sorted(seen)


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


class EngineReader:
    """Read the four load numbers from the metrics endpoint of the engine."""

    def __init__(self, client: httpx.AsyncClient, base_url: str, timeout: float = 2.0) -> None:
        self._client = client
        self._url = base_url.rstrip("/") + "/metrics"
        self._timeout = timeout
        self._failing = False
        self._missing: frozenset[str] = frozenset()

    async def read(self) -> dict[str, float | None]:
        """Return the four numbers. Each one is None on a failure."""
        try:
            response = await self._client.get(self._url, timeout=self._timeout)
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
            if missing:
                logger.warning(
                    "No known metric name for: %s. The endpoint saw these %s names: %s",
                    ", ".join(sorted(missing)),
                    ENGINE_PREFIX,
                    ", ".join(names) or "none",
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
            done = self._run(GPU_COMMAND, capture_output=True, text=True, timeout=self._timeout)
        except FileNotFoundError:
            self._no_binary = True
            logger.warning("The nvidia-smi binary is missing. GPU memory is not read.")
            return None
        except (OSError, subprocess.SubprocessError) as exc:
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
