"""The GPU memory check: the parse of memory sizes and the rules of the answer."""

from __future__ import annotations

import math
import re
from fractions import Fraction

WEIGHTS_MARGIN = 1.2  # covers the working memory of the engine for a short context

_SIZE = re.compile(r"(\d+(?:\.\d+)?)\s*([tgm])?(?:i?b?)?", re.IGNORECASE)
_UNIT_MIB = {"m": 1, "g": 1024, "t": 1024 * 1024}


def parse_memory_mib(value: str | None) -> int | None:
    """Return a memory size in MiB, or None for an empty, bad, zero or negative value.

    The units are binary: `16g`, `16G`, `16gi`, `16Gi`, `15360m`, `15360Mi` and `1t`.
    A number without a unit is in MiB. A fraction rounds down. This function never raises.
    """
    if not isinstance(value, str):
        return None
    match = _SIZE.fullmatch(value.strip())
    if match is None:
        return None
    number, unit = match.group(1), (match.group(2) or "m").lower()
    mib = int(Fraction(number) * _UNIT_MIB[unit])
    return mib if mib > 0 else None


def needed_from(entry: dict | None, size_bytes: int | None) -> tuple[int | None, str]:
    """Return the needed GPU memory in MiB and its basis.

    The `gpu_memory_mib` of the catalog entry wins. Next comes the size of the cached weights
    times `WEIGHTS_MARGIN`, rounded up. Else the answer is `(None, "unknown")`.
    """
    if entry is not None and entry.get("gpu_memory_mib") is not None:
        return entry["gpu_memory_mib"], "catalog"
    if size_bytes is not None:
        return math.ceil(size_bytes / 2**20 * WEIGHTS_MARGIN), "weights"
    return None, "unknown"


def fit(needed_mib: int | None, basis: str, available_mib: int | None, available_source: str | None) -> dict:
    """Return the answer of the check. `fits` is `unknown` when a number is missing."""
    if needed_mib is None or available_mib is None:
        fits = "unknown"
    else:
        fits = "yes" if needed_mib <= available_mib else "no"
    return {
        "fits": fits,
        "needed_mib": needed_mib,
        "available_mib": available_mib,
        "basis": basis,
        "available_source": available_source,
    }
