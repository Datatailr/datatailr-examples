"""Tasks that generate the fundamentals snapshot consumed by the dashboard.

A real implementation would pull from a vendor API (Bloomberg, Refinitiv,
FMP, ...). Here we generate plausible, seeded data so the demo runs out of
the box and produces stable values per ticker.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import random
from typing import Any

from datatailr import task

from trading_dashboard.universe import UNIVERSE

BLOB_PREFIX = os.environ.get("FUNDIES_BLOB_PREFIX", "trading_dashboard/fundies").strip("/")
LATEST_KEY = f"{BLOB_PREFIX}/latest.json"

log = logging.getLogger("fundies")
log.setLevel(logging.INFO)


def _seeded_rng(symbol: str) -> random.Random:
    """Deterministic-but-different RNG per symbol so values are stable across runs."""
    digest = hashlib.sha256(symbol.encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def build_fundies_snapshot() -> list[dict[str, Any]]:
    """Generate one fundamentals row per universe symbol."""
    rows: list[dict[str, Any]] = []
    for info in UNIVERSE:
        rng = _seeded_rng(info.symbol)

        # Inject a small amount of daily drift so the dashboard shows movement
        # day-over-day if the workflow runs more than once.
        day_jitter = random.Random(info.symbol + dt.date.today().isoformat())
        drift = day_jitter.uniform(-0.02, 0.02)

        market_cap_b = round(rng.uniform(80, 3200) * (1 + drift), 1)
        shares_out_b = round(market_cap_b / info.initial_price, 2)
        eps = round(rng.uniform(1.5, 18.0) * (1 + drift), 2)
        pe = round(info.initial_price / eps, 1) if eps > 0 else None
        rev_growth_yoy = round(rng.uniform(-0.08, 0.35) * 100, 2)
        gross_margin = round(rng.uniform(0.18, 0.72) * 100, 2)
        op_margin = round(gross_margin * rng.uniform(0.25, 0.65), 2)
        div_yield = round(rng.uniform(0.0, 0.045) * 100, 2)
        debt_to_equity = round(rng.uniform(0.05, 1.4), 2)
        roe = round(rng.uniform(0.05, 0.45) * 100, 2)
        beta = round(rng.uniform(0.5, 1.7), 2)

        rows.append({
            "ticker": info.symbol,
            "name": info.name,
            "sector": info.sector,
            "market_cap_b": market_cap_b,
            "shares_out_b": shares_out_b,
            "eps_ttm": eps,
            "pe_ttm": pe,
            "rev_growth_yoy_pct": rev_growth_yoy,
            "gross_margin_pct": gross_margin,
            "op_margin_pct": op_margin,
            "div_yield_pct": div_yield,
            "debt_to_equity": debt_to_equity,
            "roe_pct": roe,
            "beta": beta,
        })
    return rows


@task()
def generate_fundies() -> dict[str, Any]:
    """Build the snapshot (no I/O so other tasks can chain off this)."""
    rows = build_fundies_snapshot()
    log.info("Generated %d fundies rows", len(rows))
    return {
        "as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
        "as_of_date": dt.date.today().isoformat(),
        "rows": rows,
    }


@task()
def publish_fundies(payload: dict[str, Any]) -> dict[str, Any]:
    """Write the snapshot to blob storage as today's file and as 'latest.json'."""
    from datatailr import Blob

    blob = Blob()
    body = json.dumps(payload, indent=2).encode("utf-8")

    dated_key = f"{BLOB_PREFIX}/{payload['as_of_date']}.json"
    blob.put(dated_key, body)
    blob.put(LATEST_KEY, body)

    log.info("Published fundies to blob: %s and %s (%d bytes)", dated_key, LATEST_KEY, len(body))
    return {
        "dated_key": dated_key,
        "latest_key": LATEST_KEY,
        "row_count": len(payload["rows"]),
        "bytes_written": len(body),
    }
