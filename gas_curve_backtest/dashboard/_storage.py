"""Storage helpers shared across dashboard pages.

Backed by Datatailr Blob when DATATAILR_JOB_TYPE is set, otherwise by
the local filesystem fallback used by `local_run.py`. The dashboard
therefore behaves identically against a freshly-deployed workflow on
the platform and against a laptop run.
"""

from __future__ import annotations

import io
import json
import os
from typing import Any

import numpy as np

from gas_curve_backtest.local_run import LOCAL_FALLBACK_ROOT, _LocalBlob
from gas_curve_backtest.workflows import blob_paths


def get_blob():
    if os.environ.get("DATATAILR_JOB_TYPE") in (None, "", "workstation"):
        return _LocalBlob(LOCAL_FALLBACK_ROOT)
    from datatailr import Blob
    return Blob()


def safe_get_json(key: str) -> Any | None:
    try:
        return json.loads(get_blob().get(key).decode("utf-8"))
    except Exception:
        return None


def safe_get_npz(key: str) -> dict[str, np.ndarray] | None:
    try:
        raw = get_blob().get(key)
        with np.load(io.BytesIO(raw)) as data:
            return {k: data[k] for k in data.files}
    except Exception:
        return None


def list_runs(limit: int = 20) -> list[str]:
    blob = get_blob()
    try:
        keys = blob.ls(f"{blob_paths.ROOT}/runs/")
    except Exception:
        return []
    runs: set[str] = set()
    for k in keys:
        s = str(k)
        marker = "/runs/"
        if marker not in s:
            continue
        rest = s.split(marker, 1)[1]
        if not rest:
            continue
        runs.add(rest.split("/", 1)[0])
    return sorted(runs, reverse=True)[:limit]


def latest_run_id() -> str | None:
    try:
        return get_blob().get(blob_paths.latest_pointer()).decode("utf-8").strip()
    except Exception:
        runs = list_runs(1)
        return runs[0] if runs else None


def load_aggregated(run_id: str) -> dict | None:
    return safe_get_json(blob_paths.aggregated(run_id))


def load_regimes(run_id: str) -> dict | None:
    return safe_get_json(blob_paths.regimes(run_id))


# Pipeline stages, in order. The dashboard derives the current stage
# from which artifacts exist on Blob — task return values are also
# auto-persisted by the platform, so we don't write a separate
# status.json.
STAGES = [
    ("generate_market", "1. Generate market"),
    ("compute_signals", "2. Compute signals"),
    ("detect_regimes", "3. Detect regimes"),
    ("child_launched", "4. Child workflow launched"),
    ("aggregate", "5. Aggregating cells"),
    ("done", "6. Done"),
]


def derive_stage(run_id: str) -> str:
    """Return the latest stage reached for `run_id`.

    Read from artifact presence on Blob storage rather than a manually
    maintained status doc.
    """
    blob = get_blob()

    def _exists(key: str) -> bool:
        try:
            if hasattr(blob, "exists"):
                return bool(blob.exists(key))
        except Exception:
            pass
        try:
            blob.get(key)
            return True
        except Exception:
            return False

    if _exists(blob_paths.aggregated(run_id)):
        return "done"
    if list_cell_results(run_id):
        return "aggregate"
    regimes = safe_get_json(blob_paths.regimes(run_id))
    if regimes:
        return "child_launched"
    if _exists(blob_paths.signals(run_id)):
        return "detect_regimes"
    if _exists(blob_paths.market_data(run_id)):
        return "compute_signals"
    return "generate_market"


def load_market(run_id: str):
    return safe_get_npz(blob_paths.market_data(run_id))


def load_signals(run_id: str):
    return safe_get_npz(blob_paths.signals(run_id))


def list_cell_results(run_id: str) -> list[dict]:
    blob = get_blob()
    try:
        keys = blob.ls(blob_paths.cell_dir(run_id))
    except Exception:
        return []
    rows: list[dict] = []
    for k in keys:
        s = str(k)
        if not s.endswith(".json"):
            continue
        try:
            rows.append(json.loads(blob.get(s).decode("utf-8")))
        except Exception:
            continue
    return rows
