"""Live tracker for the active run.

Polls the run's status blob and the per-cell result blobs every few
seconds so the dashboard reflects the deployed workflow's progress
without needing a streaming connection.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pandas as pd
import streamlit as st

from gas_curve_backtest.dashboard import _storage as storage


def main() -> None:
    st.set_page_config(page_title="Live Progress", layout="wide", page_icon=":satellite:")
    st.title("Live Progress")

    rid = st.session_state.get("active_run_id") or storage.latest_run_id()
    if not rid:
        st.info("No active run. Launch one from *Run Backtest*.")
        return
    st.caption(f"run_id: `{rid}`")

    auto = st.toggle("Auto-refresh every 3s", value=True)

    aggregated = storage.load_aggregated(rid) or {}
    regimes_payload = storage.load_regimes(rid) or {}
    cells = storage.list_cell_results(rid)
    expected = regimes_payload.get("expected_cells")
    current = storage.derive_stage(rid)

    cols = st.columns(4)
    cols[0].metric("Stage", current)
    cols[1].metric("Regimes detected", len(regimes_payload.get("regimes", [])))
    cols[2].metric("Cells completed", len(cells))
    cols[3].metric("Expected cells", expected if expected is not None else "?")

    if expected:
        progress = min(1.0, len(cells) / max(1, expected))
        st.progress(progress)

    md = []
    seen_current = False
    for key, label in storage.STAGES:
        if current == "done":
            md.append(f"- :white_check_mark: {label}")
        elif current == key:
            md.append(f"- :large_blue_circle: **{label}** (in progress)")
            seen_current = True
        elif not seen_current:
            md.append(f"- :white_check_mark: {label}")
        else:
            md.append(f"- :white_circle: {label}")
    st.markdown("\n".join(md))

    if cells:
        df = pd.DataFrame(cells)
        st.markdown("### Throughput")
        by_regime = (
            df.groupby("regime_id").size().rename("completed").reset_index()
        )
        st.dataframe(by_regime, use_container_width=True)

        top = df.nlargest(min(10, len(df)), "sharpe")[
            ["regime_id", "tenor", "sig_threshold", "asym_pivot", "sharpe", "pnl", "max_drawdown"]
        ].round(3)
        st.markdown("### Top cells so far")
        st.dataframe(top, use_container_width=True)

    if auto and current != "done":
        time.sleep(3.0)
        st.rerun()


main()
