"""Streamlit cockpit for the Gas Curve Backtest demo.

Landing page: pick a run, see top-line status. Detail pages live
under `pages/`:
  1. Run Backtest         — configure + launch (laptop or platform)
  2. Live Progress        — workflow stage tracker
  3. Threshold Heatmap    — Sharpe / PnL across (signal x pivot) cells
  4. Regime Drilldown     — equity curves and per-regime stats
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from gas_curve_backtest.dashboard import _storage as storage


def _set_active_run(run_id: str | None) -> None:
    st.session_state["active_run_id"] = run_id


def main() -> None:
    st.set_page_config(page_title="Gas Curve Backtest", layout="wide", page_icon=":zap:")

    st.title("Gas Curve Backtest")
    st.caption(
        "Stack-model pricing + ECMWF-driven signals + dynamic regime-aware fan-out."
    )

    runs = storage.list_runs(20)
    latest = storage.latest_run_id()
    # Keep an in-flight run visible even before its first blob lands —
    # the parent's early tasks (`generate_market`, `compute_signals`)
    # pass their payloads through the platform's task-return channel,
    # so nothing is written under `runs/<rid>/` until `compute_signals`
    # finishes. Without this, navigating back to the main page would
    # silently re-elect an older run and clobber `active_run_id`.
    in_flight = st.session_state.get("active_run_id")
    if in_flight and in_flight not in runs:
        runs = [in_flight] + runs
    default_idx = 0
    if in_flight and in_flight in runs:
        default_idx = runs.index(in_flight)
    elif latest and latest in runs:
        default_idx = runs.index(latest)

    with st.sidebar:
        st.subheader("Active run")
        if runs:
            choice = st.selectbox("Pick a run_id", runs, index=default_idx)
            _set_active_run(choice)
        else:
            st.info("No runs yet. Open *Run Backtest* to start one.")
            _set_active_run(None)
        st.divider()
        st.markdown(
            "**Pages**\n\n"
            "1. Run Backtest\n"
            "2. Live Progress\n"
            "3. Threshold Heatmap\n"
            "4. Regime Drilldown"
        )

    active = st.session_state.get("active_run_id")
    if not active:
        st.info("Use the *Run Backtest* page on the left to launch your first run.")
        return

    st.subheader(f"Run `{active}`")
    aggregated = storage.load_aggregated(active) or {}
    regimes_payload = storage.load_regimes(active) or {}
    stage = storage.derive_stage(active)

    cols = st.columns(4)
    cols[0].metric("Stage", stage)
    cols[1].metric("Cells written", aggregated.get("cells", 0))
    cols[2].metric("Regimes", len(regimes_payload.get("regimes", [])))
    cols[3].metric("Expected cells", regimes_payload.get("expected_cells", "?"))

    if regimes_payload:
        st.markdown("### Detected regimes")
        rows = [
            {
                "regime_id": r["regime_id"],
                "days": r["size"],
                "median asymmetry": round(r["median_asymmetry"], 3),
                "median spread (EUR/MWh)": round(r["median_spread"], 2),
                "median signal": round(r["median_signal"], 3),
            }
            for r in regimes_payload.get("regimes", [])
        ]
        st.dataframe(rows, use_container_width=True)

    if aggregated.get("best_per_regime"):
        st.markdown("### Best cell per regime")
        best_rows = []
        for entry in aggregated["best_per_regime"]:
            cell = entry.get("best") or {}
            best_rows.append(
                {
                    "regime_id": cell.get("regime_id"),
                    "tenor": cell.get("tenor"),
                    "sig_threshold": cell.get("sig_threshold"),
                    "asym_pivot": cell.get("asym_pivot"),
                    "Sharpe": round(cell.get("sharpe", 0.0), 3),
                    "PnL": round(cell.get("pnl", 0.0), 2),
                    "max DD": round(cell.get("max_drawdown", 0.0), 2),
                    "hit rate": round(cell.get("hit_rate", 0.0), 3),
                }
            )
        st.dataframe(best_rows, use_container_width=True)


if __name__ == "__main__":
    main()
