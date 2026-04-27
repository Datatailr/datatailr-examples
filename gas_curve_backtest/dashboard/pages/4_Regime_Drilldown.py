"""Per-regime equity curve, asymmetry distribution and signal trace."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from gas_curve_backtest.backtest.core import backtest_cell
from gas_curve_backtest.dashboard import _storage as storage


def main() -> None:
    st.set_page_config(page_title="Regime Drilldown", layout="wide", page_icon=":mag:")
    st.title("Regime Drilldown")

    rid = st.session_state.get("active_run_id") or storage.latest_run_id()
    if not rid:
        st.info("No active run.")
        return
    st.caption(f"run_id: `{rid}`")

    regimes_payload = storage.load_regimes(rid)
    sigs = storage.load_signals(rid)
    if not regimes_payload or not sigs:
        st.info("Regimes or signals not yet available.")
        return

    regimes = regimes_payload["regimes"]
    cells = storage.list_cell_results(rid)
    df_cells = pd.DataFrame(cells) if cells else pd.DataFrame()

    regime_ids = [r["regime_id"] for r in regimes]
    chosen_regime = st.selectbox("Regime", regime_ids)
    regime = next(r for r in regimes if r["regime_id"] == chosen_regime)
    days = np.array(regime["day_indices"], dtype=int)

    n_tenors = sigs["combined_signal"].shape[1]
    tenor = st.slider("Tenor (M+)", 0, n_tenors - 1, 0)

    if not df_cells.empty:
        sub = df_cells[
            (df_cells["regime_id"] == chosen_regime) & (df_cells["tenor"] == tenor)
        ]
        if not sub.empty:
            best = sub.loc[sub["sharpe"].idxmax()]
            sig_thr = float(best["sig_threshold"])
            pivot = float(best["asym_pivot"])
        else:
            sig_thr, pivot = 0.2, regime["median_asymmetry"]
    else:
        sig_thr, pivot = 0.2, regime["median_asymmetry"]

    c1, c2 = st.columns(2)
    sig_thr = c1.slider("Signal threshold", 0.0, 0.7, float(sig_thr), 0.01)
    pivot = c2.slider("Asymmetry pivot", 0.5, 1.8, float(pivot), 0.01)

    sig_arr = sigs["combined_signal"][days, tenor]
    asym_arr = sigs["asymmetry"][days, tenor]
    pnl_arr = sigs["pnl_per_unit"][days, tenor]
    equity = backtest_cell(sig_arr, asym_arr, pnl_arr, sig_thr, pivot)

    eq_df = pd.DataFrame({"day": np.arange(equity.size), "equity": equity})
    fig = px.line(eq_df, x="day", y="equity", title="Equity curve (this regime, this tenor)")
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Signal & asymmetry on regime days")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(y=sig_arr, mode="lines", name="combined signal"))
    fig2.add_trace(go.Scatter(y=asym_arr, mode="lines", name="asymmetry", yaxis="y2"))
    fig2.update_layout(
        height=380,
        yaxis=dict(title="signal"),
        yaxis2=dict(title="asymmetry", overlaying="y", side="right"),
        legend=dict(orientation="h"),
    )
    st.plotly_chart(fig2, use_container_width=True)

    if not df_cells.empty:
        st.markdown("### Sharpe across tenors at the chosen (threshold, pivot)")
        nearest = df_cells[df_cells["regime_id"] == chosen_regime].copy()
        nearest["dist"] = (
            (nearest["sig_threshold"] - sig_thr) ** 2
            + (nearest["asym_pivot"] - pivot) ** 2
        )
        slice_df = (
            nearest.sort_values("dist")
            .groupby("tenor")
            .head(1)
            .sort_values("tenor")
        )
        fig3 = px.bar(slice_df, x="tenor", y="sharpe", title="Sharpe by tenor")
        fig3.update_layout(height=320)
        st.plotly_chart(fig3, use_container_width=True)


main()
