"""The deliverable Marco actually asked for: where do thresholds work?

For a chosen (regime, tenor) we render a heatmap of Sharpe over the
(signal-threshold, asymmetry-pivot) grid, plus a complementary PnL
heatmap and the robust-pick highlight.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from gas_curve_backtest.dashboard import _storage as storage


def main() -> None:
    st.set_page_config(page_title="Threshold Heatmap", layout="wide", page_icon=":bar_chart:")
    st.title("Threshold Heatmap")

    rid = st.session_state.get("active_run_id") or storage.latest_run_id()
    if not rid:
        st.info("No active run. Launch one from *Run Backtest*.")
        return
    st.caption(f"run_id: `{rid}`")

    cells = storage.list_cell_results(rid)
    if not cells:
        st.info("No cell results yet. Try refreshing once *Live Progress* reports cells.")
        return

    df = pd.DataFrame(cells)
    regimes = sorted(df["regime_id"].unique().tolist())
    tenors = sorted(df["tenor"].unique().tolist())

    c1, c2, c3 = st.columns(3)
    regime = c1.selectbox("Regime", regimes, index=0)
    tenor = c2.selectbox("Tenor (M+)", tenors, index=0)
    metric = c3.selectbox("Heatmap metric", ["sharpe", "pnl", "max_drawdown", "hit_rate"])

    sub = df[(df["regime_id"] == regime) & (df["tenor"] == tenor)]
    if sub.empty:
        st.info("No cells for that selection yet.")
        return

    pivot = sub.pivot_table(
        index="asym_pivot",
        columns="sig_threshold",
        values=metric,
        aggfunc="mean",
    )
    pivot = pivot.sort_index().sort_index(axis=1)

    fig = px.imshow(
        pivot.values,
        x=[round(c, 3) for c in pivot.columns],
        y=[round(r, 3) for r in pivot.index],
        labels={"x": "Signal threshold", "y": "Asymmetry pivot", "color": metric},
        color_continuous_scale="RdBu" if metric != "max_drawdown" else "Reds_r",
        origin="lower",
        aspect="auto",
    )
    fig.update_layout(height=520)
    st.plotly_chart(fig, use_container_width=True)

    best = sub.iloc[sub[metric].idxmax() if metric != "max_drawdown" else sub[metric].idxmin()]
    bcols = st.columns(5)
    bcols[0].metric("Best signal threshold", round(float(best["sig_threshold"]), 3))
    bcols[1].metric("Best asymmetry pivot", round(float(best["asym_pivot"]), 3))
    bcols[2].metric("Sharpe", round(float(best["sharpe"]), 3))
    bcols[3].metric("PnL", round(float(best["pnl"]), 2))
    bcols[4].metric("Max drawdown", round(float(best["max_drawdown"]), 2))

    st.markdown("### Robustness across all tenors (this regime)")
    by_threshold = (
        df[df["regime_id"] == regime]
        .groupby(["sig_threshold", "asym_pivot"])[metric]
        .mean()
        .reset_index()
    )
    pivot_all = by_threshold.pivot(index="asym_pivot", columns="sig_threshold", values=metric)
    fig2 = px.imshow(
        pivot_all.values,
        x=[round(c, 3) for c in pivot_all.columns],
        y=[round(r, 3) for r in pivot_all.index],
        labels={"x": "Signal threshold", "y": "Asymmetry pivot", "color": f"avg {metric}"},
        color_continuous_scale="RdBu" if metric != "max_drawdown" else "Reds_r",
        origin="lower",
        aspect="auto",
    )
    fig2.update_layout(height=420)
    st.plotly_chart(fig2, use_container_width=True)

    st.caption(
        "The robust pick is the (threshold, pivot) cell that performs well "
        "averaged across tenors — exactly the filter-and-sizing question "
        "Marco described."
    )


main()
