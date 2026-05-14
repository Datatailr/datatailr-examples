"""Streamlit dashboard for the EIA Weekly Petroleum Status Report.

Reads parsed weekly reports from blob storage (written by the ingestion
workflow) and renders KPI cards, time-series charts, and a section browser.
"""

from __future__ import annotations

import io
import time
from datetime import date, datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from weekly_oil_reports.common.parser import (
    PARSED_PREFIX,
    parse_report,
    parsed_blob_key,
    raw_blob_key,
)

REFRESH_SECONDS = 300


def _blob():
    from datatailr import Blob

    return Blob()


def _list_parsed_reports() -> list[tuple[date, str]]:
    """Return [(report_date, full_blob_key)] for every parsed report blob.

    The Datatailr Blob.ls() returns entries whose `name` is relative to the
    bucket root (not the requested prefix), so we extract the YYYY-MM-DD
    date from the basename and rebuild the canonical full key ourselves.
    """
    blob = _blob()
    try:
        entries = blob.ls(PARSED_PREFIX + "/") or []
    except Exception as exc:
        st.error(f"Could not list blob storage: {exc}")
        return []

    out: list[tuple[date, str]] = []
    for entry in entries:
        name = entry["name"] if isinstance(entry, dict) else str(entry)
        if not name.endswith(".parquet"):
            continue
        basename = name.rsplit("/", 1)[-1]
        stem = basename[: -len(".parquet")]
        try:
            report_date = datetime.strptime(stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append((report_date, parsed_blob_key(report_date)))
    return sorted(out)


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def load_all_reports(_cache_bust: int) -> pd.DataFrame:
    """Load every parsed report from blob storage into a single DataFrame."""
    blob = _blob()
    reports = _list_parsed_reports()
    frames: list[pd.DataFrame] = []
    for _report_date, key in reports:
        try:
            data = blob.get(key)
            df = pd.read_parquet(io.BytesIO(data))
            frames.append(df)
        except Exception as exc:
            st.warning(f"Skipping unreadable blob `{key}`: {exc}")
    if not frames:
        return pd.DataFrame(
            columns=[
                "report_date",
                "as_of_date",
                "column_type",
                "category",
                "series",
                "value",
            ]
        )
    full = pd.concat(frames, ignore_index=True)
    full["report_date"] = pd.to_datetime(full["report_date"])
    full["as_of_date"] = pd.to_datetime(full["as_of_date"])
    return full


def _trigger_download() -> str:
    """Synchronously download the latest report and store it; returns status."""
    import requests

    from weekly_oil_reports.ingest_workflow.tasks import EIA_TABLE9_URL

    resp = requests.get(EIA_TABLE9_URL, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    csv_text = resp.text
    df = parse_report(csv_text)
    if df.empty:
        return "Parsed an empty report; nothing stored."
    report_date = df["report_date"].iloc[0].date()

    blob = _blob()
    blob.put(raw_blob_key(report_date), csv_text.encode("utf-8"))
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    blob.put(parsed_blob_key(report_date), buf.getvalue())
    return f"Stored report for week ending {report_date} ({len(df):,} rows)."


def _format_value(value: float, units: str) -> str:
    if pd.isna(value):
        return "--"
    if units == "Mbbl":
        return f"{value:,.1f} Mbbl"
    if value >= 1000:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def _delta_pct(current: float, baseline: float) -> str | None:
    if pd.isna(current) or pd.isna(baseline) or baseline == 0:
        return None
    pct = (current - baseline) / abs(baseline) * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}% vs prior"


def _kpi_card(
    df: pd.DataFrame,
    *,
    category: str,
    series: str,
    label: str,
    units: str,
) -> tuple[str, str, str | None]:
    """Build (label, value_str, delta_str) for a KPI metric on the latest report."""
    sub = df[(df["category"] == category) & (df["series"] == series)]
    if sub.empty:
        return label, "--", None
    latest_report = sub["report_date"].max()
    sub = sub[sub["report_date"] == latest_report]
    current = sub.loc[sub["column_type"] == "weekly_current", "value"]
    prior = sub.loc[sub["column_type"] == "weekly_prior", "value"]
    year_ago = sub.loc[sub["column_type"] == "weekly_year_ago", "value"]
    cur_val = float(current.iloc[0]) if not current.empty else float("nan")
    prior_val = float(prior.iloc[0]) if not prior.empty else float("nan")
    yoy_val = float(year_ago.iloc[0]) if not year_ago.empty else float("nan")

    wow = _delta_pct(cur_val, prior_val)
    yoy = _delta_pct(cur_val, yoy_val)
    delta_lines = [d for d in (wow, yoy and yoy.replace("vs prior", "vs YoY")) if d]
    delta = " | ".join(delta_lines) if delta_lines else None
    return label, _format_value(cur_val, units), delta


def _build_time_series(df: pd.DataFrame, category: str, series: str) -> pd.DataFrame:
    """Return one-point-per-report time series for a (category, series) pair."""
    sub = df[
        (df["category"] == category)
        & (df["series"] == series)
        & (df["column_type"] == "weekly_current")
    ].copy()
    sub = sub.sort_values("report_date")
    sub["four_week_avg"] = sub["value"].rolling(4, min_periods=1).mean()
    return sub


def _section_snapshot(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """Pivot the latest report for a single section into a wide compare table."""
    latest_report = df.loc[df["category"] == category, "report_date"].max()
    if pd.isna(latest_report):
        return pd.DataFrame()
    sub = df[(df["category"] == category) & (df["report_date"] == latest_report)]
    pivoted = sub.pivot_table(
        index="series",
        columns="column_type",
        values="value",
        aggfunc="first",
    ).reset_index()
    columns_order = ["series"] + [
        c
        for c in (
            "weekly_current",
            "weekly_prior",
            "weekly_year_ago",
            "four_week_avg_current",
            "four_week_avg_year_ago",
        )
        if c in pivoted.columns
    ]
    pivoted = pivoted[columns_order]
    if "weekly_prior" in pivoted.columns and "weekly_current" in pivoted.columns:
        pivoted["WoW %"] = (
            (pivoted["weekly_current"] - pivoted["weekly_prior"])
            / pivoted["weekly_prior"].replace(0, pd.NA)
            * 100
        )
    if "weekly_year_ago" in pivoted.columns and "weekly_current" in pivoted.columns:
        pivoted["YoY %"] = (
            (pivoted["weekly_current"] - pivoted["weekly_year_ago"])
            / pivoted["weekly_year_ago"].replace(0, pd.NA)
            * 100
        )
    return pivoted


def _empty_state():
    st.info(
        "No reports in blob storage yet. Run the **EIA Weekly Oil Report "
        "Ingestion** workflow (Wed/Thu/Fri at 16:00 UTC) or trigger a "
        "one-off fetch below."
    )
    if st.button("Fetch latest report now", type="primary"):
        with st.spinner("Downloading and parsing EIA Table 9..."):
            try:
                msg = _trigger_download()
                st.success(msg)
                st.cache_data.clear()
                st.rerun()
            except Exception as exc:
                st.error(f"Fetch failed: {exc}")


def _render_overview(df: pd.DataFrame, latest_report: date):
    st.subheader(f"Latest report — week ending {latest_report.strftime('%b %d, %Y')}")

    kpis = [
        _kpi_card(df, category="Crude Oil Production", series="Domestic Production",
                  label="Crude oil production", units="kbbl/d"),
        _kpi_card(df, category="Refiner Inputs and Utilization", series="Crude Oil Inputs",
                  label="Refiner crude inputs", units="kbbl/d"),
        _kpi_card(df, category="Refiner Inputs and Utilization", series="Percent Utilization",
                  label="Refinery utilization", units="%"),
        _kpi_card(df, category="Stocks (Million Barrels)", series="Commercial",
                  label="Commercial crude stocks", units="Mbbl"),
        _kpi_card(df, category="Product Supplied", series="Finished Motor Gasoline",
                  label="Gasoline demand", units="kbbl/d"),
        _kpi_card(df, category="Exports", series="Crude Oil",
                  label="Crude oil exports", units="kbbl/d"),
    ]

    cols = st.columns(3)
    for i, (label, value_str, delta) in enumerate(kpis):
        with cols[i % 3]:
            st.metric(label=label, value=value_str, delta=delta)


def _render_time_series(df: pd.DataFrame):
    st.subheader("Time series across all stored reports")

    pairs = (
        df[["category", "series"]]
        .drop_duplicates()
        .sort_values(["category", "series"])
    )
    categories = sorted(pairs["category"].unique().tolist())
    default_cat = "Crude Oil Production" if "Crude Oil Production" in categories else categories[0]
    cat_idx = categories.index(default_cat)

    col1, col2 = st.columns([1, 2])
    with col1:
        chosen_cat = st.selectbox("Section", categories, index=cat_idx)
    cat_series = sorted(pairs.loc[pairs["category"] == chosen_cat, "series"].unique().tolist())
    default_series = "Domestic Production" if "Domestic Production" in cat_series else cat_series[0]
    with col2:
        chosen_series = st.selectbox(
            "Series", cat_series, index=cat_series.index(default_series)
        )

    ts = _build_time_series(df, chosen_cat, chosen_series)
    if ts.empty:
        st.info("No history yet for this series — it will fill in as more weekly reports arrive.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=ts["report_date"],
            y=ts["value"],
            mode="lines+markers",
            name="Weekly value",
            line=dict(color="#1f77b4", width=2),
        )
    )
    if len(ts) >= 2:
        fig.add_trace(
            go.Scatter(
                x=ts["report_date"],
                y=ts["four_week_avg"],
                mode="lines",
                name="4-week MA",
                line=dict(color="#ff7f0e", width=2, dash="dash"),
            )
        )
    fig.update_layout(
        title=f"{chosen_cat} — {chosen_series}",
        xaxis_title="Report week",
        yaxis_title="Value",
        height=420,
        margin=dict(l=10, r=10, t=50, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_section_browser(df: pd.DataFrame):
    st.subheader("Section snapshot")
    categories = sorted(df["category"].unique().tolist())
    default = "Stocks (Million Barrels)" if "Stocks (Million Barrels)" in categories else categories[0]
    chosen = st.selectbox(
        "Section", categories, index=categories.index(default), key="section_browser"
    )
    pivoted = _section_snapshot(df, chosen)
    if pivoted.empty:
        st.info("No data for this section.")
        return

    st.dataframe(
        pivoted.style.format(
            {
                "weekly_current": "{:,.2f}",
                "weekly_prior": "{:,.2f}",
                "weekly_year_ago": "{:,.2f}",
                "four_week_avg_current": "{:,.2f}",
                "four_week_avg_year_ago": "{:,.2f}",
                "WoW %": "{:+.1f}%",
                "YoY %": "{:+.1f}%",
            },
            na_rep="--",
        ),
        use_container_width=True,
        height=min(80 + 35 * len(pivoted), 600),
    )


def _render_padd_breakdown(df: pd.DataFrame):
    """Bar chart of regional (PADD) refinery utilization for the latest report."""
    st.subheader("Refinery utilization by region (PADD)")
    sub = df[
        (df["category"] == "Refiner Inputs and Utilization")
        & (df["column_type"] == "weekly_current")
        & (df["series"].str.contains("PADD", na=False))
    ].copy()
    if sub.empty:
        st.info("No regional utilization data available.")
        return
    latest_report = sub["report_date"].max()
    sub = sub[sub["report_date"] == latest_report]

    # The CSV repeats the PADD region series under several parent rows
    # (Crude Oil Inputs, Gross Inputs, Operable Capacity, Percent Utilization).
    # The "Percent Utilization" block has values 0-100; pick those.
    util = sub[sub["value"].between(0, 100)].copy()
    if util.empty:
        st.info("Could not isolate utilization rows for the latest report.")
        return

    util = util.drop_duplicates(subset=["series"], keep="last").sort_values("value")
    fig = px.bar(
        util,
        x="value",
        y="series",
        orientation="h",
        text=util["value"].round(1).astype(str) + "%",
        color="value",
        color_continuous_scale="Blues",
        height=380,
    )
    fig.update_layout(
        xaxis_title="Utilization (%)",
        yaxis_title="",
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_showscale=False,
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig, use_container_width=True)


def _render_history_panel(df: pd.DataFrame):
    dates = [d for d, _key in _list_parsed_reports()]
    st.sidebar.header("Stored reports")
    st.sidebar.write(f"**{len(dates)}** weekly report(s) in blob storage")
    if dates:
        st.sidebar.write(f"Range: {dates[0]} → {dates[-1]}")
        with st.sidebar.expander("All report dates"):
            for d in reversed(dates):
                st.sidebar.write(f"• {d}")
    if st.sidebar.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    if st.sidebar.button("Fetch latest now"):
        with st.spinner("Downloading and parsing EIA Table 9..."):
            try:
                msg = _trigger_download()
                st.sidebar.success(msg)
                st.cache_data.clear()
                st.rerun()
            except Exception as exc:
                st.sidebar.error(f"Fetch failed: {exc}")


def main():
    st.set_page_config(
        page_title="EIA Weekly Oil Report",
        page_icon=":fuelpump:",
        layout="wide",
    )
    st.title("EIA Weekly Petroleum Status Report")
    st.caption(
        "Source: U.S. Energy Information Administration — Table 9 "
        "(thousand barrels per day unless noted). "
        "Data is fetched and stored weekly to Datatailr blob storage."
    )

    cache_bust = int(time.time() // REFRESH_SECONDS)
    df = load_all_reports(cache_bust)
    _render_history_panel(df)

    if df.empty:
        _empty_state()
        return

    latest_report = df["report_date"].max().date()

    _render_overview(df, latest_report)
    st.divider()

    left, right = st.columns([3, 2])
    with left:
        _render_time_series(df)
    with right:
        _render_padd_breakdown(df)

    st.divider()
    _render_section_browser(df)


if __name__ == "__main__":
    main()
