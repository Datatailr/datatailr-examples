import json
import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

MARKET_DATA_URL = "http://market-data-generator"
PRICE_ENGINE_URL = "http://price-engine"
RISK_MONITOR_URL = "http://risk-monitor"

if os.getenv("DATATAILR_JOB_TYPE") == "workspace":
    MARKET_DATA_URL = "http://localhost:1024"
    PRICE_ENGINE_URL = "http://localhost:1025"
    RISK_MONITOR_URL = "http://localhost:1026"


def api_get(base_url, path, params=None, timeout=5):
    try:
        resp = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def main():
    st.set_page_config(
        page_title="CSP Price Analytics",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.sidebar.title("CSP Price Analytics")
    st.sidebar.markdown("---")
    page = st.sidebar.radio(
        "Navigation",
        ["Live Monitor", "Risk Dashboard", "Analytics", "System Control"],
    )

    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh interval (s)", 2, 30, 5)

    if page == "Live Monitor":
        page_live_monitor()
    elif page == "Risk Dashboard":
        page_risk_dashboard()
    elif page == "Analytics":
        page_analytics()
    elif page == "System Control":
        page_system_control()

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


# ---------------------------------------------------------------------------
# Live Monitor
# ---------------------------------------------------------------------------

def page_live_monitor():
    st.header("Live Market Monitor")

    col1, col2, col3 = st.columns(3)
    md_status = api_get(MARKET_DATA_URL, "/status")
    pe_status = api_get(PRICE_ENGINE_URL, "/status")
    rm_status = api_get(RISK_MONITOR_URL, "/status")

    with col1:
        if md_status:
            st.metric("Market Data Ticks", f"{md_status.get('tick_count', 0):,}")
            st.caption(f"Symbols active: {md_status.get('symbols_active', 0)}")
        else:
            st.warning("Market Data Generator offline")

    with col2:
        if pe_status:
            st.metric("Price Engine Ticks", f"{pe_status.get('tick_count', 0):,}")
            st.caption(f"Signals: {pe_status.get('signals_generated', 0)}")
        else:
            st.warning("Price Engine offline")

    with col3:
        if rm_status:
            st.metric("Risk Alerts", f"{rm_status.get('alert_count', 0):,}")
            st.caption(f"Active: {rm_status.get('active_alerts', 0)}")
        else:
            st.warning("Risk Monitor offline")

    st.markdown("---")

    # Latest prices table
    st.subheader("Latest Prices")
    latest = api_get(MARKET_DATA_URL, "/latest")
    if latest:
        rows = []
        for sym, tick in sorted(latest.items()):
            rows.append({
                "Symbol": sym,
                "Price": tick.get("price", 0),
                "Bid": tick.get("bid", 0),
                "Ask": tick.get("ask", 0),
                "Volume": tick.get("volume", 0),
                "Spread": round(tick.get("ask", 0) - tick.get("bid", 0), 6),
            })
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Price": st.column_config.NumberColumn(format="%.4f"),
                    "Bid": st.column_config.NumberColumn(format="%.4f"),
                    "Ask": st.column_config.NumberColumn(format="%.4f"),
                    "Spread": st.column_config.NumberColumn(format="%.6f"),
                },
            )

    # Analytics snapshot
    st.subheader("Analytics Snapshot")
    snapshot = api_get(PRICE_ENGINE_URL, "/snapshot")
    if snapshot:
        rows = []
        for sym, a in sorted(snapshot.items()):
            rows.append({
                "Symbol": sym,
                "Price": a.get("price", 0),
                "VWAP": a.get("vwap", 0),
                "SMA Short": a.get("sma_short", 0),
                "SMA Long": a.get("sma_long", 0),
                "RSI": a.get("rsi", 50),
                "Boll Upper": a.get("bollinger_upper", 0),
                "Boll Lower": a.get("bollinger_lower", 0),
            })
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Price": st.column_config.NumberColumn(format="%.4f"),
                    "VWAP": st.column_config.NumberColumn(format="%.4f"),
                    "SMA Short": st.column_config.NumberColumn(format="%.4f"),
                    "SMA Long": st.column_config.NumberColumn(format="%.4f"),
                    "RSI": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f"),
                },
            )

    # Recent signals
    st.subheader("Recent Signals")
    signals = api_get(PRICE_ENGINE_URL, "/signals", params={"limit": 20})
    if signals:
        for sig in reversed(signals[-10:]):
            icon = "🟢" if sig.get("signal_type") == "BUY" else "🔴" if sig.get("signal_type") == "SELL" else "⚪"
            st.markdown(
                f"{icon} **{sig.get('symbol')}** {sig.get('signal_type')} "
                f"@ {sig.get('price', 0):.4f} — {sig.get('reason', '')} "
                f"(strength: {sig.get('strength', 0):.2f})"
            )
    else:
        st.info("No signals generated yet")


# ---------------------------------------------------------------------------
# Risk Dashboard
# ---------------------------------------------------------------------------

def page_risk_dashboard():
    st.header("Risk Dashboard")

    metrics = api_get(RISK_MONITOR_URL, "/risk-metrics")
    if metrics:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total P&L", f"{metrics.get('total_pnl', 0):.2f}%")
        col2.metric("Max Drawdown", f"{metrics.get('max_drawdown', 0):.2f}%")
        col3.metric("Avg Volatility", f"{metrics.get('volatility', 0):.4f}")
        col4.metric("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.2f}")
    else:
        st.warning("Risk Monitor offline")

    st.markdown("---")

    # Portfolio positions
    st.subheader("Portfolio Positions")
    portfolio = api_get(RISK_MONITOR_URL, "/portfolio")
    if portfolio:
        rows = []
        for sym, pos in sorted(portfolio.items()):
            rows.append({
                "Symbol": sym,
                "P&L (%)": pos.get("pnl", 0),
                "Drawdown (%)": pos.get("drawdown", 0),
                "Volatility": pos.get("volatility", 0),
            })
        if rows:
            df = pd.DataFrame(rows)

            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    df.sort_values("P&L (%)", ascending=True),
                    x="P&L (%)", y="Symbol",
                    orientation="h",
                    color="P&L (%)",
                    color_continuous_scale=["#ef4444", "#fbbf24", "#22c55e"],
                    title="P&L by Symbol",
                )
                fig.update_layout(height=500, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    df.sort_values("Drawdown (%)", ascending=False),
                    x="Drawdown (%)", y="Symbol",
                    orientation="h",
                    color="Drawdown (%)",
                    color_continuous_scale=["#22c55e", "#fbbf24", "#ef4444"],
                    title="Drawdown by Symbol",
                )
                fig.update_layout(height=500, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

    # Alerts
    st.subheader("Recent Alerts")

    col1, col2 = st.columns([1, 3])
    with col1:
        severity_filter = st.selectbox("Filter by severity", ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"])

    alerts_params = {"limit": 30}
    if severity_filter != "ALL":
        alerts_params["severity"] = severity_filter
    alerts = api_get(RISK_MONITOR_URL, "/alerts", params=alerts_params)

    if alerts:
        for alert in reversed(alerts[-15:]):
            sev = alert.get("severity", "LOW")
            icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
            st.markdown(
                f"{icon} **[{sev}]** {alert.get('alert_type', '')} — "
                f"{alert.get('message', '')} "
                f"(value: {alert.get('value', 0):.4f}, threshold: {alert.get('threshold', 0):.4f})"
            )
    else:
        st.info("No alerts")


# ---------------------------------------------------------------------------
# Analytics (batch report)
# ---------------------------------------------------------------------------

def page_analytics():
    st.header("Analytics Report")

    report = None
    try:
        from datatailr import Blob
        blob = Blob()
        if blob.exists("csp_price_analytics/reports/daily_report.json"):
            raw = blob.get("csp_price_analytics/reports/daily_report.json")
            report = json.loads(raw)
    except Exception:
        pass

    if not report:
        st.info(
            "No daily report available yet. Run the Daily Analytics Pipeline from the System Control tab "
            "to generate one, or wait for the scheduled run."
        )
        return

    st.caption(f"Report generated: {report.get('generated_at', 'N/A')}")

    summary = report.get("summary", {})
    col1, col2, col3 = st.columns(3)
    col1.metric("Symbols Analyzed", summary.get("num_symbols", 0))
    col2.metric("Ticks Processed", f"{summary.get('total_ticks_processed', 0):,}")
    col3.metric("Avg Return", f"{summary.get('avg_return', 0):.2f}%")

    st.markdown("---")

    # OHLCV table
    st.subheader("OHLCV Summary")
    ohlcv = report.get("ohlcv", {})
    if ohlcv:
        rows = []
        for sym, bars in sorted(ohlcv.items()):
            rows.append({
                "Symbol": sym,
                "Open": bars["open"],
                "High": bars["high"],
                "Low": bars["low"],
                "Close": bars["close"],
                "Volume": bars["volume"],
                "Return (%)": report.get("returns", {}).get(sym, 0),
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)

    # Top gainers & losers
    with col1:
        st.subheader("Top Gainers")
        gainers = report.get("top_gainers", [])
        if gainers:
            gdf = pd.DataFrame(gainers)
            fig = px.bar(gdf, x="symbol", y="return", color="return",
                         color_continuous_scale=["#86efac", "#22c55e", "#15803d"],
                         title="Top Gainers")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Losers")
        losers = report.get("top_losers", [])
        if losers:
            ldf = pd.DataFrame(losers)
            fig = px.bar(ldf, x="symbol", y="return", color="return",
                         color_continuous_scale=["#dc2626", "#f87171", "#fca5a5"],
                         title="Top Losers")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # Correlation heatmap
    st.subheader("Return Correlations")
    correlations = report.get("correlations", {})
    if correlations:
        symbols = sorted(correlations.keys())
        matrix = [[correlations[s1].get(s2, 0) for s2 in symbols] for s1 in symbols]
        fig = go.Figure(data=go.Heatmap(
            z=matrix, x=symbols, y=symbols,
            colorscale="RdYlGn", zmin=-1, zmax=1,
        ))
        fig.update_layout(height=600, title="Correlation Matrix")
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# System Control
# ---------------------------------------------------------------------------

def page_system_control():
    st.header("System Control")

    st.subheader("Service Status")
    services = [
        ("Market Data Generator", MARKET_DATA_URL),
        ("Price Engine", PRICE_ENGINE_URL),
        ("Risk Monitor", RISK_MONITOR_URL),
    ]

    for name, url in services:
        status = api_get(url, "/status")
        with st.expander(f"{'🟢' if status else '🔴'} {name}", expanded=True):
            if status:
                st.json(status)
            else:
                st.error(f"{name} is not responding")

    st.markdown("---")

    st.subheader("Market Data Configuration")
    config = api_get(MARKET_DATA_URL, "/config")
    if config:
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Symbols:**", ", ".join(config.get("symbols", [])))
        with col2:
            st.write(f"**WebSocket Port:** {config.get('ws_port', 'N/A')}")

        with st.expander("Initial Prices"):
            st.json(config.get("initial_prices", {}))
        with st.expander("Volatilities"):
            st.json(config.get("volatilities", {}))

    st.markdown("---")

    st.subheader("Batch Pipeline")
    st.markdown(
        "The **Daily Analytics Pipeline** processes accumulated tick data, "
        "computes OHLCV bars, statistics, and generates reports."
    )

    if st.button("Trigger Daily Analytics Pipeline", type="primary"):
        st.info(
            "Pipeline triggered. In a production setup this would invoke the Datatailr workflow. "
            "The report will appear in the Analytics tab once complete."
        )

    st.markdown("---")

    st.subheader("Architecture")
    st.markdown("""
    This demo consists of five Datatailr components:

    | Component | Type | Description |
    |-----------|------|-------------|
    | Market Data Generator | Service | Produces synthetic price ticks via CSP, broadcasts over WebSocket |
    | Price Engine | Service | Consumes ticks via WebSocket, computes VWAP/SMA/RSI/signals via CSP |
    | Risk Monitor | Service | Consumes ticks + signals, tracks P&L/drawdown/alerts via CSP |
    | Daily Analytics Pipeline | Workflow | Batch DAG: ingest -> aggregate -> statistics -> report |
    | Control Dashboard | App | This Streamlit dashboard for monitoring and control |

    **Inter-service communication:** WebSocket for streaming data, REST for queries, Blob Storage for persistence.

    **CSP Integration:** Each service runs a CSP graph in realtime mode on a background thread.
    The CSP engine processes ticks through `@csp.node` computations with sub-millisecond latency.
    """)


if __name__ == "__main__":
    main()
