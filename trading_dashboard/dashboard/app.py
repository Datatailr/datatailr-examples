"""Trading desk dashboard.

Single Dash application with four tabs:

    1. Strategies & PnL  - live ticking PnL chart + per-strategy positions
    2. Fundies           - latest fundamentals snapshot from blob storage
    3. Vendor Inbox AI   - emails enriched with AI summaries
    4. Live Prices       - real-time top-of-book quotes

Data sources are pluggable (env vars override the defaults) so the same
app runs both locally and on Datatailr without code changes.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import plotly.graph_objects as go
import requests
from dash import ALL, Dash, Input, Output, State, ctx, dash_table, dcc, html


log = logging.getLogger("dashboard")
logging.basicConfig(level=logging.INFO)


# ----------------------------------------------------------------------------
# Runtime configuration
# ----------------------------------------------------------------------------

_JOB_TYPE = os.environ.get("DATATAILR_JOB_TYPE", "")
_IS_LOCAL = _JOB_TYPE in ("workstation", "")

REQUESTS_PATHNAME_PREFIX = os.environ.get("DASH_REQUESTS_PATHNAME_PREFIX", "/")

MARKET_DATA_URL = os.environ.get(
    "MARKET_DATA_URL",
    "http://localhost:8090" if _IS_LOCAL else "http://trading-market-data",
).rstrip("/")
STRATEGY_ENGINE_URL = os.environ.get(
    "STRATEGY_ENGINE_URL",
    "http://localhost:8091" if _IS_LOCAL else "http://trading-strategy-engine",
).rstrip("/")
FUNDIES_BLOB_PREFIX = os.environ.get("FUNDIES_BLOB_PREFIX", "trading_dashboard/fundies").strip("/")
INBOX_BLOB_PREFIX = os.environ.get("INBOX_BLOB_PREFIX", "trading_dashboard/inbox").strip("/")

REFRESH_INTERVAL_MS = int(os.environ.get("DASHBOARD_REFRESH_MS", "1500"))
SLOW_REFRESH_INTERVAL_MS = int(os.environ.get("DASHBOARD_SLOW_REFRESH_MS", "30000"))


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def _safe_get(url: str, timeout: float = 5.0) -> Any | None:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("GET %s failed: %s", url, exc)
        return None


def _read_blob_json(key: str) -> Any | None:
    try:
        from datatailr import Blob

        blob = Blob()
        if not blob.exists(key):
            return None
        return json.loads(blob.get(key).decode("utf-8"))
    except Exception as exc:
        log.warning("Read blob %s failed: %s", key, exc)
        return None


def _fmt_money(v: float | int | None) -> str:
    if v is None:
        return "-"
    sign = "-" if v < 0 else ""
    n = abs(float(v))
    if n >= 1_000_000:
        return f"{sign}${n/1_000_000:,.2f}M"
    if n >= 1_000:
        return f"{sign}${n/1_000:,.1f}k"
    return f"{sign}${n:,.2f}"


# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------

app = Dash(
    __name__,
    title="Trading Desk Dashboard",
    suppress_callback_exceptions=True,
    requests_pathname_prefix=REQUESTS_PATHNAME_PREFIX,
)
server = app.server


_HEADER_STYLE = {
    "padding": "14px 24px",
    "background": "linear-gradient(90deg, #0f172a 0%, #1e293b 100%)",
    "color": "white",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "fontFamily": "Inter, system-ui, sans-serif",
}

_KPI_STYLE = {"display": "flex", "gap": "24px", "alignItems": "center"}

_KPI_BOX = {
    "background": "rgba(255,255,255,0.08)",
    "padding": "8px 14px",
    "borderRadius": "8px",
    "minWidth": "140px",
}


def _kpi(label: str, value_id: str, subtitle_id: str | None = None) -> html.Div:
    children = [
        html.Div(label, style={"fontSize": "11px", "opacity": 0.7, "letterSpacing": "0.06em", "textTransform": "uppercase"}),
        html.Div(id=value_id, style={"fontSize": "20px", "fontWeight": 600}),
    ]
    if subtitle_id:
        children.append(html.Div(id=subtitle_id, style={"fontSize": "11px", "opacity": 0.7}))
    return html.Div(children, style=_KPI_BOX)


def _layout() -> html.Div:
    return html.Div(
        style={"fontFamily": "Inter, system-ui, sans-serif", "background": "#f8fafc", "minHeight": "100vh"},
        children=[
            html.Div(
                style=_HEADER_STYLE,
                children=[
                    html.Div([
                        html.Div("TRADING DESK", style={"fontSize": "11px", "opacity": 0.7, "letterSpacing": "0.18em"}),
                        html.Div("Live PnL & Vendor AI", style={"fontSize": "20px", "fontWeight": 700}),
                    ]),
                    html.Div(
                        [
                            _kpi("Total PnL", "kpi-pnl", "kpi-pnl-as-of"),
                            _kpi("Gross Exposure", "kpi-gross"),
                            _kpi("Net Exposure", "kpi-net"),
                            _kpi("Strategies", "kpi-strats"),
                        ],
                        style=_KPI_STYLE,
                    ),
                ],
            ),
            dcc.Tabs(
                id="tabs",
                value="tab-strategies",
                children=[
                    dcc.Tab(label="Strategies & PnL", value="tab-strategies"),
                    dcc.Tab(label="Fundamentals", value="tab-fundies"),
                    dcc.Tab(label="Vendor Inbox AI", value="tab-inbox"),
                    dcc.Tab(label="Live Prices", value="tab-prices"),
                ],
                style={"background": "white"},
            ),
            html.Div(id="tab-content", style={"padding": "24px"}),

            dcc.Interval(id="fast-tick", interval=REFRESH_INTERVAL_MS, n_intervals=0),
            dcc.Interval(id="slow-tick", interval=SLOW_REFRESH_INTERVAL_MS, n_intervals=0),

            dcc.Store(id="store-strategies"),
            dcc.Store(id="store-pnl-history"),
            dcc.Store(id="store-quotes"),
            dcc.Store(id="store-fundies"),
            dcc.Store(id="store-inbox-index"),
            dcc.Store(id="store-selected-email-id"),
            dcc.Store(id="store-inbox-detail"),
        ],
    )


app.layout = _layout()


# ----------------------------------------------------------------------------
# Data fetch (centralised so each tab uses the same store)
# ----------------------------------------------------------------------------

@app.callback(
    Output("store-strategies", "data"),
    Output("store-pnl-history", "data"),
    Output("store-quotes", "data"),
    Input("fast-tick", "n_intervals"),
)
def _refresh_fast(_n):
    strategies = _safe_get(f"{STRATEGY_ENGINE_URL}/strategies")
    history = _safe_get(f"{STRATEGY_ENGINE_URL}/pnl/history")
    quotes = _safe_get(f"{MARKET_DATA_URL}/quotes")
    return strategies, history, quotes


@app.callback(
    Output("store-fundies", "data"),
    Output("store-inbox-index", "data"),
    Input("slow-tick", "n_intervals"),
)
def _refresh_slow(_n):
    fundies = _read_blob_json(f"{FUNDIES_BLOB_PREFIX}/latest.json")
    inbox = _read_blob_json(f"{INBOX_BLOB_PREFIX}/index.json")
    return fundies, inbox


# ----------------------------------------------------------------------------
# Header KPIs (always visible)
# ----------------------------------------------------------------------------

@app.callback(
    Output("kpi-pnl", "children"),
    Output("kpi-pnl-as-of", "children"),
    Output("kpi-gross", "children"),
    Output("kpi-net", "children"),
    Output("kpi-strats", "children"),
    Input("store-strategies", "data"),
)
def _update_kpis(payload):
    if not payload or "total" not in payload:
        return "...", "", "...", "...", "..."
    t = payload["total"]
    pnl = t.get("pnl") or 0.0
    pnl_color = "#22c55e" if pnl >= 0 else "#f87171"
    return (
        html.Span(_fmt_money(pnl), style={"color": pnl_color}),
        f"as of {payload.get('as_of', '')[:19].replace('T', ' ')} UTC",
        _fmt_money(t.get("gross_exposure")),
        _fmt_money(t.get("net_exposure")),
        str(len(payload.get("strategies", []) or [])),
    )


# ----------------------------------------------------------------------------
# Tab content router
# ----------------------------------------------------------------------------

@app.callback(Output("tab-content", "children"), Input("tabs", "value"))
def _render_tab(tab):
    if tab == "tab-strategies":
        return _render_strategies_tab()
    if tab == "tab-fundies":
        return _render_fundies_tab()
    if tab == "tab-inbox":
        return _render_inbox_tab()
    if tab == "tab-prices":
        return _render_prices_tab()
    return html.Div("Unknown tab")


# ----------------------------------------------------------------------------
# Tab 1 - Strategies & PnL
# ----------------------------------------------------------------------------

def _render_strategies_tab() -> html.Div:
    return html.Div([
        dcc.Graph(id="pnl-chart", config={"displayModeBar": False}, style={"height": "320px"}),
        html.Div(id="strategy-cards"),
    ])


@app.callback(Output("pnl-chart", "figure"), Input("store-pnl-history", "data"))
def _update_pnl_chart(history):
    fig = go.Figure()
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        legend={"orientation": "h", "y": -0.18},
        title="Realtime PnL",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis={"showgrid": False},
        yaxis={"gridcolor": "#e2e8f0", "tickformat": "$,.0f"},
    )
    if not history:
        return fig

    total = history.get("total") or []
    if total:
        fig.add_trace(go.Scatter(
            x=[pt["ts"] for pt in total],
            y=[pt["pnl"] for pt in total],
            mode="lines", name="Total",
            line={"width": 3, "color": "#0f172a"},
        ))

    palette = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#0ea5e9"]
    for i, (name, points) in enumerate(sorted((history.get("strategies") or {}).items())):
        if not points:
            continue
        fig.add_trace(go.Scatter(
            x=[p["ts"] for p in points],
            y=[p["pnl"] for p in points],
            mode="lines",
            name=name,
            line={"width": 1.5, "dash": "dot", "color": palette[i % len(palette)]},
        ))
    return fig


@app.callback(Output("strategy-cards", "children"), Input("store-strategies", "data"))
def _update_strategy_cards(payload):
    if not payload or "strategies" not in payload:
        return html.Div("Waiting for strategy engine...", style={"padding": "24px", "color": "#64748b"})

    cards = []
    for s in payload["strategies"]:
        pnl = s.get("pnl") or 0.0
        pnl_color = "#16a34a" if pnl >= 0 else "#dc2626"
        positions = s.get("positions", [])
        rows = [
            {
                "Ticker": p["ticker"],
                "Qty": f"{p['quantity']:,}",
                "Avg Px": f"${p['avg_price']:.2f}" if p.get("avg_price") is not None else "-",
                "Last": f"${p['last_price']:.2f}" if p.get("last_price") is not None else "-",
                "Mkt Val": _fmt_money(p.get("market_value")),
                "PnL": _fmt_money(p.get("pnl")),
                "PnL %": f"{p.get('pnl_pct', 0.0):.2f}%",
            }
            for p in positions
        ]
        cards.append(html.Div(
            style={
                "background": "white",
                "borderRadius": "10px",
                "padding": "16px 20px",
                "marginTop": "16px",
                "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
            },
            children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
                    html.Div([
                        html.Div(s["name"], style={"fontWeight": 700, "fontSize": "16px"}),
                        html.Div(s.get("description", ""), style={"fontSize": "12px", "color": "#64748b"}),
                    ]),
                    html.Div([
                        html.Span(s.get("style", ""), style={
                            "background": "#e2e8f0", "padding": "4px 10px", "borderRadius": "999px",
                            "fontSize": "11px", "marginRight": "12px",
                        }),
                        html.Span(_fmt_money(pnl), style={"color": pnl_color, "fontWeight": 700, "fontSize": "18px"}),
                    ]),
                ]),
                dash_table.DataTable(
                    data=rows,
                    columns=[{"name": c, "id": c} for c in ["Ticker", "Qty", "Avg Px", "Last", "Mkt Val", "PnL", "PnL %"]],
                    style_cell={"fontFamily": "Inter, system-ui, sans-serif", "fontSize": "13px", "padding": "6px 10px"},
                    style_header={"fontWeight": 600, "background": "#f1f5f9"},
                    style_data_conditional=[
                        {"if": {"filter_query": "{PnL} contains \"-$\"", "column_id": "PnL"}, "color": "#dc2626"},
                        {"if": {"filter_query": "{PnL} !contains \"-$\"", "column_id": "PnL"}, "color": "#16a34a"},
                    ],
                    style_as_list_view=True,
                ),
            ],
        ))
    return cards


# ----------------------------------------------------------------------------
# Tab 2 - Fundies
# ----------------------------------------------------------------------------

def _render_fundies_tab() -> html.Div:
    return html.Div([
        html.Div(id="fundies-meta", style={"color": "#64748b", "fontSize": "13px", "marginBottom": "12px"}),
        html.Div(id="fundies-table"),
    ])


@app.callback(
    Output("fundies-table", "children"),
    Output("fundies-meta", "children"),
    Input("store-fundies", "data"),
)
def _update_fundies_table(payload):
    if not payload:
        return (
            html.Div(
                "No fundamentals snapshot in blob yet. Run the 'Trading Fundies Snapshot' "
                "workflow once to populate it.",
                style={"padding": "24px", "background": "white", "borderRadius": "10px", "color": "#64748b"},
            ),
            "",
        )

    rows = payload.get("rows", [])
    columns = [
        {"name": "Ticker",      "id": "ticker"},
        {"name": "Name",        "id": "name"},
        {"name": "Sector",      "id": "sector"},
        {"name": "Mkt Cap (B)", "id": "market_cap_b",       "type": "numeric", "format": {"specifier": ",.1f"}},
        {"name": "P/E",         "id": "pe_ttm",             "type": "numeric", "format": {"specifier": ",.1f"}},
        {"name": "EPS (TTM)",   "id": "eps_ttm",            "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "Rev Gr %",    "id": "rev_growth_yoy_pct", "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "GM %",        "id": "gross_margin_pct",   "type": "numeric", "format": {"specifier": ",.1f"}},
        {"name": "OM %",        "id": "op_margin_pct",      "type": "numeric", "format": {"specifier": ",.1f"}},
        {"name": "Div Y %",     "id": "div_yield_pct",      "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "D/E",         "id": "debt_to_equity",     "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "ROE %",       "id": "roe_pct",            "type": "numeric", "format": {"specifier": ",.1f"}},
        {"name": "Beta",        "id": "beta",               "type": "numeric", "format": {"specifier": ",.2f"}},
    ]

    table = dash_table.DataTable(
        data=rows,
        columns=columns,
        sort_action="native",
        filter_action="native",
        page_size=20,
        style_table={"background": "white", "borderRadius": "10px"},
        style_cell={"fontFamily": "Inter, system-ui, sans-serif", "fontSize": "13px", "padding": "8px 10px"},
        style_header={"fontWeight": 600, "background": "#f1f5f9"},
        style_as_list_view=True,
    )
    meta = (
        f"Snapshot date: {payload.get('as_of_date', '?')}  -  "
        f"As-of: {payload.get('as_of', '')[:19].replace('T', ' ')} UTC  -  "
        f"{len(rows)} symbols"
    )
    return table, meta


# ----------------------------------------------------------------------------
# Tab 3 - Vendor Inbox AI
# ----------------------------------------------------------------------------

def _render_inbox_tab() -> html.Div:
    return html.Div([
        html.Div(style={"display": "grid", "gridTemplateColumns": "minmax(360px, 460px) 1fr", "gap": "20px"}, children=[
            html.Div([
                html.Div("Recent vendor emails", style={"fontWeight": 600, "marginBottom": "10px"}),
                html.Div(id="inbox-list", style={"maxHeight": "70vh", "overflowY": "auto"}),
            ]),
            html.Div(id="inbox-detail", style={
                "background": "white", "borderRadius": "10px", "padding": "20px",
                "boxShadow": "0 1px 3px rgba(0,0,0,0.06)", "minHeight": "70vh",
            }),
        ]),
    ])


@app.callback(Output("inbox-list", "children"), Input("store-inbox-index", "data"))
def _update_inbox_list(index):
    if not index:
        return html.Div(
            "No vendor emails yet. Run the 'Trading Vendor Inbox AI' workflow to ingest a batch.",
            style={"padding": "16px", "background": "white", "borderRadius": "10px", "color": "#64748b"},
        )

    sentiment_color = {"bullish": "#16a34a", "bearish": "#dc2626", "neutral": "#64748b"}
    items = []
    for em in index:
        col = sentiment_color.get(em.get("sentiment", "neutral"), "#64748b")
        items.append(html.Div(
            id={"type": "inbox-item", "id": em["id"]},
            n_clicks=0,
            style={
                "background": "white",
                "padding": "12px 14px",
                "borderRadius": "8px",
                "marginBottom": "8px",
                "boxShadow": "0 1px 2px rgba(0,0,0,0.05)",
                "cursor": "pointer",
                "borderLeft": f"4px solid {col}",
            },
            children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between"}, children=[
                    html.Span(em.get("from_name", ""), style={"fontWeight": 600, "fontSize": "12px", "color": "#0f172a"}),
                    html.Span(em.get("received_at", "")[:16].replace("T", " "),
                              style={"fontSize": "11px", "color": "#64748b"}),
                ]),
                html.Div(em.get("subject", ""), style={"fontSize": "13px", "marginTop": "4px"}),
                html.Div(em.get("summary", ""), style={"fontSize": "12px", "marginTop": "6px", "color": "#475569"}),
                html.Div(style={"marginTop": "6px"}, children=[
                    html.Span(f"#{em.get('ticker', '-')}", style={
                        "background": "#e2e8f0", "padding": "2px 8px", "borderRadius": "999px",
                        "fontSize": "11px", "marginRight": "6px",
                    }),
                    html.Span(em.get("sentiment", "neutral").upper(), style={
                        "background": col, "color": "white", "padding": "2px 8px",
                        "borderRadius": "999px", "fontSize": "10px", "fontWeight": 600,
                    }),
                ]),
            ],
        ))
    return items


@app.callback(
    Output("store-selected-email-id", "data"),
    Input({"type": "inbox-item", "id": ALL}, "n_clicks"),
    Input("store-inbox-index", "data"),
    State("store-selected-email-id", "data"),
    prevent_initial_call=False,
)
def _track_selected_email(_clicks, index, current_id):
    triggered = ctx.triggered_id
    if isinstance(triggered, dict) and triggered.get("type") == "inbox-item":
        return triggered.get("id")
    if current_id and any(e.get("id") == current_id for e in (index or [])):
        return current_id
    if index:
        return index[0].get("id")
    return None


@app.callback(
    Output("store-inbox-detail", "data"),
    Input("store-selected-email-id", "data"),
    State("store-inbox-index", "data"),
)
def _load_email_detail(eid, index):
    if not eid or not index:
        return None
    item = next((x for x in index if x.get("id") == eid), None)
    if not item:
        return None
    return _read_blob_json(item.get("key")) or item


@app.callback(Output("inbox-detail", "children"), Input("store-inbox-detail", "data"))
def _render_inbox_detail(em):
    if not em:
        return html.Div("Select an email to see the full content + AI summary.",
                        style={"color": "#64748b"})
    summary = em.get("ai_summary") or {}
    sentiment = summary.get("sentiment", "neutral")
    sentiment_color = {"bullish": "#16a34a", "bearish": "#dc2626", "neutral": "#64748b"}.get(sentiment, "#64748b")

    return html.Div([
        html.Div(em.get("subject", "(no subject)"),
                 style={"fontSize": "20px", "fontWeight": 700, "marginBottom": "4px"}),
        html.Div(
            f"From {em.get('from_name', '')} <{em.get('from_email', '')}>  -  "
            f"{em.get('received_at', '')[:19].replace('T', ' ')} UTC  -  "
            f"#{em.get('ticker', '-')}",
            style={"color": "#64748b", "fontSize": "12px", "marginBottom": "16px"},
        ),
        html.Div(style={
            "background": "#f8fafc", "padding": "16px", "borderRadius": "10px",
            "border": "1px solid #e2e8f0", "marginBottom": "16px",
            "borderLeft": f"4px solid {sentiment_color}",
        }, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "marginBottom": "8px"}, children=[
                html.Div("AI SUMMARY", style={"fontSize": "11px", "letterSpacing": "0.12em", "color": "#64748b"}),
                html.Div(f"model: {summary.get('model', '?')}  ({sentiment})",
                         style={"fontSize": "11px", "color": "#64748b"}),
            ]),
            html.Div(summary.get("summary", ""), style={"fontSize": "14px", "marginBottom": "10px"}),
            html.Ul([html.Li(p, style={"marginBottom": "4px"}) for p in (summary.get("key_points") or [])],
                    style={"fontSize": "13px", "color": "#334155"}),
            html.Div(style={"marginTop": "10px", "fontSize": "13px"}, children=[
                html.Strong("Suggested action: "),
                html.Span(summary.get("action", "")),
            ]),
        ]),
        html.Pre(em.get("body", ""), style={
            "whiteSpace": "pre-wrap", "fontFamily": "Inter, system-ui, sans-serif",
            "fontSize": "13px", "lineHeight": "1.55", "color": "#1e293b",
            "background": "white", "padding": "12px", "borderRadius": "8px", "border": "1px solid #e2e8f0",
        }),
    ])


# ----------------------------------------------------------------------------
# Tab 4 - Live Prices
# ----------------------------------------------------------------------------

def _render_prices_tab() -> html.Div:
    return html.Div([
        html.Div(id="prices-meta", style={"color": "#64748b", "fontSize": "13px", "marginBottom": "12px"}),
        html.Div(id="prices-table"),
    ])


@app.callback(
    Output("prices-table", "children"),
    Output("prices-meta", "children"),
    Input("store-quotes", "data"),
)
def _update_prices(quotes):
    if not quotes:
        return (
            html.Div("Waiting for market data...",
                     style={"padding": "24px", "background": "white", "borderRadius": "10px"}),
            "",
        )

    columns = [
        {"name": "Ticker", "id": "ticker"},
        {"name": "Name",   "id": "name"},
        {"name": "Sector", "id": "sector"},
        {"name": "Bid",    "id": "bid",        "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "Ask",    "id": "ask",        "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "Last",   "id": "last",       "type": "numeric", "format": {"specifier": ",.2f"}},
        {"name": "Chg",    "id": "change",     "type": "numeric", "format": {"specifier": "+,.2f"}},
        {"name": "Chg %",  "id": "change_pct", "type": "numeric", "format": {"specifier": "+,.2f"}},
        {"name": "Volume", "id": "volume",     "type": "numeric", "format": {"specifier": ",.0f"}},
    ]
    table = dash_table.DataTable(
        data=quotes,
        columns=columns,
        sort_action="native",
        style_table={"background": "white", "borderRadius": "10px"},
        style_cell={"fontFamily": "Inter, system-ui, sans-serif", "fontSize": "13px", "padding": "8px 10px"},
        style_header={"fontWeight": 600, "background": "#f1f5f9"},
        style_data_conditional=[
            {"if": {"filter_query": "{change} > 0", "column_id": "change"}, "color": "#16a34a"},
            {"if": {"filter_query": "{change} < 0", "column_id": "change"}, "color": "#dc2626"},
            {"if": {"filter_query": "{change_pct} > 0", "column_id": "change_pct"}, "color": "#16a34a"},
            {"if": {"filter_query": "{change_pct} < 0", "column_id": "change_pct"}, "color": "#dc2626"},
        ],
        style_as_list_view=True,
    )
    return table, f"{len(quotes)} symbols  -  refreshes every {REFRESH_INTERVAL_MS/1000:.1f}s"


# ----------------------------------------------------------------------------
# Local entrypoint
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", "8050")))
