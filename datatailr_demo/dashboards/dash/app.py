# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""
Dash framework showcase deployed via datatailr_run_app.py (gunicorn).

Features demonstrated:
- Multi-page navigation using ``dcc.Location`` and URL routing
- Tabs with interactive Plotly charts
- Widgets (sliders, dropdowns, radio buttons, date pickers)
- Reactive callbacks that update plots in real time
- A clean, modern layout using ``dash-bootstrap-components``
"""

import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dcc, html

ACCENT = "#0072B5"
ACCENT_LIGHT = "#e8f4fa"

requests_pathname = os.environ.get("DASH_REQUESTS_PATHNAME_PREFIX", "/")

app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    requests_pathname_prefix=requests_pathname,
    title="Dash Showcase",
)

# ---------------------------------------------------------------------------
# Shared synthetic data
# ---------------------------------------------------------------------------

np.random.seed(42)

_dates = pd.date_range("2024-01-01", periods=365, freq="D")
_ts_df = pd.DataFrame(
    {
        "date": _dates,
        "revenue": np.cumsum(np.random.randn(365) * 50 + 20),
        "users": np.cumsum(np.random.randint(5, 30, 365)),
        "sessions": np.cumsum(np.random.randint(10, 80, 365)),
    }
)

_categories = ["Electronics", "Clothing", "Groceries", "Books", "Sports"]
_regions = ["North", "South", "East", "West"]
_scatter_df = pd.DataFrame(
    {
        "category": np.random.choice(_categories, 200),
        "region": np.random.choice(_regions, 200),
        "sales": np.random.exponential(500, 200),
        "profit": np.random.randn(200) * 200 + 100,
        "units": np.random.randint(1, 100, 200),
    }
)

_heatmap_data = np.random.randn(12, 7)
_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

# ---------------------------------------------------------------------------
# Style helpers
# ---------------------------------------------------------------------------

_SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "220px",
    "padding": "24px 16px",
    "background": "#1a1a2e",
    "color": "#ffffff",
    "overflow": "auto",
    "zIndex": 1000,
}

_CONTENT_STYLE = {
    "marginLeft": "240px",
    "padding": "24px 32px",
    "minHeight": "100vh",
    "background": "#f8f9fa",
}

_NAV_LINK = {
    "display": "block",
    "padding": "10px 14px",
    "margin": "4px 0",
    "borderRadius": "6px",
    "textDecoration": "none",
    "color": "#c8c8d8",
    "fontSize": "14px",
    "transition": "background 0.2s",
}

_NAV_LINK_ACTIVE = {
    **_NAV_LINK,
    "background": ACCENT,
    "color": "#ffffff",
    "fontWeight": "600",
}

_CARD = {
    "background": "#ffffff",
    "borderRadius": "10px",
    "boxShadow": "0 2px 12px rgba(0,0,0,0.06)",
    "padding": "20px",
    "marginBottom": "20px",
}

_HEADER = {
    "fontSize": "22px",
    "fontWeight": "700",
    "color": "#1a1a2e",
    "marginBottom": "4px",
}

_SUBHEADER = {
    "fontSize": "13px",
    "color": "#888",
    "marginBottom": "20px",
}

# ---------------------------------------------------------------------------
# Navigation helpers
# ---------------------------------------------------------------------------

_PAGES = [
    ("/", "Overview", "📊"),
    ("/time-series", "Time Series", "📈"),
    ("/explorer", "Data Explorer", "🔍"),
    ("/distributions", "Distributions", "📉"),
]


def _make_nav_links(active_path):
    links = []
    for href, label, icon in _PAGES:
        style = _NAV_LINK_ACTIVE if href == active_path else _NAV_LINK
        links.append(
            dcc.Link(
                f"{icon}  {label}",
                href=requests_pathname.rstrip("/") + href,
                style=style,
            )
        )
    return links


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

sidebar = html.Div(
    [
        html.H3("Dash Showcase", style={"fontWeight": "700", "marginBottom": "6px"}),
        html.P(
            "Interactive demo",
            style={"fontSize": "12px", "color": "#8888aa", "marginBottom": "28px"},
        ),
        html.Div(id="nav-links"),
    ],
    style=_SIDEBAR_STYLE,
)

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        sidebar,
        html.Div(id="page-content", style=_CONTENT_STYLE),
    ],
    style={"fontFamily": "'Inter', 'Segoe UI', system-ui, sans-serif"},
)

# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------


def _page_overview():
    latest = _ts_df.iloc[-1]
    kpi_items = [
        ("Revenue", f"${latest['revenue']:,.0f}", "↑ 12%", "#28a745"),
        ("Users", f"{latest['users']:,}", "↑ 8%", ACCENT),
        ("Sessions", f"{latest['sessions']:,}", "↑ 15%", "#6f42c1"),
        ("Avg Order", "$127", "↓ 2%", "#dc3545"),
    ]

    kpi_cards = []
    for title, value, change, color in kpi_items:
        kpi_cards.append(
            html.Div(
                [
                    html.P(
                        title,
                        style={"fontSize": "12px", "color": "#888", "margin": "0"},
                    ),
                    html.H2(value, style={"margin": "6px 0 2px", "color": "#1a1a2e"}),
                    html.Span(
                        change,
                        style={"fontSize": "13px", "color": color, "fontWeight": "600"},
                    ),
                ],
                style={
                    **_CARD,
                    "flex": "1",
                    "minWidth": "180px",
                    "textAlign": "center",
                },
            )
        )

    fig_revenue = px.area(
        _ts_df,
        x="date",
        y="revenue",
        title="Cumulative Revenue",
        color_discrete_sequence=[ACCENT],
    )
    fig_revenue.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font_family="Inter, sans-serif",
    )

    fig_bar = px.bar(
        _scatter_df.groupby("category", as_index=False)["sales"].sum(),
        x="category",
        y="sales",
        title="Sales by Category",
        color="category",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_bar.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        font_family="Inter, sans-serif",
    )

    return html.Div(
        [
            html.H1("Overview", style=_HEADER),
            html.P("Key metrics and summary charts", style=_SUBHEADER),
            html.Div(
                kpi_cards, style={"display": "flex", "gap": "16px", "flexWrap": "wrap"}
            ),
            html.Div(
                [
                    html.Div(
                        dcc.Graph(figure=fig_revenue, config={"displayModeBar": False}),
                        style={**_CARD, "flex": "2", "minWidth": "400px"},
                    ),
                    html.Div(
                        dcc.Graph(figure=fig_bar, config={"displayModeBar": False}),
                        style={**_CARD, "flex": "1", "minWidth": "300px"},
                    ),
                ],
                style={"display": "flex", "gap": "16px", "flexWrap": "wrap"},
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Page: Time Series (tabs + widgets)
# ---------------------------------------------------------------------------


def _page_time_series():
    return html.Div(
        [
            html.H1("Time Series", style=_HEADER),
            html.P("Explore trends with interactive controls", style=_SUBHEADER),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label(
                                "Metric",
                                style={"fontWeight": "600", "fontSize": "13px"},
                            ),
                            dcc.Dropdown(
                                id="ts-metric",
                                options=[
                                    {"label": "Revenue", "value": "revenue"},
                                    {"label": "Users", "value": "users"},
                                    {"label": "Sessions", "value": "sessions"},
                                ],
                                value="revenue",
                                clearable=False,
                                style={"marginBottom": "16px"},
                            ),
                            html.Label(
                                "Rolling window",
                                style={"fontWeight": "600", "fontSize": "13px"},
                            ),
                            dcc.Slider(
                                id="ts-window",
                                min=1,
                                max=60,
                                step=1,
                                value=7,
                                marks={1: "1", 7: "7", 14: "14", 30: "30", 60: "60"},
                                tooltip={"placement": "bottom"},
                            ),
                            html.Label(
                                "Chart type",
                                style={
                                    "fontWeight": "600",
                                    "fontSize": "13px",
                                    "marginTop": "16px",
                                },
                            ),
                            dcc.RadioItems(
                                id="ts-chart-type",
                                options=[
                                    {"label": " Line", "value": "line"},
                                    {"label": " Area", "value": "area"},
                                    {"label": " Bar", "value": "bar"},
                                ],
                                value="line",
                                labelStyle={
                                    "display": "block",
                                    "marginBottom": "4px",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                        style={**_CARD, "width": "260px", "flexShrink": "0"},
                    ),
                    html.Div(
                        [
                            dcc.Tabs(
                                id="ts-tabs",
                                value="raw",
                                children=[
                                    dcc.Tab(label="Raw data", value="raw"),
                                    dcc.Tab(label="Rolling average", value="rolling"),
                                    dcc.Tab(label="Daily change", value="diff"),
                                ],
                                style={"marginBottom": "12px"},
                            ),
                            dcc.Graph(id="ts-graph", config={"displayModeBar": False}),
                        ],
                        style={**_CARD, "flex": "1"},
                    ),
                ],
                style={"display": "flex", "gap": "16px", "alignItems": "flex-start"},
            ),
        ]
    )


@callback(
    Output("ts-graph", "figure"),
    Input("ts-metric", "value"),
    Input("ts-window", "value"),
    Input("ts-chart-type", "value"),
    Input("ts-tabs", "value"),
)
def _update_ts(metric, window, chart_type, tab):
    series = _ts_df[["date", metric]].copy()

    if tab == "rolling":
        series[metric] = series[metric].rolling(window, min_periods=1).mean()
        title = f"{metric.title()} — {window}-day rolling average"
    elif tab == "diff":
        series[metric] = series[metric].diff().fillna(0)
        title = f"{metric.title()} — daily change"
    else:
        title = f"{metric.title()} — raw values"

    plot_fn = {"line": px.line, "area": px.area, "bar": px.bar}[chart_type]
    fig = plot_fn(
        series, x="date", y=metric, title=title, color_discrete_sequence=[ACCENT]
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font_family="Inter, sans-serif",
    )
    return fig


# ---------------------------------------------------------------------------
# Page: Data Explorer (scatter + heatmap tabs)
# ---------------------------------------------------------------------------


def _page_explorer():
    return html.Div(
        [
            html.H1("Data Explorer", style=_HEADER),
            html.P("Scatter plots, heatmaps, and cross-filtering", style=_SUBHEADER),
            dcc.Tabs(
                id="explorer-tabs",
                value="scatter",
                children=[
                    dcc.Tab(label="Scatter plot", value="scatter"),
                    dcc.Tab(label="Heatmap", value="heatmap"),
                    dcc.Tab(label="Parallel coordinates", value="parallel"),
                ],
                style={"marginBottom": "12px"},
            ),
            html.Div(id="explorer-controls", style={"marginBottom": "12px"}),
            dcc.Graph(id="explorer-graph", config={"displayModeBar": False}),
        ],
        style=_CARD,
    )


@callback(
    Output("explorer-controls", "children"),
    Input("explorer-tabs", "value"),
)
def _explorer_controls(tab):
    if tab == "scatter":
        return html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "X axis", style={"fontWeight": "600", "fontSize": "13px"}
                        ),
                        dcc.Dropdown(
                            id="scatter-x",
                            options=["sales", "profit", "units"],
                            value="sales",
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Label(
                            "Y axis", style={"fontWeight": "600", "fontSize": "13px"}
                        ),
                        dcc.Dropdown(
                            id="scatter-y",
                            options=["sales", "profit", "units"],
                            value="profit",
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Label(
                            "Color by", style={"fontWeight": "600", "fontSize": "13px"}
                        ),
                        dcc.Dropdown(
                            id="scatter-color",
                            options=["category", "region"],
                            value="category",
                            clearable=False,
                        ),
                    ],
                    style={"flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "16px"},
        )
    return html.Div()


@callback(
    Output("explorer-graph", "figure"),
    Input("explorer-tabs", "value"),
    Input("scatter-x", "value"),
    Input("scatter-y", "value"),
    Input("scatter-color", "value"),
)
def _update_explorer(tab, scatter_x, scatter_y, scatter_color):
    if tab == "scatter":
        fig = px.scatter(
            _scatter_df,
            x=scatter_x,
            y=scatter_y,
            color=scatter_color,
            size="units",
            hover_data=["category", "region"],
            title=f"{scatter_y.title()} vs {scatter_x.title()}",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
    elif tab == "heatmap":
        fig = go.Figure(
            go.Heatmap(
                z=_heatmap_data,
                x=_days,
                y=_months,
                colorscale="Blues",
                hoverongaps=False,
            )
        )
        fig.update_layout(title="Activity Heatmap (Month × Day)")
    else:
        fig = px.parallel_coordinates(
            _scatter_df,
            dimensions=["sales", "profit", "units"],
            color="sales",
            color_continuous_scale="Blues",
            title="Parallel Coordinates — Sales, Profit, Units",
        )

    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font_family="Inter, sans-serif",
        height=500,
    )
    return fig


# ---------------------------------------------------------------------------
# Page: Distributions
# ---------------------------------------------------------------------------


def _page_distributions():
    return html.Div(
        [
            html.H1("Distributions", style=_HEADER),
            html.P("Histograms, box plots, and violin charts", style=_SUBHEADER),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label(
                                "Variable",
                                style={"fontWeight": "600", "fontSize": "13px"},
                            ),
                            dcc.Dropdown(
                                id="dist-var",
                                options=["sales", "profit", "units"],
                                value="sales",
                                clearable=False,
                                style={"marginBottom": "16px"},
                            ),
                            html.Label(
                                "Group by",
                                style={"fontWeight": "600", "fontSize": "13px"},
                            ),
                            dcc.Dropdown(
                                id="dist-group",
                                options=[
                                    {"label": "None", "value": "none"},
                                    {"label": "Category", "value": "category"},
                                    {"label": "Region", "value": "region"},
                                ],
                                value="none",
                                clearable=False,
                                style={"marginBottom": "16px"},
                            ),
                            html.Label(
                                "Bins", style={"fontWeight": "600", "fontSize": "13px"}
                            ),
                            dcc.Slider(
                                id="dist-bins",
                                min=10,
                                max=80,
                                step=5,
                                value=30,
                                marks={10: "10", 30: "30", 50: "50", 80: "80"},
                            ),
                        ],
                        style={**_CARD, "width": "240px", "flexShrink": "0"},
                    ),
                    html.Div(
                        [
                            dcc.Tabs(
                                id="dist-tabs",
                                value="histogram",
                                children=[
                                    dcc.Tab(label="Histogram", value="histogram"),
                                    dcc.Tab(label="Box plot", value="box"),
                                    dcc.Tab(label="Violin", value="violin"),
                                ],
                                style={"marginBottom": "12px"},
                            ),
                            dcc.Graph(
                                id="dist-graph", config={"displayModeBar": False}
                            ),
                        ],
                        style={**_CARD, "flex": "1"},
                    ),
                ],
                style={"display": "flex", "gap": "16px", "alignItems": "flex-start"},
            ),
        ]
    )


@callback(
    Output("dist-graph", "figure"),
    Input("dist-var", "value"),
    Input("dist-group", "value"),
    Input("dist-bins", "value"),
    Input("dist-tabs", "value"),
)
def _update_dist(var, group, bins, tab):
    color_col = group if group != "none" else None

    if tab == "histogram":
        fig = px.histogram(
            _scatter_df,
            x=var,
            color=color_col,
            nbins=bins,
            title=f"Distribution of {var.title()}",
            color_discrete_sequence=px.colors.qualitative.Set2,
            barmode="overlay",
            opacity=0.75,
        )
    elif tab == "box":
        fig = px.box(
            _scatter_df,
            y=var,
            x=color_col,
            color=color_col,
            title=f"Box plot — {var.title()}",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
    else:
        fig = px.violin(
            _scatter_df,
            y=var,
            x=color_col,
            color=color_col,
            box=True,
            points="all",
            title=f"Violin — {var.title()}",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )

    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font_family="Inter, sans-serif",
        height=480,
    )
    return fig


# ---------------------------------------------------------------------------
# Router callback
# ---------------------------------------------------------------------------

_PAGE_MAP = {
    "/": _page_overview,
    "/time-series": _page_time_series,
    "/explorer": _page_explorer,
    "/distributions": _page_distributions,
}


@callback(
    Output("page-content", "children"),
    Output("nav-links", "children"),
    Input("url", "pathname"),
)
def _route(pathname):
    prefix = requests_pathname.rstrip("/")
    relative = pathname
    if prefix and pathname and pathname.startswith(prefix):
        relative = pathname[len(prefix) :] or "/"

    page_fn = _PAGE_MAP.get(relative, _page_overview)
    return page_fn(), _make_nav_links(relative)


# Expose the underlying WSGI callable for gunicorn.
server = app.server

if __name__ == "__main__":
    app.run(debug=True)
