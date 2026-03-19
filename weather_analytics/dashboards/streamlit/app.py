"""Global Weather Analytics Dashboard -- Streamlit app.

Sections:
  1. World Overview   -- map + global stats
  2. City Explorer    -- per-city detail charts
  3. Rankings         -- sortable top/bottom tables
  4. Alerts           -- severity-coded alert list
  5. Forecast Trends  -- 7-day outlook and biggest changes
  6. Run Pipeline     -- trigger a new run with custom params
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st


# ---------------------------------------------------------------------------
# Service URL helper (same pattern as existing demo)
# ---------------------------------------------------------------------------

def _service_url() -> str:
    if os.getenv("DATATAILR_JOB_TYPE") == "workspace":
        return "http://localhost:1024"
    return "http://weather-analytics-service:8080"


def _api(path: str, **params) -> dict | list | None:
    """GET helper -- returns parsed JSON or None on error."""
    try:
        r = requests.get(f"{_service_url()}{path}", params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def _api_post(path: str, json_body: dict | None = None) -> dict | None:
    try:
        r = requests.post(f"{_service_url()}{path}", json=json_body, timeout=10)
        return r.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Global Weather Analytics",
        page_icon="🌍",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Global Weather Analytics")

    service_status = _api("/")
    if service_status and service_status.get("data_loaded"):
        st.sidebar.success("Data service: connected")
    elif service_status:
        st.sidebar.warning("Service up, but no data loaded yet. Trigger a run below.")
    else:
        st.sidebar.error("Cannot reach weather analytics service.")

    section = st.sidebar.radio(
        "Navigate",
        [
            "World Overview",
            "City Explorer",
            "Rankings",
            "Alerts",
            "Forecast Trends",
            "Run Pipeline",
        ],
    )

    if section == "World Overview":
        _section_world_overview()
    elif section == "City Explorer":
        _section_city_explorer()
    elif section == "Rankings":
        _section_rankings()
    elif section == "Alerts":
        _section_alerts()
    elif section == "Forecast Trends":
        _section_forecast_trends()
    elif section == "Run Pipeline":
        _section_run_pipeline()


# ===================================================================
# Section 1 -- World Overview
# ===================================================================

def _section_world_overview():
    st.header("World Overview")

    stats = _api("/stats")
    if not stats:
        st.info("No data available. Use **Run Pipeline** to fetch weather data.")
        return

    ga = stats.get("global_aggregates", {})
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Cities Tracked", ga.get("total_cities", 0))
    col2.metric("Avg Temperature", f"{ga.get('global_avg_temp', '—')} °C")
    col3.metric("Avg Humidity", f"{ga.get('global_avg_humidity', '—')} %")
    col4.metric("Avg Pressure", f"{ga.get('global_avg_pressure', '—')} hPa")
    col5.metric("Avg Wind", f"{ga.get('global_avg_wind', '—')} km/h")

    cities_resp = _api("/cities")
    if not cities_resp:
        return

    city_names = [c["city"] for c in cities_resp.get("cities", [])]

    map_data: list[dict] = []
    for name in city_names:
        rec = _api("/current", city=name)
        if rec:
            cur = rec.get("current", {})
            map_data.append({
                "city": rec["city"],
                "lat": rec["lat"],
                "lon": rec["lon"],
                "temperature": cur.get("temperature_2m"),
                "humidity": cur.get("relative_humidity_2m"),
                "wind": cur.get("wind_speed_10m"),
                "category": rec.get("weather_category", "unknown"),
                "country": rec.get("country", ""),
                "continent": rec.get("continent", ""),
            })

    if not map_data:
        st.warning("Could not load city data for map.")
        return

    df = pd.DataFrame(map_data)
    df = df.dropna(subset=["temperature"])

    fig = px.scatter_geo(
        df,
        lat="lat",
        lon="lon",
        color="temperature",
        size=df["temperature"].apply(lambda t: max(abs(t) + 5, 5)),
        hover_name="city",
        hover_data=["country", "temperature", "humidity", "wind", "category"],
        color_continuous_scale="RdYlBu_r",
        projection="natural earth",
        title="Current Temperatures Worldwide",
    )
    fig.update_layout(height=550, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Continent Summaries")
    cont = stats.get("continent_summaries", {})
    if cont:
        cont_df = pd.DataFrame([
            {"Continent": k, **v} for k, v in cont.items()
        ]).sort_values("num_cities", ascending=False)
        st.dataframe(cont_df, use_container_width=True, hide_index=True)

    st.subheader("Cross-Variable Correlations")
    corr = stats.get("correlations", {})
    if corr:
        corr_df = pd.DataFrame([
            {"Variable Pair": k.replace("_", " ").title(), "Pearson r": v}
            for k, v in corr.items()
            if v is not None
        ])
        st.dataframe(corr_df, use_container_width=True, hide_index=True)


# ===================================================================
# Section 2 -- City Explorer
# ===================================================================

def _section_city_explorer():
    st.header("City Explorer")

    cities_resp = _api("/cities")
    if not cities_resp:
        st.info("No data available. Trigger a pipeline run first.")
        return

    city_names = sorted(c["city"] for c in cities_resp.get("cities", []))
    selected = st.selectbox("Select a city", city_names, index=0)

    rec = _api("/current", city=selected)
    fc = _api("/forecast", city=selected)

    if not rec:
        st.error(f"Could not load data for {selected}")
        return

    cur = rec.get("current", {})
    derived = rec.get("derived", {})

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Temperature", f"{cur.get('temperature_2m', '—')} °C")
    col2.metric("Feels Like", f"{cur.get('apparent_temperature', '—')} °C")
    col3.metric("Humidity", f"{cur.get('relative_humidity_2m', '—')} %")
    col4.metric("Wind", f"{cur.get('wind_speed_10m', '—')} km/h")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Pressure", f"{cur.get('pressure_msl', '—')} hPa")
    col6.metric("Cloud Cover", f"{cur.get('cloud_cover', '—')} %")
    col7.metric("Heat Index", f"{derived.get('heat_index', '—')} °C")
    col8.metric("Wind Chill", f"{derived.get('wind_chill', '—')} °C")

    st.caption(f"Category: **{rec.get('weather_category', 'unknown')}** | "
               f"Elevation: {rec.get('elevation', '—')} m | "
               f"Dew Point: {derived.get('dew_point', '—')} °C")

    if fc and fc.get("daily"):
        daily = fc["daily"]
        days = daily.get("time", [])
        highs = daily.get("temperature_2m_max", [])
        lows = daily.get("temperature_2m_min", [])

        if days:
            st.subheader("7-Day Temperature Forecast")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=days, y=highs, mode="lines+markers",
                name="Daily High", line=dict(color="#ef5350", width=2),
            ))
            fig.add_trace(go.Scatter(
                x=days, y=lows, mode="lines+markers",
                name="Daily Low", line=dict(color="#42a5f5", width=2),
            ))
            fig.update_layout(
                yaxis_title="Temperature (°C)", xaxis_title="Date",
                height=350, margin=dict(l=40, r=20, t=30, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

        precip = daily.get("precipitation_sum", [])
        if days and precip:
            st.subheader("7-Day Precipitation")
            fig_p = px.bar(
                x=days, y=precip,
                labels={"x": "Date", "y": "Precipitation (mm)"},
            )
            fig_p.update_layout(height=300, margin=dict(l=40, r=20, t=30, b=40))
            st.plotly_chart(fig_p, use_container_width=True)

        wind_max = daily.get("wind_speed_10m_max", [])
        wind_dir = daily.get("wind_direction_10m_dominant", [])
        if wind_dir and wind_max:
            st.subheader("Wind Rose (7-Day Dominant Direction)")
            fig_w = go.Figure(go.Barpolar(
                r=wind_max,
                theta=wind_dir,
                marker_color=wind_max,
                marker_colorscale="Viridis",
                opacity=0.8,
            ))
            fig_w.update_layout(
                height=400,
                polar=dict(radialaxis=dict(visible=True, title="km/h")),
                margin=dict(l=40, r=40, t=30, b=30),
            )
            st.plotly_chart(fig_w, use_container_width=True)


# ===================================================================
# Section 3 -- Rankings
# ===================================================================

def _section_rankings():
    st.header("City Rankings")

    metrics = {
        "hottest": "Hottest Cities",
        "coldest": "Coldest Cities",
        "wettest_7d": "Wettest (7-day precip)",
        "windiest": "Windiest Cities",
        "most_comfortable": "Most Comfortable",
        "least_comfortable": "Least Comfortable",
    }

    tabs = st.tabs(list(metrics.values()))

    for tab, (metric_key, label) in zip(tabs, metrics.items()):
        with tab:
            limit = st.slider(f"Top N — {label}", 5, 15, 10, key=f"slider_{metric_key}")
            data = _api("/rankings", metric=metric_key, limit=limit)
            if data and data.get("rankings"):
                df = pd.DataFrame(data["rankings"])
                df.index = range(1, len(df) + 1)
                df.index.name = "Rank"
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No ranking data available.")


# ===================================================================
# Section 4 -- Alerts
# ===================================================================

_SEVERITY_COLORS = {"high": "🔴", "medium": "🟠", "low": "🟡"}


def _section_alerts():
    st.header("Weather Alerts")

    data = _api("/alerts")
    if not data:
        st.info("No alert data available.")
        return

    alerts = data.get("alerts", [])
    st.metric("Active Alerts", len(alerts))

    if not alerts:
        st.success("No active weather alerts worldwide.")
        return

    severity_filter = st.multiselect(
        "Filter by severity",
        ["high", "medium", "low"],
        default=["high", "medium"],
    )

    filtered = [a for a in alerts if a["severity"] in severity_filter]

    for a in filtered:
        icon = _SEVERITY_COLORS.get(a["severity"], "⚪")
        with st.container(border=True):
            cols = st.columns([1, 3, 2])
            cols[0].write(f"### {icon}")
            cols[1].write(f"**{a['city']}** — {a['message']}")
            cols[2].write(f"Type: `{a['type']}` | Severity: `{a['severity']}`")


# ===================================================================
# Section 5 -- Forecast Trends
# ===================================================================

def _section_forecast_trends():
    st.header("Forecast Trends")

    stats = _api("/stats")
    if not stats:
        st.info("No data available.")
        return

    trend_summary = stats.get("trend_summary", {})
    if trend_summary:
        st.subheader("Global Trend Distribution")
        fig_pie = px.pie(
            names=list(trend_summary.keys()),
            values=list(trend_summary.values()),
            title="7-Day Temperature Trends Across All Cities",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_pie.update_layout(height=350)
        st.plotly_chart(fig_pie, use_container_width=True)

    col_w, col_c = st.columns(2)

    with col_w:
        st.subheader("Biggest Warming")
        warming = _api("/rankings", metric="hottest", limit=10)
        if warming and warming.get("rankings"):
            cities_resp = _api("/cities")
            if cities_resp:
                warming_data = []
                for entry in warming["rankings"][:10]:
                    fc = _api("/forecast", city=entry["city"])
                    if fc and fc.get("temp_change") is not None:
                        warming_data.append({
                            "City": fc["city"],
                            "Trend": fc.get("trend", ""),
                            "Temp Change (°C)": fc["temp_change"],
                            "7d Precip (mm)": fc.get("precip_total_7d", 0),
                        })
                if warming_data:
                    st.dataframe(pd.DataFrame(warming_data), use_container_width=True, hide_index=True)

    with col_c:
        st.subheader("Biggest Cooling")
        cooling = _api("/rankings", metric="coldest", limit=10)
        if cooling and cooling.get("rankings"):
            cooling_data = []
            for entry in cooling["rankings"][:10]:
                fc = _api("/forecast", city=entry["city"])
                if fc and fc.get("temp_change") is not None:
                    cooling_data.append({
                        "City": fc["city"],
                        "Trend": fc.get("trend", ""),
                        "Temp Change (°C)": fc["temp_change"],
                        "7d Precip (mm)": fc.get("precip_total_7d", 0),
                    })
            if cooling_data:
                st.dataframe(pd.DataFrame(cooling_data), use_container_width=True, hide_index=True)

    st.subheader("City Forecast Detail")
    cities_resp = _api("/cities")
    if cities_resp:
        city_names = sorted(c["city"] for c in cities_resp.get("cities", []))
        sel = st.selectbox("Select city for forecast", city_names, key="fc_city")
        fc = _api("/forecast", city=sel)
        if fc and fc.get("daily"):
            daily = fc["daily"]
            days = daily.get("time", [])
            highs = daily.get("temperature_2m_max", [])
            lows = daily.get("temperature_2m_min", [])
            if days:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=days, y=highs, name="High", line=dict(color="#ef5350")))
                fig.add_trace(go.Scatter(x=days, y=lows, name="Low", line=dict(color="#42a5f5")))
                fig.update_layout(yaxis_title="°C", height=350, margin=dict(l=40, r=20, t=30, b=40))
                st.plotly_chart(fig, use_container_width=True)

            st.write(f"**Trend:** {fc.get('trend', '—')} | "
                     f"**Change:** {fc.get('temp_change', '—')} °C | "
                     f"**7d Precip:** {fc.get('precip_total_7d', '—')} mm | "
                     f"**Max Wind:** {fc.get('max_wind_7d', '—')} km/h")


# ===================================================================
# Section 6 -- Run Pipeline
# ===================================================================

def _section_run_pipeline():
    st.header("Run Pipeline")
    st.write("Trigger a new weather data processing pipeline run with custom parameters.")

    with st.form("trigger_form"):
        num_cities = st.slider("Number of cities", 10, 192, 192, step=10)
        include_forecast = st.checkbox("Include 7-day forecast data", value=True)
        submitted = st.form_submit_button("Start Pipeline Run")

    if submitted:
        with st.spinner("Starting pipeline..."):
            result = _api_post("/trigger", {
                "num_cities": num_cities,
                "include_forecast": include_forecast,
            })
            if result and result.get("status") == "pipeline_started":
                st.success(
                    f"Pipeline started for {num_cities} cities. "
                    "Data will be available shortly — refresh the page in a minute."
                )
            else:
                st.error("Failed to trigger pipeline. Check service status.")

    st.divider()
    st.subheader("Service Status")
    status = _api("/")
    if status:
        st.json(status)
    else:
        st.error("Cannot reach the weather analytics service.")


if __name__ == "__main__":
    main()
