API_BASE = "http://weather-analytics-api"

VARIABLE_LABELS = {
    "temperature_2m": "Temperature (°C)",
    "relative_humidity_2m": "Relative Humidity (%)",
    "precipitation": "Precipitation (mm)",
    "wind_speed_10m": "Wind Speed (km/h)",
    "pressure_msl": "Sea-Level Pressure (hPa)",
    "cloud_cover": "Cloud Cover (%)",
    "dew_point_2m": "Dew Point (°C)",
    "apparent_temperature": "Apparent Temperature (°C)",
}


def api_get(path, params=None):
    import requests
    import streamlit as st
    try:
        resp = requests.get(f"{API_BASE}{path}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        st.error(f"API request failed: {e}")
        return None


def api_post(path, json_data=None):
    import requests
    import streamlit as st
    try:
        resp = requests.post(f"{API_BASE}{path}", json=json_data, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        st.error(f"API request failed: {e}")
        return None


def page_overview():
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    st.header("Global Overview")

    metadata = api_get("/run-metadata")
    if metadata:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Stations", metadata.get("total_stations", "N/A"))
        c2.metric("Stats Records", metadata.get("total_records_in_stats", "N/A"))
        c3.metric("Anomalies Detected", metadata.get("total_anomalies", "N/A"))
        c4.metric("Last Run", metadata.get("run_timestamp", "N/A")[:19].replace("T", " "))

    stations = api_get("/stations")
    if not stations:
        st.warning("No station data available. Run the pipeline first.")
        return

    stats_data = api_get("/statistics")
    if stats_data:
        stats_df = pd.DataFrame(stats_data)
        if "temperature_2m_mean" in stats_df.columns:
            fig = px.scatter_mapbox(
                stats_df,
                lat="lat",
                lon="lon",
                color="temperature_2m_mean",
                size=abs(stats_df["temperature_2m_mean"] - stats_df["temperature_2m_mean"].min()) + 2,
                hover_name="city",
                hover_data={
                    "continent": True,
                    "temperature_2m_mean": ":.1f",
                    "temperature_2m_min": ":.1f",
                    "temperature_2m_max": ":.1f",
                    "lat": ":.2f",
                    "lon": ":.2f",
                },
                color_continuous_scale="RdYlBu_r",
                mapbox_style="carto-positron",
                zoom=1,
                height=600,
                title="Average Temperature by Station",
            )
            fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)

    continent_data = api_get("/continent-statistics")
    if continent_data:
        cont_df = pd.DataFrame(continent_data)
        st.subheader("Continental Summary")

        if "temperature_2m_mean" in cont_df.columns:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(
                    cont_df,
                    x="continent",
                    y="temperature_2m_mean",
                    color="continent",
                    title="Avg Temperature by Continent",
                    labels={"temperature_2m_mean": "Temperature (°C)", "continent": ""},
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "precipitation_mean" in cont_df.columns:
                    fig = px.bar(
                        cont_df,
                        x="continent",
                        y="precipitation_mean",
                        color="continent",
                        title="Avg Precipitation by Continent",
                        labels={"precipitation_mean": "Precipitation (mm)", "continent": ""},
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)


def page_city_explorer():
    import streamlit as st
    import pandas as pd
    import plotly.graph_objects as go

    st.header("City Explorer")

    stations = api_get("/stations")
    if not stations:
        st.warning("No station data available. Run the pipeline first.")
        return

    stations_df = pd.DataFrame(stations)
    continents = sorted(stations_df["continent"].unique())

    col1, col2 = st.columns([1, 2])
    with col1:
        continent = st.selectbox("Continent", ["All"] + continents)
    with col2:
        if continent == "All":
            city_list = sorted(stations_df["city"].unique())
        else:
            city_list = sorted(stations_df[stations_df["continent"] == continent]["city"].unique())
        city = st.selectbox("City", city_list)

    weather = api_get("/weather", {"city": city, "limit": 50000})
    if not weather or not weather.get("data"):
        st.info(f"No weather data for {city}.")
        return

    df = pd.DataFrame(weather["data"])
    df["time"] = pd.to_datetime(df["time"])
    st.caption(f"Showing {len(df):,} hourly records for {city}")

    available_vars = [v for v in VARIABLE_LABELS if v in df.columns]
    selected_vars = st.multiselect(
        "Variables to plot",
        available_vars,
        default=available_vars[:4],
        format_func=lambda v: VARIABLE_LABELS.get(v, v),
    )

    anomaly_data = api_get("/anomalies", {"city": city, "limit": 50000})
    anomaly_df = pd.DataFrame(anomaly_data["data"]) if anomaly_data and anomaly_data.get("data") else pd.DataFrame()
    if not anomaly_df.empty and "time" in anomaly_df.columns:
        anomaly_df["time"] = pd.to_datetime(anomaly_df["time"])

    for var in selected_vars:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["time"], y=df[var],
            mode="lines", name=VARIABLE_LABELS.get(var, var),
            line=dict(width=1),
        ))

        if not anomaly_df.empty and var in anomaly_df["variable"].values:
            var_anom = anomaly_df[anomaly_df["variable"] == var]
            color_map = {"mild": "gold", "moderate": "orange", "severe": "red"}
            for sev in ["mild", "moderate", "severe"]:
                sev_data = var_anom[var_anom["severity"] == sev]
                if not sev_data.empty:
                    fig.add_trace(go.Scatter(
                        x=sev_data["time"], y=sev_data["value"],
                        mode="markers", name=f"{sev.title()} Anomaly",
                        marker=dict(color=color_map[sev], size=8, symbol="diamond"),
                        hovertemplate=f"z-score: %{{customdata:.2f}}<extra>{sev}</extra>",
                        customdata=sev_data["z_score"],
                    ))

        fig.update_layout(
            title=f"{VARIABLE_LABELS.get(var, var)} - {city}",
            xaxis_title="Time",
            yaxis_title=VARIABLE_LABELS.get(var, var),
            height=350,
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)


def page_continental_comparison():
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    st.header("Continental Comparison")

    stats = api_get("/statistics")
    if not stats:
        st.warning("No statistics available. Run the pipeline first.")
        return

    stats_df = pd.DataFrame(stats)
    available_vars = [v for v in VARIABLE_LABELS if f"{v}_mean" in stats_df.columns]

    selected_var = st.selectbox(
        "Variable",
        available_vars,
        format_func=lambda v: VARIABLE_LABELS.get(v, v),
    )

    col1, col2 = st.columns(2)
    with col1:
        fig = px.box(
            stats_df,
            x="continent",
            y=f"{selected_var}_mean",
            color="continent",
            title=f"{VARIABLE_LABELS[selected_var]} Distribution by Continent",
            labels={f"{selected_var}_mean": VARIABLE_LABELS[selected_var], "continent": ""},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        continent_means = stats_df.groupby("continent")[f"{selected_var}_mean"].mean().reset_index()
        fig = px.bar(
            continent_means.sort_values(f"{selected_var}_mean", ascending=False),
            x="continent",
            y=f"{selected_var}_mean",
            color="continent",
            title=f"Average {VARIABLE_LABELS[selected_var]} by Continent",
            labels={f"{selected_var}_mean": VARIABLE_LABELS[selected_var], "continent": ""},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    if "temp_trend_per_day" in stats_df.columns and selected_var == "temperature_2m":
        st.subheader("Temperature Trend (°C/day)")
        top_warming = stats_df.nlargest(10, "temp_trend_per_day")[["city", "continent", "temp_trend_per_day"]]
        top_cooling = stats_df.nsmallest(10, "temp_trend_per_day")[["city", "continent", "temp_trend_per_day"]]
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Top 10 Warming")
            st.dataframe(top_warming, hide_index=True, use_container_width=True)
        with c2:
            st.caption("Top 10 Cooling")
            st.dataframe(top_cooling, hide_index=True, use_container_width=True)

    if "total_precipitation" in stats_df.columns and selected_var == "precipitation":
        st.subheader("Total Precipitation (mm)")
        top_wet = stats_df.nlargest(15, "total_precipitation")[["city", "continent", "total_precipitation"]]
        fig = px.bar(
            top_wet,
            x="city",
            y="total_precipitation",
            color="continent",
            title="Top 15 Wettest Cities",
            labels={"total_precipitation": "Total Precipitation (mm)", "city": ""},
        )
        st.plotly_chart(fig, use_container_width=True)


def page_anomaly_monitor():
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    st.header("Anomaly Monitor")

    metadata = api_get("/run-metadata")
    if metadata and metadata.get("anomaly_summary"):
        summary = metadata["anomaly_summary"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Anomalies", summary.get("total", 0))
        c2.metric("Severe", summary.get("severe", 0))
        c3.metric("Moderate", summary.get("moderate", 0))
        c4.metric("Mild", summary.get("mild", 0))

    col1, col2, col3 = st.columns(3)
    with col1:
        severity_filter = st.selectbox("Severity", ["All", "severe", "moderate", "mild"])
    with col2:
        variable_filter = st.selectbox("Variable", ["All"] + list(VARIABLE_LABELS.keys()),
                                       format_func=lambda v: "All" if v == "All" else VARIABLE_LABELS.get(v, v))
    with col3:
        continent_filter = st.selectbox("Continent Filter", ["All", "North America", "Europe", "Asia",
                                                              "South America", "Africa", "Oceania"])

    params = {"limit": 5000}
    if severity_filter != "All":
        params["severity"] = severity_filter
    if variable_filter != "All":
        params["variable"] = variable_filter
    if continent_filter != "All":
        params["continent"] = continent_filter

    anomaly_data = api_get("/anomalies", params)
    if not anomaly_data or not anomaly_data.get("data"):
        st.info("No anomalies found with current filters.")
        return

    df = pd.DataFrame(anomaly_data["data"])
    st.caption(f"Showing {len(df):,} of {anomaly_data['total']:,} anomalies")

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])

    color_map = {"severe": "red", "moderate": "orange", "mild": "gold"}
    if "lat" in df.columns and "lon" in df.columns:
        fig = px.scatter_mapbox(
            df,
            lat="lat",
            lon="lon",
            color="severity",
            color_discrete_map=color_map,
            hover_name="city",
            hover_data={"variable": True, "value": ":.1f", "z_score": ":.2f", "lat": False, "lon": False},
            mapbox_style="carto-positron",
            zoom=1,
            height=500,
            title="Anomaly Locations",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    display_cols = ["city", "continent", "time", "variable", "value", "z_score", "severity"]
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[display_cols].sort_values("z_score", key=abs, ascending=False),
        hide_index=True,
        use_container_width=True,
        height=400,
    )


def page_pipeline_control():
    import streamlit as st

    st.header("Pipeline Control")

    metadata = api_get("/run-metadata")
    if metadata:
        st.subheader("Last Run")
        c1, c2, c3 = st.columns(3)
        c1.metric("Status", metadata.get("status", "unknown").title())
        c2.metric("Stations", metadata.get("total_stations", "N/A"))
        c3.metric("Anomalies", metadata.get("total_anomalies", "N/A"))

        ts = metadata.get("run_timestamp", "")
        if ts:
            st.caption(f"Run completed at: {ts[:19].replace('T', ' ')} UTC")

        if metadata.get("anomaly_summary"):
            with st.expander("Anomaly Breakdown"):
                summary = metadata["anomaly_summary"]
                if "by_variable" in summary:
                    st.write("**By Variable:**")
                    st.json(summary["by_variable"])
                if "by_continent" in summary:
                    st.write("**By Continent:**")
                    st.json(summary["by_continent"])
    else:
        st.info("No previous pipeline run found.")

    st.divider()
    st.subheader("Trigger New Run")

    with st.form("trigger_run"):
        days_back = st.slider("Days of historical data", min_value=7, max_value=90, value=30)

        all_variables = list(VARIABLE_LABELS.keys())
        selected_variables = st.multiselect(
            "Variables to fetch",
            all_variables,
            default=all_variables,
            format_func=lambda v: VARIABLE_LABELS.get(v, v),
        )

        submitted = st.form_submit_button("Start Pipeline Run", type="primary")
        if submitted:
            if not selected_variables:
                st.error("Select at least one variable.")
            else:
                with st.spinner("Triggering pipeline..."):
                    result = api_post("/trigger-run", {
                        "days_back": days_back,
                        "variables": selected_variables,
                    })
                    if result:
                        st.success(f"Pipeline triggered! Days: {days_back}, Variables: {len(selected_variables)}")
                    else:
                        st.error("Failed to trigger pipeline.")


def main():
    import streamlit as st

    st.set_page_config(
        page_title="Weather Analytics",
        page_icon="",
        layout="wide",
    )

    st.title("Global Weather Analytics")

    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "City Explorer", "Continental Comparison", "Anomaly Monitor", "Pipeline Control"],
    )

    if page == "Overview":
        page_overview()
    elif page == "City Explorer":
        page_city_explorer()
    elif page == "Continental Comparison":
        page_continental_comparison()
    elif page == "Anomaly Monitor":
        page_anomaly_monitor()
    elif page == "Pipeline Control":
        page_pipeline_control()


if __name__ == "__main__":
    main()
