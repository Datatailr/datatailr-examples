"""Deploy Global Weather Analytics components to Datatailr.

Usage:
    # Deploy all components
    python deploy.py

    # Individual components
    python -c "from deploy import deploy_pipeline; deploy_pipeline()"
    python -c "from deploy import deploy_service; deploy_service()"
    python -c "from deploy import deploy_dashboard; deploy_dashboard()"
"""

from datatailr import workflow, App, Service, Resources
from datatailr.logging import CYAN


PIPELINE_REQUIREMENTS = [
    "requests",
]

SERVICE_REQUIREMENTS = [
    "flask",
    "requests",
]

DASHBOARD_REQUIREMENTS = [
    "streamlit",
    "pandas",
    "plotly",
    "requests",
]


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------

def weather_pipeline():
    from weather_analytics.data_pipelines.weather_pipeline import (
        ingest_weather_data,
        clean_and_normalize,
        enrich_and_classify,
        statistical_analysis,
        alerts_and_rankings,
        forecast_summary,
    )

    @workflow(
        name="Global Weather Analytics",
        python_requirements=PIPELINE_REQUIREMENTS,
    )
    def global_weather_analytics_pipeline():
        raw = ingest_weather_data(192).alias("Ingest Weather Data")
        clean = clean_and_normalize(raw).alias("Clean & Normalize")
        enriched = enrich_and_classify(clean).alias("Enrich & Classify")
        stats = statistical_analysis(enriched).alias("Statistical Analysis")
        ranked = alerts_and_rankings(stats).alias("Alerts & Rankings")
        forecast_summary(ranked, enriched).alias("Forecast Summary")

    return global_weather_analytics_pipeline


# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

def weather_service():
    from weather_analytics.services.weather_service import main

    return Service(
        name="Weather Analytics Service",
        entrypoint=main,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=SERVICE_REQUIREMENTS,
    )


# ---------------------------------------------------------------------------
# Dashboard definition
# ---------------------------------------------------------------------------

def weather_dashboard():
    import weather_analytics.dashboards.streamlit.app as entrypoint

    return App(
        name="Weather Analytics Dashboard",
        entrypoint=entrypoint,
        framework="streamlit",
        resources=Resources(memory="2g", cpu=1),
        python_requirements=DASHBOARD_REQUIREMENTS,
    )


# ---------------------------------------------------------------------------
# Deploy helpers
# ---------------------------------------------------------------------------

def deploy_pipeline():
    wf = weather_pipeline()
    print(CYAN("Deploying Global Weather Analytics pipeline..."))
    wf()


def deploy_service():
    service = weather_service()
    print(CYAN("Deploying Weather Analytics service..."))
    service.run()


def deploy_dashboard():
    app = weather_dashboard()
    print(CYAN("Deploying Weather Analytics dashboard..."))
    app.run()


def deploy_all():
    deploy_pipeline()
    deploy_service()
    deploy_dashboard()


if __name__ == "__main__":
    deploy_all()
