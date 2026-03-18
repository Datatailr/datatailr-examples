"""Deploy CSP Price Analytics components to Datatailr.

Usage:
    # Deploy all components
    python -m csp_price_analytics.deploy

    # Individual components
    python -c "from csp_price_analytics.deploy import deploy_market_data; deploy_market_data()"
    python -c "from csp_price_analytics.deploy import deploy_price_engine; deploy_price_engine()"
    python -c "from csp_price_analytics.deploy import deploy_risk_monitor; deploy_risk_monitor()"
    python -c "from csp_price_analytics.deploy import deploy_pipeline; deploy_pipeline()"
    python -c "from csp_price_analytics.deploy import deploy_dashboard; deploy_dashboard()"
"""

from datatailr import workflow, App, Service, Resources
from datatailr.logging import CYAN


SERVICE_REQUIREMENTS = [
    "csp",
    "websockets",
    "flask",
    "flask-sock",
    "numpy",
]

PIPELINE_REQUIREMENTS = [
    "csp",
    "numpy",
]

DASHBOARD_REQUIREMENTS = [
    "streamlit",
    "pandas",
    "plotly",
    "requests",
    "websockets",
]


# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------

def market_data_service():
    from csp_price_analytics.market_data_generator.app import main
    return Service(
        name="Market Data Generator",
        entrypoint=main,
        resources=Resources(memory="1g", cpu=1),
        python_requirements=SERVICE_REQUIREMENTS,
    )


def price_engine_service():
    from csp_price_analytics.price_engine.app import main
    return Service(
        name="Price Engine",
        entrypoint=main,
        resources=Resources(memory="1g", cpu=1),
        python_requirements=SERVICE_REQUIREMENTS,
    )


def risk_monitor_service():
    from csp_price_analytics.risk_monitor.app import main
    return Service(
        name="Risk Monitor",
        entrypoint=main,
        resources=Resources(memory="1g", cpu=1),
        python_requirements=SERVICE_REQUIREMENTS,
    )


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------

def analytics_pipeline():
    from csp_price_analytics.analytics_pipeline.tasks import (
        ingest_tick_data,
        aggregate_ohlcv,
        compute_statistics,
        generate_report,
    )

    @workflow(
        name="CSP Daily Analytics",
        python_requirements=PIPELINE_REQUIREMENTS,
    )
    def csp_daily_analytics():
        raw = ingest_tick_data().alias("Ingest Tick Data")
        aggregated = aggregate_ohlcv(raw).alias("Aggregate OHLCV")
        statistics = compute_statistics(aggregated).alias("Compute Statistics")
        generate_report(aggregated, statistics).alias("Generate Report")

    return csp_daily_analytics


# ---------------------------------------------------------------------------
# Dashboard definition
# ---------------------------------------------------------------------------

def analytics_dashboard():
    import csp_price_analytics.dashboard.app as entrypoint
    return App(
        name="CSP Price Analytics Dashboard",
        entrypoint=entrypoint,
        framework="streamlit",
        resources=Resources(memory="2g", cpu=1),
        python_requirements=DASHBOARD_REQUIREMENTS,
    )


# ---------------------------------------------------------------------------
# Deploy helpers
# ---------------------------------------------------------------------------

def deploy_market_data():
    service = market_data_service()
    print(CYAN("Deploying Market Data Generator service..."))
    service.run()


def deploy_price_engine():
    service = price_engine_service()
    print(CYAN("Deploying Price Engine service..."))
    service.run()


def deploy_risk_monitor():
    service = risk_monitor_service()
    print(CYAN("Deploying Risk Monitor service..."))
    service.run()


def deploy_pipeline():
    wf = analytics_pipeline()
    print(CYAN("Deploying CSP Daily Analytics pipeline..."))
    wf()


def deploy_dashboard():
    app = analytics_dashboard()
    print(CYAN("Deploying CSP Price Analytics Dashboard..."))
    app.run()


def deploy_all():
    deploy_market_data()
    deploy_price_engine()
    deploy_risk_monitor()
    deploy_pipeline()
    deploy_dashboard()


if __name__ == "__main__":
    deploy_all()
