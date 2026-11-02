"""The daily curve: fetch the weather forecast, price each market, publish, tell the desk."""

from pathlib import Path

from datatailr import Schedule, task, workflow

from merit_desk.common import curve
from merit_desk.common.dates import london_today
from merit_desk.common.notify import post_summary
from merit_desk.common.settings import get_markets
from merit_desk.common.store import default_store, load_forecast, save_curves
from merit_desk.common.weather import fetch_and_save

REQUIREMENTS = str(Path(__file__).with_name("requirements.txt"))  # works from any folder


@task(memory="2g")  # reading the ECMWF file takes about 1 GB
def fetch_forecast() -> str:
    return fetch_and_save()


@task()
def build_curve(market: str, forecast_key: str) -> dict:
    forecast = load_forecast(forecast_key, default_store())
    return curve.build_curve(market, forecast, london_today()).to_dict()


@task()
def publish_curves(curves: list) -> str:
    save_curves(curves, default_store())
    return curve.summary(curves)


@task()
def notify_desk(summary: str) -> None:
    post_summary(summary)


@workflow(
    name="Merit Desk Daily Curve",
    schedule=Schedule(
        at_hours=[7],
        at_minutes=[0],
        weekdays=["Mon", "Tue", "Wed", "Thu", "Fri"],
        timezone="Europe/London",
    ),
    budget="Trading",
    python_version="3.12",
    python_requirements=REQUIREMENTS,
)
def daily_curve():
    key = fetch_forecast()
    curves = [build_curve(m.code, key).alias(f"curve_{m.code}") for m in get_markets()]
    notify_desk(publish_curves(curves))
