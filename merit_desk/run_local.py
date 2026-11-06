"""Run the daily curve on this machine: no Datatailr login, files in .merit_desk_data/."""

import os
import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))  # so "python merit_desk/run_local.py" works

from merit_desk.common import curve
from merit_desk.common.dates import london_today
from merit_desk.common.settings import get_markets
from merit_desk.common.store import TextStore, default_store, load_forecast, save_curves
from merit_desk.common.weather import fetch_and_save


def run_daily_curve_locally(store: TextStore, today: date | None = None) -> str:
    """The steps of the daily curve workflow, in order. It does not post to the desk chat."""
    today = today or london_today()
    forecast = load_forecast(fetch_and_save(store), store)
    curves = [curve.build_curve(m, forecast, today).to_dict() for m in get_markets()]
    save_curves(curves, store)
    return curve.summary(curves)


if __name__ == "__main__":
    os.environ["MERIT_DESK_STORE"] = "local"
    print(run_daily_curve_locally(default_store()))
