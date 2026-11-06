"""Temperature forecasts: the data class, the ECMWF download and the fallback."""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from merit_desk.common.markets import MARKETS, City, Market

if TYPE_CHECKING:
    import xarray as xr

    from merit_desk.common.store import TextStore

logger = logging.getLogger(__name__)

SOURCE_NAME = "ECMWF IFS HRES open data"
CREDIT = (
    "Temperature forecasts: based on data and products of the European Centre for "
    "Medium-Range Weather Forecasts (ECMWF), licensed under CC BY 4.0 "
    "(https://creativecommons.org/licenses/by/4.0/). Modified: daily mean 2 m temperature "
    "at one grid point per city."
)

# Every 3 hours for 6 days, then every 6 hours up to 10 days: 65 steps, about 21 MB.
STEPS = list(range(0, 145, 3)) + list(range(150, 241, 6))
KELVIN = 273.15
FULL_DAY_HOURS = {0, 6, 12, 18}  # a day needs a step in each quarter to have a fair mean


@dataclass
class Forecast:
    """Daily mean temperatures in degrees C, by market code and then by day."""

    run_time: datetime
    source: str
    temps: dict[str, dict[date, float]]

    def to_json(self) -> str:
        return json.dumps({
            "run_time": self.run_time.isoformat(),
            "source": self.source,
            "temps": {code: {day.isoformat(): t for day, t in days.items()}
                      for code, days in self.temps.items()},
        })

    @classmethod
    def from_json(cls, text: str) -> Forecast:
        raw = json.loads(text)
        return cls(
            run_time=datetime.fromisoformat(raw["run_time"]),
            source=raw["source"],
            temps={code: {date.fromisoformat(day): t for day, t in days.items()}
                   for code, days in raw["temps"].items()},
        )


def _nearest_point(t2m: xr.DataArray, city: City) -> xr.DataArray:
    """The grid point closest to the city. Longitudes wrap, so -0.25 and 359.75 are the same."""
    lat = int(abs(t2m["latitude"] - city.lat).argmin("latitude"))
    lon = int(abs((t2m["longitude"] - city.lon + 180) % 360 - 180).argmin("longitude"))
    return t2m.isel(latitude=lat, longitude=lon)


def extract_daily_means(ds: xr.Dataset, markets: Sequence[Market]) -> dict[str, dict[date, float]]:
    """Daily mean temperature in degrees C for each market, from a 2t dataset in kelvin.

    Each market is the mean of its cities' nearest grid points. Days are UTC days, and a day
    without a step in every quarter (such as the last, lone 00:00 step) is left out.
    """
    t2m = ds["t2m"]
    temps: dict[str, dict[date, float]] = {}
    for market in markets:
        points = [_nearest_point(t2m, city) for city in market.cities]
        celsius = sum(points) / len(points) - KELVIN
        by_time = celsius.swap_dims(step="valid_time").to_pandas()
        temps[market.code] = {
            day: round(float(day_temps.mean()), 2)
            for day, day_temps in by_time.groupby(by_time.index.date)
            if FULL_DAY_HOURS <= set(day_temps.index.hour)
        }
    return temps


def download_forecast(markets: Sequence[Market]) -> Forecast:
    """Download the latest ECMWF run (2 m temperature only) and reduce it to daily means."""
    import requests
    import xarray as xr
    from ecmwf.opendata import Client

    class TimeoutSession(requests.Session):
        """requests never times out by default, and ecmwf-opendata passes timeout=None."""

        def request(self, *args, **kwargs):
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = (10, 30)  # seconds to connect, then between bytes
            return super().request(*args, **kwargs)

    # The default is 500 retries, two minutes apart: too patient for a job with a fallback.
    client = Client(source="ecmwf", maximum_retries=3, retry_after=30)
    client.session = TimeoutSession()
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "ifs_2t.grib2"
        client.retrieve(type="fc", param="2t", step=STEPS, target=str(path))
        with xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""}) as ds:
            run = ds["time"].values.astype("datetime64[s]").item()  # a naive datetime, in UTC
            run_time = run.replace(tzinfo=timezone.utc)
            temps = extract_daily_means(ds, markets)
    return Forecast(run_time=run_time, source=f"{SOURCE_NAME}, run {run_time:%Y-%m-%d %Hz}",
                    temps=temps)


def fetch_and_save(store: TextStore | None = None) -> str:
    """Save the newest ECMWF forecast and return its key, the run date as YYYY-MM-DD.

    This must not fail. If the download does, it logs a warning and returns the key of the newest
    saved forecast, or "sample" for the forecast that ships with the template.
    """
    # Imported here because store.py imports Forecast from this module.
    from merit_desk.common.store import default_store, list_forecast_keys, save_forecast

    store = default_store() if store is None else store
    try:
        forecast = download_forecast(MARKETS)
    except Exception as error:
        saved = list_forecast_keys(store)
        key = saved[-1] if saved else "sample"
        logger.warning("ECMWF download failed (%s: %s). Using the forecast '%s'.",
                       type(error).__name__, error, key)
        return key
    key = forecast.run_time.date().isoformat()
    save_forecast(key, forecast, store)
    return key
