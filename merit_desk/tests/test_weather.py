from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pytest
import requests
import xarray as xr

import merit_desk.common.store as store_module
import merit_desk.common.weather as weather
from merit_desk.common.markets import GB, MARKETS
from merit_desk.common.store import load_forecast, save_forecast
from merit_desk.common.weather import (
    CREDIT, SOURCE_NAME, Forecast, download_forecast, extract_daily_means, fetch_and_save,
)

RUN_TIME = datetime(2026, 10, 29, tzinfo=timezone.utc)


def tiny_dataset(longitudes: list[float], hours=range(0, 48, 6)) -> xr.Dataset:
    """Two grid points, shaped like the cfgrib output for 2t.

    The point near Manchester is 290 K and the point near London is 280 K, plus 1 K for each
    step. The other two points are 0 K, so picking one of them would show.
    """
    run = np.datetime64("2026-10-29T00:00", "ns")
    step = np.array(list(hours), dtype="timedelta64[h]").astype("timedelta64[ns]")
    kelvin = np.zeros((len(step), 2, 2)) + np.arange(len(step))[:, None, None]
    kelvin[:, 0, 0] += 290  # Manchester
    kelvin[:, 1, 1] += 280  # London
    return xr.Dataset(
        {"t2m": (("step", "latitude", "longitude"), kelvin)},
        coords={"step": step, "latitude": [53.5, 51.5], "longitude": longitudes,
                "time": run, "valid_time": ("step", run + step)},
    )


# The ECMWF file runs from -180 to 180 degrees east; other GRIB files run from 0 to 360.
SIGNED = [-2.25, -0.25]
UNSIGNED = [357.75, 359.75]


@pytest.mark.parametrize("longitudes", [SIGNED, UNSIGNED], ids=["-180..180", "0..360"])
def test_extract_daily_means_averages_cities_and_converts_kelvin(longitudes):
    temps = extract_daily_means(tiny_dataset(longitudes), [GB])
    # Both days: the mean of 285 K plus the mean step offset (1.5 K, then 5.5 K), in degrees C.
    assert temps == {"GB": {date(2026, 10, 29): pytest.approx(13.35, abs=0.01),
                            date(2026, 10, 30): pytest.approx(17.35, abs=0.01)}}


def test_extract_daily_means_skips_a_day_without_all_six_hour_slots():
    temps = extract_daily_means(tiny_dataset(SIGNED, hours=range(0, 54, 6)), [GB])
    assert list(temps["GB"]) == [date(2026, 10, 29), date(2026, 10, 30)]  # not the lone 00:00


@pytest.fixture
def fake_ecmwf(monkeypatch):
    """Replace the ECMWF client and the GRIB reader. The fixture is the list of clients made."""
    clients = []

    class FakeClient:
        def __init__(self, **options):
            self.options = options
            self.requests = []
            clients.append(self)

        def retrieve(self, **request):
            Path(request["target"]).write_bytes(b"GRIB")
            self.requests.append(request)

    def fake_open_dataset(path, **options):
        assert Path(path).read_bytes() == b"GRIB"
        return tiny_dataset(SIGNED)

    monkeypatch.setattr("ecmwf.opendata.Client", FakeClient)
    monkeypatch.setattr(xr, "open_dataset", fake_open_dataset)
    return clients


def test_download_forecast_names_the_run_and_removes_the_file(fake_ecmwf):
    forecast = download_forecast([GB])

    request = fake_ecmwf[0].requests[0]
    assert forecast.run_time == RUN_TIME
    assert forecast.source == f"{SOURCE_NAME}, run 2026-10-29 00z"
    assert forecast.temps["GB"][date(2026, 10, 29)] == pytest.approx(13.35, abs=0.01)
    assert request["param"] == "2t" and request["step"][-1] == 240
    assert not Path(request["target"]).exists()


def test_download_forecast_does_not_wait_forever_on_a_stalled_connection(fake_ecmwf, monkeypatch):
    download_forecast([GB])
    client = fake_ecmwf[0]

    assert client.options["maximum_retries"] <= 3  # the library default is 500
    sent = []  # what reaches requests, without any network
    monkeypatch.setattr(requests.Session, "request",
                        lambda self, *args, **kwargs: sent.append(kwargs))
    client.session.get("https://example.invalid/index", timeout=None)  # how ecmwf-opendata calls
    client.session.get("https://example.invalid/index", timeout=5)
    assert sent[0]["timeout"] == (10, 30)
    assert sent[1]["timeout"] == 5


def make_forecast(run_time: datetime = RUN_TIME) -> Forecast:
    return Forecast(run_time=run_time, source=SOURCE_NAME,
                    temps={"DE": {run_time.date(): 7.5}})


def stub_download(monkeypatch, result):
    """Make download_forecast return the forecast, or raise if the result is an exception."""
    def download(markets):
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(weather, "download_forecast", download)


def test_fetch_and_save_saves_the_download(memory_store, monkeypatch):
    fresh = make_forecast()
    stub_download(monkeypatch, fresh)
    assert fetch_and_save(memory_store) == "2026-10-29"
    assert load_forecast("2026-10-29", memory_store) == fresh


def test_fetch_and_save_asks_for_every_market(memory_store, monkeypatch):
    asked = []

    def download(markets):
        asked.append(list(markets))
        return make_forecast()

    monkeypatch.setattr(weather, "download_forecast", download)
    fetch_and_save(memory_store)
    assert asked == [list(MARKETS)]


def test_fetch_and_save_falls_back_to_newest_saved_day(memory_store, monkeypatch, caplog):
    for day in (27, 28):
        run_time = datetime(2026, 10, day, tzinfo=timezone.utc)
        save_forecast(f"2026-10-{day}", make_forecast(run_time), memory_store)
    stub_download(monkeypatch, OSError("no network"))
    assert fetch_and_save(memory_store) == "2026-10-28"
    assert "no network" in caplog.text


def test_fetch_and_save_falls_back_to_sample(memory_store, monkeypatch):
    stub_download(monkeypatch, OSError("no network"))
    assert fetch_and_save(memory_store) == "sample"
    assert load_forecast("sample", memory_store).temps["DE"]


def test_fetch_and_save_uses_the_default_store(memory_store, monkeypatch):
    monkeypatch.setattr(store_module, "default_store", lambda: memory_store)
    stub_download(monkeypatch, make_forecast())
    assert fetch_and_save() == "2026-10-29"
    assert load_forecast("2026-10-29", memory_store) is not None


def test_sample_file_covers_all_markets(memory_store):
    sample = load_forecast("sample", memory_store)  # nothing stored: reads the bundled file
    assert set(sample.temps) == {market.code for market in MARKETS}
    assert all(len(days) >= 9 for days in sample.temps.values())
    assert sample.run_time.tzinfo is not None and SOURCE_NAME in sample.source


def test_credit_follows_the_ecmwf_terms():
    assert CREDIT == (
        "Temperature forecasts: based on data and products of the European Centre for "
        "Medium-Range Weather Forecasts (ECMWF), licensed under CC BY 4.0 "
        "(https://creativecommons.org/licenses/by/4.0/). Modified: daily mean 2 m temperature "
        "at one grid point per city."
    )
