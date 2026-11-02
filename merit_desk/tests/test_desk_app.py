import asyncio
import importlib
import json
import logging
import subprocess
import sys
import threading
import time
import warnings
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import datatailr
import httpx
import pytest
import requests

with warnings.catch_warnings():
    # starlette 1.10 prefers httpx2 and warns about httpx: switch when it drops the httpx fallback
    warnings.filterwarnings("ignore", message=r"Using `httpx` with `starlette\.testclient`")
    from fastapi.testclient import TestClient

from merit_desk.common.curve import build_curve
from merit_desk.common.dates import london_today
from merit_desk.common.runs import finish_run, list_runs, load_run, new_run, save_run
from merit_desk.common.scenario import PlantOverride, Scenario, scenario_curve, stack_view
from merit_desk.common.store import (MemoryStore, newest_forecast, save_backtest_summary,
                                     save_forecast)
from merit_desk.desk_app import app as app_module
from merit_desk.desk_app import client as desk_client
from merit_desk.desk_app import runner as runner_module
from merit_desk.desk_app.app import Broadcaster, Cached, create_app
from merit_desk.desk_app.manager import DESK_JOBS
from merit_desk.desk_app.runner import LocalRunner, RunRefused
from merit_desk.workflows.desk_runs import job_name

ROOT = Path(__file__).parents[2]
FEED = {code: {"price": price, "time": "2026-10-30T10:00:01+00:00"}
        for code, price in {"GB": 95.1, "DE": 101.2, "FR": 88.4}.items()}
QUICK = {"price_interval": 0.02, "poll_interval": 0.02, "heartbeat": 0.2}
BACKTEST = {"market": "DE", "tenor": 2, "threshold": 1.5}
TOO_MANY = "Three runs are in progress. Wait for one to finish."
NO_CAPACITY = "The plants have no capacity left."
CCGT_DEARER = {"name": "CCGT", "cost_eur_mwh": 200}  # every price rises, in any season


def curve_of(code: str) -> dict:
    return {"market": code, "as_of": "2026-10-30", "source": "ECMWF",
            "months": ["2026-11", "2026-12"], "prices": [101.5, 118.2]}


class FakeRunner:
    """A runner whose runs never end. It keeps the calls it gets."""

    def __init__(self, job_states=None):
        self.submitted, self.desk_started, self.stale_calls = [], [], []
        self.shut_down = False
        self.job_states = job_states or {}  # workflow name -> a state dict, or an error to raise

    def submit(self, record):
        self.submitted.append(record["id"])
        return job_name(record)

    def state(self, record):
        return "running"

    def cleanup(self, record):
        pass

    def remove_stale(self, max_age):
        self.stale_calls.append(max_age)
        return 0

    def start_desk_job(self, workflow_name):
        self.desk_started.append(workflow_name)
        return workflow_name

    def desk_job_state(self, workflow_name):
        state = self.job_states.get(workflow_name, {"last_state": None, "last_time": None})
        if isinstance(state, Exception):
            raise state
        return state

    def shutdown(self):
        self.shut_down = True


@pytest.fixture(autouse=True)
def variables(monkeypatch) -> dict:
    """The Datatailr variables (KV) that the app reads. A test never reaches the platform."""
    values = {"sweep_size": "small"}

    class FakeKV:
        def get(self, key):
            return values[key]
    monkeypatch.setattr("datatailr.KV", FakeKV)
    return values


def make_app(store=None, runner=None, prices=lambda: FEED, curves=curve_of, **intervals):
    store = MemoryStore() if store is None else store
    runner = FakeRunner() if runner is None else runner
    return create_app(store, runner, prices, curves, **{**QUICK, **intervals})


def make_client(store=None, runner=None, **kwargs) -> TestClient:
    """Use it as `with make_client() as client`: the app only works while it is started."""
    return TestClient(make_app(store, runner, **kwargs))


def wait_until(check, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("Waited for too long.")


def window(first=10, last=40) -> dict:
    """A shift window that lies inside the curve on any day."""
    today = london_today()
    return {"start": (today + timedelta(first)).isoformat(),
            "end": (today + timedelta(last)).isoformat()}


def scenario_body(**changes) -> dict:
    return {"market": "DE", "shift_c": -3.0, **window(), "overrides": [], **changes}


def scenario_run(**changes) -> dict:
    return {"kind": "scenario", "inputs": {**scenario_body(), "n_paths": 100, **changes}}


def backtest_run(**changes) -> dict:
    return {"kind": "backtest", "inputs": {**BACKTEST, **changes}}


# Health and context


def test_health():
    with make_client() as client:
        assert client.get("/health").json() == {"status": "ok"}


def test_context_gives_the_environment_and_no_user_without_a_header(monkeypatch):
    monkeypatch.setenv("DATATAILR_JOB_ENVIRONMENT", "pre")
    with make_client() as client:
        assert client.get("/api/context").json() == {"user": None, "environment": "pre"}


def test_the_environment_defaults_to_dev(monkeypatch):
    monkeypatch.delenv("DATATAILR_JOB_ENVIRONMENT", raising=False)
    with make_client() as client:
        assert client.get("/api/context").json()["environment"] == "dev"


def test_context_reads_the_user_from_the_header_without_asking_the_platform(monkeypatch):
    def not_called(request):
        raise AssertionError("The page load must not call the platform.")
    monkeypatch.setattr(datatailr.User, "from_request", staticmethod(not_called))
    header = {"x-datatailr-user": json.dumps({"name": "denis"})}
    with make_client() as client:
        assert client.get("/api/context", headers=header).json()["user"] == "denis"


@pytest.mark.parametrize("header", ["not json", "[1]", "null", '{"name": 5}', '{"name": ""}'])
def test_context_has_no_user_for_a_header_it_cannot_use(header):
    with make_client() as client:
        reply = client.get("/api/context", headers={"x-datatailr-user": header})
    assert reply.json()["user"] is None


# The previews


def test_stack_preview_returns_the_stack_view():
    body = {"market": "de", "temp_c": 4.5, "overrides": [{"name": "Peaker", "capacity_gw": 3}]}
    with make_client() as client:
        plain = client.post("/api/stack/preview", json={"market": "DE", "temp_c": 4.5})
        changed = client.post("/api/stack/preview", json=body)
    assert plain.json() == stack_view("DE", 4.5) and plain.json()["price"] == 170.0
    assert changed.json() == stack_view("DE", 4.5, [PlantOverride("Peaker", capacity_gw=3.0)])
    assert changed.json()["plants"] != plain.json()["plants"]


@pytest.mark.parametrize("temp_c", [-40, 45])
def test_stack_preview_accepts_the_temperature_limits(temp_c):
    with make_client() as client:
        response = client.post("/api/stack/preview", json={"market": "GB", "temp_c": temp_c})
    assert response.status_code == 200


@pytest.mark.parametrize("temp_c", [-40.1, 45.1, 1e9])
def test_stack_preview_refuses_a_temperature_outside_minus_40_to_45(temp_c):
    with make_client() as client:
        response = client.post("/api/stack/preview", json={"market": "DE", "temp_c": temp_c})
    assert response.status_code == 422


@pytest.mark.parametrize("content", [b'{"market": "DE", "temp_c": NaN}',
                                     b'{"market": "DE", "temp_c": Infinity}',
                                     b'{"market": "DE", "temp_c": 5, "overrides": '
                                     b'[{"name": "CCGT", "capacity_gw": NaN}]}'])
def test_nan_and_infinity_are_refused(content):
    with make_client() as client:
        response = client.post("/api/stack/preview", content=content,
                               headers={"content-type": "application/json"})
    assert response.status_code == 422


@pytest.mark.parametrize("body, detail", [
    (scenario_run(shift_c=11), "inputs.shift_c: Input should be less than or equal to 10"),
    (scenario_run(shift_c=11, n_paths=50),
     "inputs.shift_c: Input should be less than or equal to 10; "
     "inputs.n_paths: Input should be greater than or equal to 100"),
    (backtest_run(tenor=7), "inputs.tenor: Input should be less than or equal to 6"),
    ({"kind": "scenario"}, "inputs: Field required"),
])
def test_a_validation_error_is_one_text_that_names_the_field_and_the_reason(body, detail):
    with make_client() as client:
        response = client.post("/api/runs", json=body)
    assert (response.status_code, response.json()) == (422, {"detail": detail})


@pytest.mark.parametrize("url", ["/api/runs", "/api/stack/preview", "/api/scenario/preview"])
@pytest.mark.parametrize("content", [b"{", b"not json", b'{"market": "DE",}'])
def test_a_body_that_is_not_json_gets_one_fixed_text(url, content):
    with make_client() as client:
        response = client.post(url, content=content, headers={"content-type": "application/json"})
    assert (response.status_code, response.json()) == (
        422, {"detail": "The request body is not valid JSON."})


TOO_LONG = [
    pytest.param("/api/stack/preview", {"market": "GERMANYXX", "temp_c": 5},
                 "market: String should have at most 8 characters", id="stack market"),
    pytest.param("/api/scenario/preview", {"market": "GERMANYXX"},
                 "market: String should have at most 8 characters", id="scenario market"),
    pytest.param("/api/runs", backtest_run(market="GERMANYXX"),
                 "inputs.market: String should have at most 8 characters", id="run market"),
    pytest.param("/api/stack/preview",
                 {"market": "DE", "temp_c": 5, "overrides": [{"name": "C" * 41}]},
                 "overrides.0.name: String should have at most 40 characters", id="plant name"),
    pytest.param("/api/runs", scenario_run(overrides=[{"name": "C" * 41}]),
                 "inputs.overrides.0.name: String should have at most 40 characters",
                 id="run plant name"),
]


@pytest.mark.parametrize("url, body, detail", TOO_LONG)
def test_a_market_or_a_plant_name_that_is_too_long_is_refused_without_an_echo(url, body, detail):
    with make_client() as client:
        response = client.post(url, json=body)
    assert (response.status_code, response.json()) == (422, {"detail": detail})


def test_a_market_name_too_long_for_the_sweep_is_refused_without_an_echo(memory_store):
    with make_client(memory_store) as client:
        response = client.get("/api/sweep/GERMANYXX")
    assert (response.status_code, response.json()) == (
        422, {"detail": "market: String should have at most 8 characters"})


def test_the_longest_names_are_still_accepted():
    body = {"market": "DE", "temp_c": 5, "overrides": [{"name": "C" * 40}]}
    with make_client() as client:
        response = client.post("/api/stack/preview", json=body)
    assert response.status_code == 422  # the name is not a plant, but its length is fine
    assert response.json()["detail"].startswith(f"Unknown plant '{'C' * 40}' in DE.")


def test_scenario_preview_returns_the_months_the_base_and_the_scenario(memory_store):
    body = scenario_body(market="de", overrides=[CCGT_DEARER])
    with make_client(memory_store) as client:
        response = client.post("/api/scenario/preview", json=body)
    forecast, today = newest_forecast(memory_store), london_today()
    first, last = date.fromisoformat(body["start"]), date.fromisoformat(body["end"])
    scenario = Scenario("DE", -3.0, first, last, (PlantOverride("CCGT", cost_eur_mwh=200.0),))
    base = build_curve("DE", forecast, today)
    assert response.status_code == 200
    assert response.json() == {"months": base.months, "base": base.prices,
                               "scenario": scenario_curve(scenario, forecast, today).prices}
    assert all(s > b for s, b in zip(response.json()["scenario"], response.json()["base"]))


def test_scenario_preview_without_a_shift_or_overrides_is_the_base_curve():
    with make_client() as client:
        body = client.post("/api/scenario/preview",
                           json={"market": "GB", "shift_c": 0, "overrides": []}).json()
    assert body["scenario"] == body["base"] and len(body["months"]) == 12


def test_the_previews_use_the_newest_forecast_in_the_store(memory_store):
    sample, today = newest_forecast(memory_store), london_today()
    freezing = {day: -20.0 for day in (today + timedelta(n) for n in range(400))}  # every day
    save_forecast("2099-01-01", replace(sample, temps={c: freezing for c in sample.temps}),
                  memory_store)
    with make_client(memory_store) as client:
        newest = client.post("/api/scenario/preview", json={"market": "DE"}).json()["base"]
    with make_client(MemoryStore()) as client:  # no saved forecast: the bundled sample
        fallback = client.post("/api/scenario/preview", json={"market": "DE"}).json()["base"]
    assert min(newest) > max(fallback)  # -20 C is scarcity in DE, in any season


def test_the_previews_and_the_run_check_read_the_forecast_once_a_minute(monkeypatch,
                                                                         memory_store):
    reads = []

    def counted(store):
        reads.append(store)
        return newest_forecast(store)
    monkeypatch.setattr(app_module, "newest_forecast", counted)
    with make_client(memory_store) as client:
        for shift in (-3, -2, -1):
            body = scenario_body(shift_c=shift)
            assert client.post("/api/scenario/preview", json=body).status_code == 200
        assert client.post("/api/runs", json=scenario_run()).status_code == 202
    assert reads == [memory_store]


def test_a_cached_value_is_read_again_once_it_is_a_minute_old():
    now, reads = [0.0], []
    cached = Cached(lambda: reads.append(now[0]) or len(reads), 60, clock=lambda: now[0])
    assert [cached(), cached()] == [1, 1]
    now[0] = 59.9
    assert cached() == 1
    now[0] = 60.0
    assert [cached(), cached()] == [2, 2] and reads == [0.0, 60.0]


def test_a_failed_read_is_not_cached():
    answers = iter([ValueError("no store"), "forecast"])

    def read():
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer
    cached = Cached(read, 60)
    with pytest.raises(ValueError):
        cached()
    assert cached() == "forecast"


class ForecastsDown(MemoryStore):
    """A store whose forecasts cannot be read: blob storage is down for them."""

    def list(self, prefix):
        if "/forecasts/" in prefix:
            raise OSError("blob storage is down: /srv/secret")
        return super().list(prefix)


def test_the_previews_use_the_sample_when_the_forecast_cannot_be_read(caplog):
    with caplog.at_level(logging.WARNING, logger="merit_desk.desk_app.app"):
        with make_client(ForecastsDown()) as client:
            down = client.post("/api/scenario/preview", json={"market": "DE"})
            started = client.post("/api/runs", json=scenario_run())
    with make_client(MemoryStore()) as client:  # no saved forecast: the bundled sample
        sample = client.post("/api/scenario/preview", json={"market": "DE"}).json()
    assert down.status_code == 200 and down.json() == sample
    assert started.status_code == 202
    assert "OSError" in caplog.text and "secret" not in caplog.text


SCENARIO_REFUSED = [
    pytest.param({"shift_c": 11}, None, id="shift above 10"),
    pytest.param({"shift_c": -10.5}, None, id="shift below -10"),
    pytest.param(window(40, 10), "must start on or before its end", id="reversed window"),
    pytest.param(window(10, 400), "The shift window must lie between", id="window after the curve"),
    pytest.param(window(-3, 20), "The shift window must lie between", id="window before today"),
    pytest.param({"start": None, "end": None}, "The shift window must lie between",
                 id="a shift with no window"),
    pytest.param({"overrides": [{"name": c, "capacity_gw": 0} for c in
                                ("Renewables", "Lignite", "Coal", "CCGT", "Peaker")]},
                 NO_CAPACITY, id="total capacity of 0"),
    pytest.param({"overrides": [{"name": "Fusion", "cost_eur_mwh": 5}]}, "Unknown plant 'Fusion'",
                 id="unknown plant"),
    pytest.param({"overrides": [{"name": "CCGT", "cost_eur_mwh": 1001}]}, None,
                 id="cost above 1000"),
    pytest.param({"overrides": [{"name": "CCGT", "cost_eur_mwh": -1}]}, None, id="negative cost"),
    pytest.param({"overrides": [{"name": "CCGT", "capacity_gw": 201}]}, None,
                 id="capacity above 200"),
    pytest.param({"overrides": [{"name": "CCGT", "capacity_gw": -1}]}, None,
                 id="negative capacity"),
    pytest.param({"overrides": [{"name": "CCGT"}] * 11}, None, id="eleven overrides"),
    pytest.param({"start": "2026-13-45"}, None, id="a date that does not exist"),
]


@pytest.mark.parametrize("changes, reason", SCENARIO_REFUSED)
def test_scenario_preview_refuses_bad_inputs_with_422(changes, reason):
    with make_client() as client:
        response = client.post("/api/scenario/preview", json=scenario_body(**changes))
    assert response.status_code == 422
    if reason:
        assert reason in response.json()["detail"]


@pytest.mark.parametrize("changes, reason", SCENARIO_REFUSED)
def test_a_run_with_bad_scenario_inputs_is_refused_before_any_job_starts(
        memory_store, changes, reason):
    runner = FakeRunner()
    with make_client(memory_store, runner) as client:
        response = client.post("/api/runs", json=scenario_run(**changes))
    assert response.status_code == 422
    if reason:
        assert reason in response.json()["detail"]
    assert runner.submitted == [] and list_runs(memory_store) == []


def test_a_total_capacity_of_0_is_a_clear_422_and_not_a_division_by_zero():
    overrides = [{"name": c, "capacity_gw": 0} for c in
                 ("Renewables", "Lignite", "Coal", "CCGT", "Peaker")]
    with make_client() as client:
        stack = {"market": "DE", "temp_c": 5, "overrides": overrides}
        for url, body in (("/api/scenario/preview", scenario_body(overrides=overrides)),
                          ("/api/stack/preview", stack)):
            response = client.post(url, json=body)
            assert (response.status_code, response.json()) == (422, {"detail": NO_CAPACITY})


@pytest.mark.parametrize("url, body", [
    ("/api/stack/preview", {"market": "NL", "temp_c": 5}),
    ("/api/scenario/preview", {"market": "NL"}),
    ("/api/runs", {"kind": "backtest", "inputs": {**BACKTEST, "market": "NL"}}),
    ("/api/runs", {"kind": "scenario", "inputs": {"market": "NL"}}),
])
def test_an_unknown_market_is_404_with_the_markets_text(url, body):
    with make_client() as client:
        response = client.post(url, json=body)
    assert (response.status_code, response.json()) == (
        404, {"detail": "Unknown market 'NL'. Use one of: GB, DE, FR."})


# Runs


def test_a_scenario_run_starts_with_202_and_ends_done_with_a_result(memory_store):
    inputs = scenario_body(market="de", overrides=[{"name": "Peaker", "cost_eur_mwh": 300}])
    with make_client(memory_store, LocalRunner(memory_store)) as client:
        response = client.post("/api/runs", json={"kind": "scenario",
                                                  "inputs": {**inputs, "n_paths": 200}})
        record = response.json()
        done = wait_until(lambda: (r := client.get(f"/api/runs/{record['id']}").json())
                          ["status"] == "done" and r)
        assert [r["id"] for r in client.get("/api/runs").json()] == [record["id"]]
    assert response.status_code == 202
    assert record["kind"] == "scenario" and record["status"] in ("running", "done")
    assert record["label"] == "Scenario DE -3.0C 1 plant"
    assert record["inputs"] == {**inputs, "market": "DE", "n_paths": 200, "overrides": [
        {"name": "Peaker", "cost_eur_mwh": 300.0, "capacity_gw": None}]}
    assert done["error"] is None and done["result"]["n_paths"] == 200
    assert len(done["result"]["p50"]) == 12 and len(done["result"]["unshifted"]) == 12


def test_a_backtest_run_ends_done_with_its_daily_result(memory_store):
    with make_client(memory_store, LocalRunner(memory_store)) as client:
        record = client.post("/api/runs", json=backtest_run(market="de")).json()
        done = wait_until(lambda: (r := client.get(f"/api/runs/{record['id']}").json())
                          ["status"] == "done" and r)
    assert record["inputs"] == BACKTEST  # the market is stored as its code
    assert done["result"]["market"] == "DE" and len(done["result"]["dates"]) > 1000


def test_a_run_records_who_started_it(monkeypatch, memory_store):
    seen = []

    def from_request(request):
        seen.append(request.headers["x-datatailr-user"])
        return SimpleNamespace(name="denis")
    monkeypatch.setattr(datatailr.User, "from_request", staticmethod(from_request))
    header = {"x-datatailr-user": json.dumps({"name": "denis"})}
    with make_client(memory_store) as client:
        record = client.post("/api/runs", json=backtest_run(), headers=header).json()
    assert record["started_by"] == "denis" and seen == [header["x-datatailr-user"]]
    assert load_run(record["id"], memory_store)["started_by"] == "denis"


@pytest.mark.parametrize("lookup", [lambda request: None, pytest.param(None, id="raises")])
def test_a_run_without_a_known_user_starts_with_no_name(monkeypatch, memory_store, lookup):
    def broken(request):
        raise RuntimeError("The platform is down.")
    monkeypatch.setattr(datatailr.User, "from_request", staticmethod(lookup or broken))
    with make_client(memory_store) as client:
        response = client.post("/api/runs", json=backtest_run())
    assert response.status_code == 202 and response.json()["started_by"] is None


def test_a_fourth_run_while_three_are_active_gets_429(memory_store):
    runner = FakeRunner()
    with make_client(memory_store, runner) as client:
        started = [client.post("/api/runs", json=backtest_run()) for _ in range(3)]
        refused = client.post("/api/runs", json=backtest_run())
        desk_job = client.post("/api/jobs/daily-curve/start")  # desk jobs do not count
    assert [r.status_code for r in started] == [202, 202, 202]
    assert (refused.status_code, refused.json()) == (429, {"detail": TOO_MANY})
    assert desk_job.status_code == 202
    assert len(runner.submitted) == 3 and len(list_runs(memory_store)) == 4


@pytest.mark.parametrize("changes", [{"tenor": 7}, {"tenor": 0}, {"threshold": 0.2},
                                     {"threshold": 3.1}, {"tenor": 1.5}])
def test_a_backtest_with_bad_inputs_is_refused_with_422(memory_store, changes):
    runner = FakeRunner()
    with make_client(memory_store, runner) as client:
        response = client.post("/api/runs", json=backtest_run(**changes))
    assert response.status_code == 422 and runner.submitted == []


@pytest.mark.parametrize("n_paths", [50, 99, 5001, 1000.5])
def test_a_scenario_run_with_bad_paths_is_refused_with_422(memory_store, n_paths):
    runner = FakeRunner()
    with make_client(memory_store, runner) as client:
        response = client.post("/api/runs", json=scenario_run(n_paths=n_paths))
    assert response.status_code == 422 and runner.submitted == []


@pytest.mark.parametrize("n_paths", [100, 5000])
def test_a_scenario_run_accepts_the_limits_of_the_paths(n_paths):
    with make_client() as client:
        assert client.post("/api/runs", json=scenario_run(n_paths=n_paths)).status_code == 202


@pytest.mark.parametrize("body", [{"kind": "job", "inputs": {"name": "x"}},
                                  {"kind": "scenario"}, {"inputs": BACKTEST}, {}])
def test_a_run_of_another_kind_or_without_inputs_is_refused(body):
    with make_client() as client:
        assert client.post("/api/runs", json=body).status_code == 422


def test_runs_come_newest_first_and_the_limit_is_checked(memory_store):
    ids = []
    for _ in range(3):
        record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, None)
        save_run(record, memory_store)
        ids.append(record["id"])
        time.sleep(0.002)  # the id has microseconds in it
    with make_client(memory_store) as client:
        assert [r["id"] for r in client.get("/api/runs").json()] == ids[::-1]
        assert [r["id"] for r in client.get("/api/runs", params={"limit": 2}).json()] == ids[:0:-1]
        codes = [client.get("/api/runs", params={"limit": n}).status_code for n in (0, 201)]
    assert codes == [422, 422]


def test_one_run_by_id(memory_store):
    record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    save_run(record, memory_store)
    with make_client(memory_store) as client:
        response = client.get(f"/api/runs/{record['id']}")
    assert response.status_code == 200 and response.json() == load_run(record["id"], memory_store)


class SpyStore(MemoryStore):
    def __init__(self):
        super().__init__()
        self.read = []

    def get_text(self, name):
        self.read.append(name)
        return super().get_text(name)


def saved(store, label, **changes):
    """A record that another app or a task wrote: the manager does not track it."""
    record = {**new_run("backtest", label, BACKTEST, None), **changes}
    save_run(record, store)
    time.sleep(0.002)  # the id has microseconds in it
    return record


def test_the_run_list_reads_an_ended_run_once_and_then_keeps_it():
    store, runner = SpyStore(), FakeRunner()
    with make_client(store, runner) as client:
        wait_until(lambda: runner.stale_calls)  # resume has read the store by now
        ended = saved(store, "Backtest DE M2 t1.5", status="done", result={"pnl": 1})
        active = saved(store, "Backtest DE M3 t1.5", status="running")
        lists, reads = [], []
        for change in (None, None, "end", None):
            if change == "end":
                finish_run(active["id"], store, result={"pnl": 2})
            store.read.clear()
            lists.append([(r["id"], r["status"]) for r in client.get("/api/runs").json()])
            reads.append(sorted(store.read))
        newest = client.get("/api/runs", params={"limit": 1}).json()
    key = f"merit-desk/dev/runs/{active['id']}.json"
    both = sorted([key, f"merit-desk/dev/runs/{ended['id']}.json"])
    assert reads == [both, [key], [key], []]  # the ended run is read once, the active one again
    assert lists[0] == [(active["id"], "running"), (ended["id"], "done")]  # newest first
    assert lists[3] == [(active["id"], "done"), (ended["id"], "done")]
    assert [r["id"] for r in newest] == [active["id"]]


def test_a_cached_run_keeps_its_place_and_has_no_result():
    store, runner = SpyStore(), FakeRunner()
    with make_client(store, runner) as client:
        wait_until(lambda: runner.stale_calls)
        old = saved(store, "Backtest DE M2 t1.5", status="failed", error="boom")
        client.get("/api/runs")
        new = saved(store, "Backtest GB M2 t1.5", status="done", result={"pnl": 3})
        listed = client.get("/api/runs").json()
    assert [(r["id"], r["has_result"]) for r in listed] == [(new["id"], True), (old["id"], False)]
    assert all("result" not in r for r in listed) and listed[1]["error"] == "boom"


def test_a_good_run_id_with_no_run_is_404():
    with make_client() as client:
        response = client.get("/api/runs/20261030T101500-0a1b2c")
    assert (response.status_code, response.json()) == (
        404, {"detail": "There is no run '20261030T101500-0a1b2c'."})


@pytest.mark.parametrize("run_id", ["x", "20261030T101500-0A1B2C", "20261030T101500-0a1b2",
                                    "20261030T101500-0a1b2c.json", "٢٠٢٦١٠٣٠T101500-0a1b2c",
                                    "x" * 300])
def test_a_malformed_run_id_is_404_with_one_text_and_never_reaches_the_store(run_id):
    store = SpyStore()
    with make_client(store) as client:
        response = client.get(f"/api/runs/{run_id}")
    assert (response.status_code, response.json()) == (404, {"detail": "No such run."})
    assert store.read == []  # the blob name is built from the id: check it first


def test_a_path_out_of_the_runs_never_reaches_the_store():
    store = SpyStore()
    with make_client(store) as client:
        response = client.get("/api/runs/..%2Fx")  # the router answers: it is not a run path
    assert response.status_code == 404 and store.read == []


# Curves, sweep, jobs


def test_curves_come_from_the_pricing_api_with_one_message_per_failed_market():
    def curves(code):
        if code == "FR":
            raise desk_client.ServiceUnavailable(
                "No curve yet. The daily curve workflow has not run.")
        return curve_of(code)
    with make_client(curves=curves) as client:
        body = client.get("/api/curves").json()
    assert body == {"curves": [curve_of("GB"), curve_of("DE")],
                    "errors": {"FR": "No curve yet. The daily curve workflow has not run."}}


def test_the_pricing_client_is_the_default_for_curves(monkeypatch, memory_store):
    monkeypatch.setattr(desk_client, "get_curve", curve_of)
    with TestClient(create_app(memory_store, FakeRunner(), lambda: FEED, **QUICK)) as client:
        found = client.get("/api/curves").json()["curves"]
    assert found == [curve_of(code) for code in ("GB", "DE", "FR")]


SUMMARY = {"as_of": "2026-10-25", "size": 5, "top": [], "results": [
    {"market": "DE", "tenor": 2, "threshold": 1.0, "total_pnl_eur": 20.5},
    {"market": "GB", "tenor": 3, "threshold": 2.0, "total_pnl_eur": 7.0},
    {"market": "DE", "tenor": 1, "threshold": 1.0, "total_pnl_eur": -50.0},
    {"market": "DE", "tenor": 1, "threshold": 0.5, "total_pnl_eur": 100.0},
    {"market": "FR", "tenor": 1, "threshold": 0.5, "total_pnl_eur": 3.0}]}


def test_the_sweep_is_a_matrix_of_tenors_by_thresholds_with_null_for_a_missing_cell(memory_store):
    save_backtest_summary(SUMMARY, memory_store)
    with make_client(memory_store) as client:
        response = client.get("/api/sweep/de")
    assert response.status_code == 200
    assert response.json() == {"as_of": "2026-10-25", "size": 5, "tenors": [1, 2],
                               "thresholds": [0.5, 1.0], "pnl": [[100.0, -50.0], [None, 20.5]]}


def test_the_sweep_of_a_market_the_summary_does_not_hold_is_empty():
    summary = {**SUMMARY, "results": SUMMARY["results"][:1]}
    store = MemoryStore()
    save_backtest_summary(summary, store)
    with make_client(store) as client:
        body = client.get("/api/sweep/GB").json()
    assert body["tenors"] == [] and body["thresholds"] == [] and body["pnl"] == []


def test_without_a_sweep_the_answer_is_404_with_the_text(memory_store):
    with make_client(memory_store) as client:
        response = client.get("/api/sweep/DE")
    assert (response.status_code, response.json()) == (
        404, {"detail": "No sweep in this environment yet."})


def test_the_sweep_of_an_unknown_market_is_404(memory_store):
    save_backtest_summary(SUMMARY, memory_store)
    with make_client(memory_store) as client:
        response = client.get("/api/sweep/NL")
    assert response.status_code == 404 and "Unknown market 'NL'" in response.json()["detail"]


def test_jobs_list_both_desk_workflows_with_their_last_run():
    runner = FakeRunner({
        DESK_JOBS["daily-curve"]["name"]: {"last_state": "done",
                                           "last_time": "2026-10-30T07:00:12+00:00"},
        DESK_JOBS["strategy-sweep"]["name"]: RuntimeError(
            "Command failed: dt job file:///tmp/secret"),
    })
    with make_client(runner=runner) as client:
        jobs = client.get("/api/jobs").json()
    assert [job["key"] for job in jobs] == ["daily-curve", "strategy-sweep"]
    assert jobs[0] == {"key": "daily-curve", "name": "Merit Desk Daily Curve",
                       "schedule": "Weekdays 07:00 London", "last_state": "done",
                       "last_time": "2026-10-30T07:00:12+00:00"}
    assert jobs[1]["last_state"] is None and jobs[1]["last_time"] is None
    assert jobs[1]["name"] == "Merit Desk Strategy Sweep" and jobs[1]["schedule"]
    assert jobs[1]["error"] and "secret" not in jobs[1]["error"]


def test_the_sweep_card_shows_the_sweep_size_variable(variables):
    variables["sweep_size"] = "full"
    with make_client() as client:
        jobs = {job["key"]: job for job in client.get("/api/jobs").json()}
    assert jobs["strategy-sweep"]["sweep_size"] == "full"
    assert "sweep_size" not in jobs["daily-curve"]


def test_the_sweep_size_is_null_when_it_cannot_be_read(monkeypatch, caplog):
    def broken():
        raise RuntimeError("Command failed: dt kv get file:///tmp/secret")
    monkeypatch.setattr(app_module, "get_sweep_size", broken)
    with caplog.at_level(logging.WARNING), make_client() as client:
        jobs = {job["key"]: job for job in client.get("/api/jobs").json()}
    assert jobs["strategy-sweep"]["sweep_size"] is None
    assert "RuntimeError" in caplog.text and "secret" not in caplog.text


def test_the_local_runner_gives_jobs_with_no_last_run(memory_store):
    with make_client(memory_store, LocalRunner(memory_store)) as client:
        jobs = client.get("/api/jobs").json()
    assert [(j["last_state"], j["last_time"], "error" in j) for j in jobs] == [
        (None, None, False)] * 2


def test_a_desk_job_start_is_202_with_a_job_record(memory_store):
    runner = FakeRunner()
    header = {"x-datatailr-user": json.dumps({"name": "denis"})}
    with make_client(memory_store, runner) as client:
        response = client.post("/api/jobs/strategy-sweep/start", headers=header)
    record = response.json()
    assert response.status_code == 202 and runner.desk_started == ["Merit Desk Strategy Sweep"]
    assert record["kind"] == "job" and record["status"] == "running"
    assert record["job_name"] == "Merit Desk Strategy Sweep"
    assert list_runs(memory_store)[0]["id"] == record["id"]


def test_a_desk_job_start_with_the_local_runner_is_403_with_the_runners_text(memory_store):
    with make_client(memory_store, LocalRunner(memory_store)) as client:
        response = client.post("/api/jobs/daily-curve/start")
    assert (response.status_code, response.json()) == (
        403, {"detail": "Starting desk jobs needs the Datatailr platform."})
    assert [(r["kind"], r["status"]) for r in list_runs(memory_store)] == [("job", "failed")]


def test_a_desk_job_the_platform_refuses_is_403_with_its_message(memory_store):
    class Refusing(FakeRunner):
        def start_desk_job(self, workflow_name):
            raise RunRefused("Permission denied: no right to operate this workflow.")
    with make_client(memory_store, Refusing()) as client:
        response = client.post("/api/jobs/daily-curve/start")
    assert (response.status_code, response.json()) == (
        403, {"detail": "Permission denied: no right to operate this workflow."})


def test_a_refused_desk_job_start_reaches_every_page_as_a_failed_run(memory_store):
    refusal = "Permission denied: no right to operate this workflow."

    class Refusing(FakeRunner):
        def start_desk_job(self, workflow_name):
            raise RunRefused(refusal)
    app = make_app(memory_store, Refusing())

    async def scenario():
        async with app.router.lifespan_context(app):
            streams = [Stream(app), Stream(app)]
            await settle(lambda: app.state.bus.subscribers == 2)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post("/api/jobs/daily-curve/start")
            changes = [await stream.message("run") for stream in streams]
            for stream in streams:
                await stream.close()
            return response, changes
    response, (first, second) = asyncio.run(scenario())
    assert response.status_code == 403 and first == second
    assert (first["kind"], first["status"], first["error"]) == ("job", "failed", refusal)
    stored = {key: value for key, value in list_runs(memory_store)[0].items() if key != "result"}
    assert first == {**stored, "has_result": False}  # the summary of the stored failed record


def test_an_unknown_desk_job_is_404(memory_store):
    runner = FakeRunner()
    with make_client(memory_store, runner) as client:
        response = client.post("/api/jobs/nightly/start")
    assert (response.status_code, response.json()) == (
        404, {"detail": "Unknown desk job 'nightly'. Use one of: daily-curve, strategy-sweep."})
    assert runner.desk_started == [] and list_runs(memory_store) == []


# Startup and shutdown


def test_resume_runs_once_at_startup_and_the_runner_shuts_down_at_the_end():
    runner = FakeRunner()
    with make_client(runner=runner):
        wait_until(lambda: runner.stale_calls)
        time.sleep(0.2)  # ten rounds of polling: resume must not run again
        assert runner.shut_down is False
    assert runner.stale_calls == [timedelta(hours=1)] and runner.shut_down is True


def test_a_click_just_after_startup_counts_the_runs_of_the_last_app():
    class SlowList(MemoryStore):
        def list(self, prefix):
            time.sleep(0.3)  # resume is still reading when the click comes
            return super().list(prefix)

    store = SlowList()
    for _ in range(3):
        save_run(new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, None), store)
    with make_client(store) as client:
        response = client.post("/api/runs", json=backtest_run())
    assert (response.status_code, response.json()) == (429, {"detail": TOO_MANY})


def test_nothing_real_is_built_until_the_app_starts(monkeypatch, memory_store):
    built = []
    monkeypatch.setattr(app_module, "default_store", lambda: built.append("store") or memory_store)
    monkeypatch.setattr(app_module, "real_runner", lambda s: built.append("runner") or FakeRunner())
    app = create_app(prices=lambda: FEED, curves=curve_of, **QUICK)
    assert built == []
    with TestClient(app) as client:
        assert built == ["store", "runner"]
        assert client.get("/health").status_code == 200


def test_the_runner_is_local_when_the_environment_says_so(monkeypatch, memory_store):
    monkeypatch.setenv("MERIT_DESK_RUNNER", "local")
    runner = app_module.real_runner(memory_store)
    try:
        assert isinstance(runner, LocalRunner)
    finally:
        runner.shutdown()


@pytest.mark.parametrize("value, environment", [(None, "dev"), ("platform", "pre"),
                                                ("LOCAL", "prod"), ("", "dev")])
def test_any_other_value_selects_the_platform_runner(monkeypatch, memory_store, value, environment):
    created = []
    monkeypatch.setattr(runner_module, "PlatformRunner",
                        lambda env: created.append(env) or SimpleNamespace(kind="platform"))
    monkeypatch.setenv("DATATAILR_JOB_ENVIRONMENT", environment)
    if value is None:
        monkeypatch.delenv("MERIT_DESK_RUNNER", raising=False)
    else:
        monkeypatch.setenv("MERIT_DESK_RUNNER", value)
    assert app_module.real_runner(memory_store).kind == "platform"
    assert created == [environment]


def test_importing_the_app_loads_no_sdk_module_and_opens_no_connection():
    code = (
        "import socket, sys\n"
        "def refuse(*args, **kwargs):\n"
        "    raise AssertionError('The import used the network.')\n"
        "for name in ('connect', 'connect_ex'):\n"
        "    setattr(socket.socket, name, refuse)\n"
        "socket.create_connection = socket.getaddrinfo = refuse\n"
        "import merit_desk.desk_app.app as app_module\n"
        "print(sorted(m for m in sys.modules if m.split('.')[0] == 'datatailr'))\n"
        "print(app_module.app.title)\n")
    done = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert done.stdout.splitlines() == ["[]", "Merit Desk"]


# The event stream


def test_the_broadcaster_gives_every_subscriber_every_message():
    async def scenario():
        bus = Broadcaster()
        first, second = bus.subscribe(), bus.subscribe()
        bus.publish("a")
        bus.publish("b")
        return [[q.get_nowait(), q.get_nowait()] for q in (first, second)], bus.subscribers
    assert asyncio.run(scenario()) == ([["a", "b"], ["a", "b"]], 2)


def test_a_slow_subscriber_loses_its_oldest_messages_and_blocks_nobody():
    async def scenario():
        bus = Broadcaster(size=2)
        slow, quick = bus.subscribe(), bus.subscribe()
        got = []
        for message in ("a", "b", "c", "d"):
            bus.publish(message)
            got.append(quick.get_nowait())
        return got, [slow.get_nowait(), slow.get_nowait()], slow.empty()
    assert asyncio.run(scenario()) == (["a", "b", "c", "d"], ["c", "d"], True)


def test_an_unsubscribed_queue_gets_nothing_more():
    async def scenario():
        bus = Broadcaster()
        queue = bus.subscribe()
        bus.unsubscribe(queue)
        bus.unsubscribe(queue)  # a second call is harmless
        bus.publish("a")
        return queue.empty(), bus.subscribers
    assert asyncio.run(scenario()) == (True, 0)


class Stream:
    """One GET /api/events made through the ASGI interface, as a server makes it."""

    def __init__(self, app):
        self._frames, self._disconnect = asyncio.Queue(), asyncio.Event()
        self.start = None  # the status and headers of the response
        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
                 "scheme": "http", "path": "/api/events", "raw_path": b"/api/events",
                 "root_path": "", "query_string": b"", "headers": [], "client": ("test", 1),
                 "server": ("test", 80)}
        self._task = asyncio.create_task(app(scope, self._receive, self._send))

    async def _receive(self):
        await self._disconnect.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message):
        if message["type"] == "http.response.start":
            self.start = message
        elif message["type"] == "http.response.body" and message.get("body"):
            await self._frames.put(message["body"].decode())

    async def message(self, name):
        """The data of the next message with this event name (a comment is 'keepalive').

        It gives up after 5 s, however many other messages come, so a test never hangs."""
        async def next_one():
            while True:
                frame = await self._frames.get()
                if frame.startswith(": "):
                    if name == "keepalive":
                        return frame.strip()
                    continue
                fields = dict(line.split(": ", 1) for line in frame.strip().split("\n"))
                if fields["event"] == name:
                    return json.loads(fields["data"])
        return await asyncio.wait_for(next_one(), 5)

    def is_open(self):
        return not self._task.done()

    async def close(self):
        self._disconnect.set()
        await asyncio.wait_for(self._task, 5)


async def settle(check, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        assert asyncio.get_running_loop().time() < deadline, "Waited for too long."
        await asyncio.sleep(0.01)


def test_two_pages_both_get_the_same_run_change(memory_store):
    record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    record["status"] = "running"
    save_run(record, memory_store)  # a run of an app that stopped: the manager takes it up
    runner = FakeRunner()
    app = make_app(memory_store, runner)

    async def scenario():
        async with app.router.lifespan_context(app):
            streams = [Stream(app), Stream(app)]
            for stream in streams:
                await stream.message("prices")  # now it is subscribed
            await settle(lambda: runner.stale_calls)  # resume has listed the runs by now
            finish_run(record["id"], memory_store, result={"total_pnl_eur": 12.5})
            changes = [await stream.message("run") for stream in streams]
            for stream in streams:
                await stream.close()
            return changes, app.state.bus.subscribers
    (first, second), subscribers = asyncio.run(scenario())
    assert first == second
    assert first["id"] == record["id"] and first["status"] == "done"
    assert "result" not in first and first["has_result"] is True
    assert subscribers == 0  # a closed page leaves no queue behind


def test_the_list_and_the_events_leave_out_the_result_and_one_run_has_it(memory_store):
    record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    record["status"] = "running"
    save_run(record, memory_store)
    waiting = new_run("backtest", "Backtest GB M2 t1.5", BACKTEST, None)  # no result yet
    save_run(waiting, memory_store)
    runner = FakeRunner()
    app = make_app(memory_store, runner)
    result = {"total_pnl_eur": 12.5, "dates": ["2021-01-01"] * 1000}

    async def scenario():
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            await stream.message("prices")  # now it is subscribed
            await settle(lambda: runner.stale_calls)  # resume has listed the runs by now
            finish_run(record["id"], memory_store, result=result)
            event = await stream.message("run")
            await stream.close()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                listed = (await client.get("/api/runs")).json()
                whole = (await client.get(f"/api/runs/{record['id']}")).json()
            return event, listed, whole
    event, listed, whole = asyncio.run(scenario())
    assert "result" not in event and event["has_result"] is True and event["status"] == "done"
    by_id = {r["id"]: r for r in listed}
    assert "result" not in by_id[record["id"]] and by_id[record["id"]]["has_result"] is True
    assert "result" not in by_id[waiting["id"]] and by_id[waiting["id"]]["has_result"] is False
    assert whole["result"] == result  # only the run by id carries it


def test_a_run_start_reaches_every_page_at_once(memory_store):
    app = make_app(memory_store)

    async def scenario():
        async with app.router.lifespan_context(app):
            streams = [Stream(app), Stream(app)]
            await settle(lambda: app.state.bus.subscribers == 2)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                started = (await client.post("/api/runs", json=backtest_run())).json()
            changes = [await stream.message("run") for stream in streams]
            for stream in streams:
                await stream.close()
            return started, changes
    started, (first, second) = asyncio.run(scenario())
    without_result = {key: value for key, value in started.items() if key != "result"}
    assert first == second == {**without_result, "has_result": False}
    assert started["status"] == "running"


def test_a_slow_run_start_does_not_block_the_event_loop(memory_store):
    class SlowRunner(FakeRunner):
        """It starts only after the event loop has done other work: a blocked loop fails it."""

        def __init__(self):
            super().__init__()
            self.entered, self.loop_works = threading.Event(), threading.Event()

        def submit(self, record):
            self.entered.set()
            assert self.loop_works.wait(2), "The event loop was blocked."
            return super().submit(record)
    runner = SlowRunner()
    app = make_app(memory_store, runner)

    async def scenario():
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                posting = asyncio.create_task(client.post("/api/runs", json=backtest_run()))
                await settle(runner.entered.is_set)  # the start is inside submit
                await stream.message("prices")  # and the loop still serves the stream
                runner.loop_works.set()
                response = await posting
            await stream.close()
            return response
    assert asyncio.run(scenario()).status_code == 202


def test_prices_arrive_with_the_data_of_the_market_feed_client(monkeypatch, memory_store):
    monkeypatch.setattr(desk_client, "get_latest_prices", lambda: FEED)
    app = create_app(memory_store, FakeRunner(), curves=curve_of, **QUICK)

    async def scenario():
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            data = await stream.message("prices")
            await stream.close()
            return data, stream.start
    data, start = asyncio.run(scenario())
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    assert data == FEED and start["status"] == 200
    assert headers["content-type"].startswith("text/event-stream")
    assert headers["cache-control"] == "no-cache"


def test_the_stream_stays_open_and_carries_the_error_when_the_feed_is_down():
    calls = []

    def feed_down():
        calls.append(1)
        raise desk_client.ServiceUnavailable("The feed has no prices yet.")

    async def scenario():
        app = make_app(prices=feed_down)
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            errors = [await stream.message("prices") for _ in range(3)]
            still_open = stream.is_open()
            await stream.close()
            return errors, still_open
    errors, still_open = asyncio.run(scenario())
    assert errors == [{"error": "The feed has no prices yet."}] * 3 and still_open
    assert len(calls) >= 3  # one reader for all pages, every interval


def test_the_market_feed_is_not_read_while_no_page_is_open():
    calls = []

    def feed():
        calls.append(1)
        return FEED

    async def scenario():
        app = make_app(prices=feed)
        async with app.router.lifespan_context(app):
            await asyncio.sleep(0.2)  # ten price intervals, and no page
            unread = len(calls)
            stream = Stream(app)
            data = await stream.message("prices")
            await stream.close()
            return unread, data
    assert asyncio.run(scenario()) == (0, FEED)


def test_a_feed_that_breaks_in_another_way_gives_a_fixed_text_and_logs_the_error(caplog):
    def broken():
        raise KeyError("/srv/secret/path")

    async def scenario():
        app = make_app(prices=broken)
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            data = await stream.message("prices")
            await stream.close()
            return data
    with caplog.at_level(logging.ERROR, logger="merit_desk.desk_app.app"):
        data = asyncio.run(scenario())
    assert data == {"error": "The Market Feed is not reachable."}
    logged = [r for r in caplog.records if r.exc_info and r.exc_info[0] is KeyError]
    assert logged and logged[0].levelno == logging.ERROR


def test_a_comment_keeps_a_quiet_stream_alive():
    async def scenario():
        app = make_app(price_interval=100, heartbeat=0.05)
        async with app.router.lifespan_context(app):
            stream = Stream(app)
            comments = [await stream.message("keepalive") for _ in range(2)]
            await stream.close()
            return comments
    assert asyncio.run(scenario()) == [": keepalive", ": keepalive"]


# The client for the two services

CURVE = {"market": "DE", "as_of": "2026-10-29", "source": "ECMWF IFS HRES open data",
         "months": ["2026-10", "2026-11"], "prices": [101.5, 118.2]}


class FakeResponse:
    def __init__(self, status_code: int, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("No JSON in the body.")
        return self._body


def get_answers(monkeypatch, response: FakeResponse) -> list[dict]:
    """Make requests.get answer with this response; returns the calls it receives."""
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return response
    monkeypatch.setattr(requests, "get", fake_get)
    return calls


def get_fails(monkeypatch, error: Exception) -> None:
    def fake_get(url, **kwargs):
        raise error
    monkeypatch.setattr(requests, "get", fake_get)


def test_get_curve_returns_json(monkeypatch):
    calls = get_answers(monkeypatch, FakeResponse(200, CURVE))
    assert desk_client.get_curve("DE") == CURVE
    assert calls == [{"url": f"{desk_client.PRICING_API_URL}/curve/DE", "timeout": 5}]


def test_get_latest_prices_returns_json(monkeypatch):
    calls = get_answers(monkeypatch, FakeResponse(200, FEED))
    assert desk_client.get_latest_prices() == FEED
    assert calls == [{"url": f"{desk_client.MARKET_FEED_URL}/latest", "timeout": 5}]


@pytest.fixture
def without_service_urls(monkeypatch):
    """The client as it loads when PRICING_API_URL and MARKET_FEED_URL are not exported."""
    monkeypatch.delenv("PRICING_API_URL", raising=False)
    monkeypatch.delenv("MARKET_FEED_URL", raising=False)
    importlib.reload(desk_client)
    yield
    monkeypatch.undo()
    importlib.reload(desk_client)  # the next test sees the client as the environment makes it


def test_urls_default_to_the_service_names_on_the_platform(without_service_urls):
    assert desk_client.PRICING_API_URL == "http://pricing-api"
    assert desk_client.MARKET_FEED_URL == "http://market-feed"


def test_client_raises_service_unavailable(monkeypatch):
    detail = "No curve yet. The daily curve workflow has not run."
    get_answers(monkeypatch, FakeResponse(503, {"detail": detail}))
    with pytest.raises(desk_client.ServiceUnavailable, match="No curve yet"):
        desk_client.get_curve("DE")


def test_unknown_market_text_comes_through(monkeypatch):
    detail = "Unknown market 'NL'. Use one of: GB, DE, FR."
    get_answers(monkeypatch, FakeResponse(404, {"detail": detail}))
    with pytest.raises(desk_client.ServiceUnavailable) as raised:
        desk_client.get_curve("NL")
    assert str(raised.value) == detail


@pytest.mark.parametrize("body", [None, {"error": "bad gateway"}, {"detail": ""}])
def test_default_text_when_the_reply_has_no_detail(monkeypatch, body):
    get_answers(monkeypatch, FakeResponse(502, body))
    with pytest.raises(desk_client.ServiceUnavailable, match="The Market Feed is not reachable."):
        desk_client.get_latest_prices()


@pytest.mark.parametrize("error", [requests.ConnectionError("refused"), requests.Timeout("slow")])
def test_connection_error_becomes_service_unavailable(monkeypatch, error):
    get_fails(monkeypatch, error)
    with pytest.raises(desk_client.ServiceUnavailable, match="The Pricing API is not reachable."):
        desk_client.get_curve("DE")
    with pytest.raises(desk_client.ServiceUnavailable, match="The Market Feed is not reachable."):
        desk_client.get_latest_prices()


def test_a_reply_that_is_not_json_becomes_service_unavailable(monkeypatch):
    get_answers(monkeypatch, FakeResponse(200))
    with pytest.raises(desk_client.ServiceUnavailable, match="The Pricing API is not reachable."):
        desk_client.get_curve("DE")
