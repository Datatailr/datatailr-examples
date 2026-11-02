import inspect
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from datatailr import Environment, Workflow
from datatailr.scheduler import base
from datatailr.user import User

from merit_desk.common.backtest import run_backtest
from merit_desk.common.curve import build_curve
from merit_desk.common.runs import load_run, new_run, save_run
from merit_desk.common.scenario import PlantOverride, Scenario, ScenarioError, scenario_band
from merit_desk.common.store import MemoryStore, load_forecast, save_forecast
from merit_desk.workflows import desk_runs
from merit_desk.workflows.desk_runs import build_run_workflow, desk_run, execute_run, job_name

TODAY = date(2026, 10, 29)  # the day of the bundled sample forecast
SCENARIO = {"market": "DE", "shift_c": -3.0, "start": "2026-12-01", "end": "2026-12-31",
            "n_paths": 200, "overrides": [{"name": "Peaker", "cost_eur_mwh": 500.0,
                                           "capacity_gw": None}]}


def queued(kind: str, inputs: dict, store: MemoryStore) -> dict:
    record = new_run(kind, "A run", inputs, None)
    save_run(record, store)
    return record


def test_execute_run_writes_a_backtest_result():
    store = MemoryStore()
    r = new_run("backtest", "Backtest DE M2 t1.5", {"market": "DE", "tenor": 2, "threshold": 1.5}, None)
    save_run(r, store)
    execute_run(r["id"], store)
    done = load_run(r["id"], store)
    expected = run_backtest("DE", 2, 1.5)["total_pnl_eur"]
    assert done["status"] == "done" and done["result"]["total_pnl_eur"] == expected


def test_execute_run_writes_the_scenario_band_and_the_unshifted_curve(monkeypatch):
    monkeypatch.setattr(desk_runs, "london_today", lambda: TODAY)
    store = MemoryStore()
    record = queued("scenario", SCENARIO, store)
    execute_run(record["id"], store)
    done = load_run(record["id"], store)
    forecast = load_forecast("sample", store)
    scenario = Scenario("DE", -3.0, date(2026, 12, 1), date(2026, 12, 31),
                        (PlantOverride("Peaker", cost_eur_mwh=500.0),))
    band = scenario_band(scenario, forecast, TODAY, 200)
    assert done["status"] == "done" and done["error"] is None
    assert {key: done["result"][key] for key in band} == band
    assert done["result"]["as_of"] == "2026-10-29"
    assert done["result"]["forecast_run"] == forecast.run_time.isoformat()
    unshifted = build_curve("DE", forecast, TODAY).prices  # no shift and no changed plants
    assert done["result"]["unshifted"] == unshifted
    assert done["result"]["unshifted"] != band["base"]


def test_a_scenario_runs_on_the_newest_saved_forecast(monkeypatch):
    monkeypatch.setattr(desk_runs, "london_today", lambda: TODAY)
    store = MemoryStore()
    sample = load_forecast("sample", store)
    newer = replace(sample, run_time=sample.run_time.replace(day=30), source="newer")
    save_forecast("2026-10-30", newer, store)
    record = queued("scenario", SCENARIO, store)
    execute_run(record["id"], store)
    assert load_run(record["id"], store)["result"]["forecast_run"] == newer.run_time.isoformat()


def test_execute_run_records_a_failure_and_raises(monkeypatch):
    monkeypatch.setattr(desk_runs, "london_today", lambda: TODAY)
    store = MemoryStore()
    backwards = {"market": "DE", "shift_c": -3, "start": "2026-12-31", "end": "2026-12-01",
                 "n_paths": 100, "overrides": []}
    r = new_run("scenario", "x", backwards, None)
    save_run(r, store)
    with pytest.raises(ScenarioError):
        execute_run(r["id"], store)
    failed = load_run(r["id"], store)
    assert failed["status"] == "failed"
    assert failed["error"] == "The shift window must start on or before its end."  # no type name


@pytest.mark.parametrize("inputs, error", [
    ({"market": "DE"}, "KeyError: 'overrides'"),  # an input is missing
    ({**SCENARIO, "market": "XX"}, "UnknownMarketError: Unknown market 'XX'."),
    ({**SCENARIO, "start": "next week"}, "ValueError: Invalid isoformat string: 'next week'"),
    ({**SCENARIO, "overrides": [{"name": "Windmill"}]}, "Unknown plant 'Windmill' in DE."),
])
def test_a_scenario_with_bad_inputs_becomes_a_failed_record(inputs, error):
    store = MemoryStore()
    record = queued("scenario", inputs, store)
    with pytest.raises((KeyError, ValueError)):
        execute_run(record["id"], store)
    failed = load_run(record["id"], store)
    assert failed["status"] == "failed" and failed["result"] is None
    assert failed["error"].startswith(error)  # a ScenarioError has no type name in front


def test_a_result_that_cannot_be_stored_fails_the_record(monkeypatch):
    monkeypatch.setattr(desk_runs, "backtest_detail", lambda *args: {"pnl": object()})
    store = MemoryStore()
    record = queued("backtest", {"market": "DE", "tenor": 2, "threshold": 1.5}, store)
    with pytest.raises(TypeError):
        execute_run(record["id"], store)
    failed = load_run(record["id"], store)
    assert failed["status"] == "failed" and failed["result"] is None
    assert failed["error"].startswith("TypeError: ")


def test_an_sdk_error_in_the_task_shows_without_its_command_line_or_paths(monkeypatch):
    def blob_fails(*args):
        raise RuntimeError("Command failed: dt blob get file:///tmp/tmpab12cd.json --json\n"
                           "Blob merit-desk/dev/forecasts/x.json: access denied")

    monkeypatch.setattr(desk_runs, "backtest_detail", blob_fails)
    store = MemoryStore()
    record = queued("backtest", {"market": "DE", "tenor": 2, "threshold": 1.5}, store)
    with pytest.raises(RuntimeError):
        execute_run(record["id"], store)
    assert load_run(record["id"], store)["error"] == (
        "RuntimeError: Blob merit-desk/dev/forecasts/x.json: access denied")


def test_a_long_task_error_is_cut_to_300_characters(monkeypatch):
    def fails(*args):
        raise ValueError("bad " * 200)

    monkeypatch.setattr(desk_runs, "backtest_detail", fails)
    store = MemoryStore()
    record = queued("backtest", {"market": "DE", "tenor": 2, "threshold": 1.5}, store)
    with pytest.raises(ValueError):
        execute_run(record["id"], store)
    error = load_run(record["id"], store)["error"]
    assert len(error) <= 300 and error.startswith("ValueError: bad") and error.endswith("...")


def test_a_backtest_with_a_missing_input_becomes_a_failed_record():
    store = MemoryStore()
    record = queued("backtest", {"market": "DE", "tenor": 2}, store)
    with pytest.raises(KeyError):
        execute_run(record["id"], store)
    assert load_run(record["id"], store)["status"] == "failed"


def test_execute_run_refuses_a_job_record_and_leaves_it_alone():
    store = MemoryStore()
    record = queued("job", {"name": "Merit Desk Daily Curve"}, store)
    with pytest.raises(ValueError, match="job"):
        execute_run(record["id"], store)
    assert load_run(record["id"], store) == record


def test_execute_run_names_a_record_that_does_not_exist():
    with pytest.raises(LookupError, match="20261030T101500-000000"):
        execute_run("20261030T101500-000000", MemoryStore())


def test_the_desk_run_task_runs_its_record_and_gives_back_the_run_id(monkeypatch):
    store = MemoryStore()
    monkeypatch.setattr(desk_runs, "default_store", lambda: store)
    record = queued("backtest", {"market": "GB", "tenor": 1, "threshold": 1.0}, store)
    assert inspect.unwrap(desk_run)(record["id"]) == record["id"]  # the function under @task
    assert load_run(record["id"], store)["status"] == "done"


def test_job_names_are_ascii_and_unique():
    label = "Scenario DE -3.0C"
    r1, r2 = new_run("scenario", label, {}, None), new_run("scenario", label, {}, None)
    assert job_name(r1) != job_name(r2) and job_name(r1).isascii()
    assert job_name(r1).startswith("Merit Desk Run Scenario DE -3.0C ")


def test_a_job_name_ends_with_the_last_six_characters_of_the_run_id():
    record = new_run("backtest", "Backtest DE M2 t1.5", {}, None)
    assert job_name(record) == f"Merit Desk Run Backtest DE M2 t1.5 {record['id'][-6:]}"


@pytest.fixture
def offline_sdk(monkeypatch):
    """The real Workflow. Only the two questions it asks the platform get local answers."""
    me = User(name="tester", first_name="Test", last_name="User", email="t@example.com", id=1,
              groups=[])
    monkeypatch.setattr(User, "signed_user", staticmethod(lambda: me))  # run_as and the ACL
    monkeypatch.setattr(base, "Budget", lambda name: SimpleNamespace(name=name, id=42))

    def contact_the_platform(*args, **kwargs):
        raise AssertionError("building the workflow must not run or save it")

    for name in ("run", "save", "start", "instant_run"):
        monkeypatch.setattr(Workflow, name, contact_the_platform)


@pytest.mark.parametrize("environment, expected", [
    ("dev", Environment.DEV), ("pre", Environment.PRE), ("prod", Environment.PROD)])
def test_the_run_workflow_has_its_name_environment_budget_version_and_requirements(
        offline_sdk, monkeypatch, tmp_path, environment, expected):
    monkeypatch.chdir(tmp_path)  # not the repository root
    record = new_run("scenario", "Scenario DE -3.0C", SCENARIO, "denis")
    dag = build_run_workflow(record, environment)
    requirements = Path(desk_runs.REQUIREMENTS)
    assert isinstance(dag, Workflow)
    assert dag.name == job_name(record)
    assert dag.environment is expected
    assert dag.budget.name == "Trading desk"
    assert dag.image.python_version == "3.12"
    assert requirements == Path(desk_runs.__file__).with_name("requirements.txt")
    assert dag.image.python_requirements == requirements.read_text() != ""  # the file was found


def test_the_run_workflow_has_one_desk_run_task_with_the_run_id(offline_sdk):
    inputs = {"market": "DE", "tenor": 2, "threshold": 1.5}
    record = new_run("backtest", "Backtest DE M2 t1.5", inputs, None)
    dag = build_run_workflow(record, "dev")
    tasks = json.loads(dag.to_json())["jobs"]
    assert [t["name"] for t in tasks] == ["desk_run"]
    assert tasks[0]["entrypoint"] == "merit_desk.workflows.desk_runs:desk_run"
    assert (tasks[0]["memory"], tasks[0]["num_cpus"]) == ("1g", 1)
    assert dag.get_task_by_name("desk_run").args == {"run_id": record["id"]}  # not the inputs


def test_a_failing_task_call_does_not_leave_autorun_on(offline_sdk, monkeypatch):
    def fail_to_add(self, job):  # the task call has turned autorun on when it gets here
        raise RuntimeError("the task was not added")

    monkeypatch.setattr(Workflow, "add_job", fail_to_add)
    with pytest.raises(RuntimeError, match="not added"):  # not the offline_sdk AssertionError
        build_run_workflow(new_run("job", "x", {}, None), "dev")


def test_the_run_workflow_refuses_an_unknown_environment(offline_sdk):
    with pytest.raises(ValueError, match="staging"):
        build_run_workflow(new_run("job", "x", {}, None), "staging")
