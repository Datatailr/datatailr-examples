"""One click, one workflow: a single task computes a scenario or a backtest into its run record."""

from datetime import date
from pathlib import Path

from datatailr import Environment, Workflow, task

from merit_desk.common.backtest import backtest_detail
from merit_desk.common.curve import build_curve
from merit_desk.common.dates import london_today
from merit_desk.common.messages import MAX_TEXT, clean_platform_text
from merit_desk.common.runs import finish_run, load_run
from merit_desk.common.scenario import PlantOverride, Scenario, ScenarioError, scenario_band
from merit_desk.common.store import TextStore, default_store, newest_forecast

REQUIREMENTS = str(Path(__file__).with_name("requirements.txt"))  # works from any folder
RUN_PREFIX = "Merit Desk Run "  # every per-click job starts like this, and no desk workflow does


def _day(text: str | None) -> date | None:
    return date.fromisoformat(text) if text else None


def scenario_result(inputs: dict, store: TextStore) -> dict:
    """The weather-path band, and today's curve for the page to draw as the dashed base."""
    forecast, today = newest_forecast(store), london_today()
    overrides = tuple(PlantOverride(o["name"], o.get("cost_eur_mwh"), o.get("capacity_gw"))
                      for o in inputs["overrides"])
    scenario = Scenario(inputs["market"], inputs["shift_c"], _day(inputs.get("start")),
                        _day(inputs.get("end")), overrides)
    return {
        **scenario_band(scenario, forecast, today, inputs["n_paths"]),
        "as_of": today.isoformat(),
        "forecast_run": forecast.run_time.isoformat(),
        "unshifted": build_curve(inputs["market"], forecast, today).prices,
    }


def _error_text(error: Exception) -> str:
    """A ScenarioError already says what is wrong. Any other error also gets its type, and the
    text of an SDK error loses its command line and paths, as a refused run's text does."""
    if isinstance(error, ScenarioError):
        return str(error)
    name = type(error).__name__
    text = clean_platform_text(str(error), "", limit=MAX_TEXT - len(name) - 2)
    return f"{name}: {text}" if text else name


def execute_run(run_id: str, store: TextStore) -> None:
    """Compute one run into its record. A failure is written to the record, then raised."""
    record = load_run(run_id, store)
    if record is None:
        raise LookupError(f"There is no run record {run_id}.")
    kind, inputs = record["kind"], record["inputs"]
    if kind not in ("scenario", "backtest"):  # the app starts the desk jobs itself
        raise ValueError(f"The desk does not run '{kind}' records.")
    try:  # the app checked the inputs, but a bad one must fail the record, not hang it
        if kind == "scenario":
            result = scenario_result(inputs, store)
        else:
            result = backtest_detail(inputs["market"], inputs["tenor"], inputs["threshold"])
        finish_run(run_id, store, result=result)  # a result that cannot be stored fails it too
    except Exception as error:
        finish_run(run_id, store, error=_error_text(error))
        raise


@task(memory="1g", cpu=1)  # 5,000 weather paths peak near 180 MB, more than the 100 MB default
def desk_run(run_id: str) -> str:
    execute_run(run_id, default_store())
    return run_id


def job_name(record: dict) -> str:
    """ASCII, and the end of the id keeps two identical clicks apart."""
    return f"{RUN_PREFIX}{record['label']} {record['id'][-6:]}"


def build_run_workflow(record: dict, environment: str) -> Workflow:
    """The workflow of one click, built like @workflow does. It saves and runs nothing."""
    dag = Workflow(
        name=job_name(record),
        environment=Environment(environment.lower()),
        python_version="3.12",
        python_requirements=REQUIREMENTS,
        budget="Trading",
    )
    dag.set_autorun(False)
    with dag:
        try:
            desk_run(record["id"])  # the task gets the id only. The inputs stay in the record
        finally:  # the task call turns autorun on, and leaving the block would run the workflow
            dag.set_autorun(False)
            dag.set_instant_run(False)
    return dag
