"""What each job gives to Datatailr, checked with recorders: nothing is deployed."""

import importlib.util
import sys
from pathlib import Path

import datatailr
import pytest
from datatailr import Resources

import merit_desk.deploy as d
from merit_desk.admin import setup_demo
from merit_desk.common.runs import new_run
from merit_desk.workflows.desk_runs import build_run_workflow

WORKFLOWS = Path(__file__).parents[1] / "workflows"


@pytest.fixture
def deployed(monkeypatch, tmp_path) -> list[dict]:
    """The jobs that deploy.py runs. The current folder is not the repository root."""
    monkeypatch.chdir(tmp_path)
    jobs = []

    class Recorder:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self):
            jobs.append(self.kwargs)

    for name in ("Service", "App", "ExcelAddin"):
        monkeypatch.setattr(d, name, Recorder)
    return jobs


@pytest.mark.parametrize("deploy, names", [
    (d.deploy_services, ["Merit Desk Pricing API", "Merit Desk Market Feed"]),
    (d.deploy_app, ["Merit Desk"]),
    (d.deploy_excel, ["Merit Desk Tools"]),
])
def test_each_job_has_its_name_budget_section_version_and_requirements(deployed, deploy, names):
    deploy()
    assert [job["name"] for job in deployed] == names
    for job in deployed:
        assert job["budget"] == "Trading"
        assert job["app_section"] == "Trading"
        assert job["python_version"] == "3.12"
        assert Path(job["python_requirements"]).is_file()  # a relative path fails from here


def test_the_app_is_a_fastapi_app_served_from_the_desk_app_module(deployed):
    d.deploy_app()
    (app,) = deployed
    assert app["framework"] == "fastapi"
    assert app["entrypoint"].__name__ == "merit_desk.desk_app.app"  # the module, not a function
    assert app["resources"] == Resources(memory="1g", cpu=0.5)
    requirements = Path(app["python_requirements"])
    assert requirements.parent.name == "desk_app"
    assert requirements.name == "requirements.txt"


def workflow_arguments(monkeypatch, module: str) -> dict:
    """The arguments that a workflow module gives to @workflow. The module is loaded as a copy."""
    given = {}

    def record(**kwargs):
        given.update(kwargs)
        return lambda function: function

    monkeypatch.setattr(datatailr, "workflow", record)
    spec = importlib.util.spec_from_file_location(f"copy_of_{module}", WORKFLOWS / f"{module}.py")
    copy = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, copy)  # @task looks its module up by name
    spec.loader.exec_module(copy)
    return given


@pytest.mark.parametrize("module, name, budget, requirements", [
    ("daily_curve", "Merit Desk Daily Curve", "Trading", "requirements.txt"),
    ("strategy_sweep", "Merit Desk Strategy Sweep", "Research", "requirements.txt"),
    ("demand_training", "Merit Desk Demand Training", "Research", "training-requirements.txt"),
])
def test_each_workflow_has_its_name_budget_version_and_requirements(
        monkeypatch, tmp_path, module, name, budget, requirements):
    monkeypatch.chdir(tmp_path)  # not the repository root
    given = workflow_arguments(monkeypatch, module)
    assert given["name"] == name
    assert given["budget"] == budget
    assert given["python_version"] == "3.12"
    assert Path(given["python_requirements"]).is_file()
    assert Path(given["python_requirements"]).name == requirements


def test_every_job_name_starts_with_merit_desk(monkeypatch, deployed):
    """The jobs share a cluster with other templates: a plain "Market Feed" replaced theirs."""
    d.deploy_services()
    d.deploy_app()
    d.deploy_excel()
    names = [job["name"] for job in deployed]
    for module in ("daily_curve", "strategy_sweep", "demand_training"):
        names.append(workflow_arguments(monkeypatch, module)["name"])
    assert len(names) == 7, f"expected 7 jobs, found {names}"
    job_names = [name.lower().replace(" ", "-") for name in names]  # how Datatailr names a job
    assert [n for n in job_names if n != "merit-desk" and not n.startswith("merit-desk-")] == []


@pytest.mark.parametrize("module", ["daily_curve", "strategy_sweep", "demand_training"])
def test_no_workflow_is_scheduled_on_a_sunday(monkeypatch, module):
    """The platform writes Sunday as day 7 and then rejects it, so the sweep runs on Saturdays."""
    # No schedule means no weekdays. No local holds the Schedule: pytest -l would repr it on a
    # failure, and its repr calls the platform.
    weekdays = list(getattr(workflow_arguments(monkeypatch, module).get("schedule"),
                            "weekdays", None) or [])
    assert "Sun" not in weekdays


def test_no_budget_name_has_a_space_or_quote(monkeypatch, deployed, offline_sdk, cluster):
    """A space, a double quote, a single quote: the SDK quotes them all. See BUDGET in deploy.py."""
    d.deploy_services()
    d.deploy_app()
    d.deploy_excel()
    names = [job["budget"] for job in deployed]
    for module in ("daily_curve", "strategy_sweep", "demand_training"):
        names.append(workflow_arguments(monkeypatch, module)["budget"])
    names.append(build_run_workflow(new_run("job", "x", {}, None), "dev").budget.name)
    setup_demo.main(["--research", "500", "--trading-desk", "300"])
    names += list(cluster.budgets)  # the names that setup_demo creates
    assert len(names) == 10, f"expected 10 budget names, found {names}"
    assert [name for name in names if any(mark in name for mark in " \"'")] == []
