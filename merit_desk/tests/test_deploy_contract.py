"""What each job gives to Datatailr, checked with recorders: nothing is deployed."""

import importlib.util
import sys
from pathlib import Path

import datatailr
import pytest
from datatailr import Resources

import merit_desk.deploy as d

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
    (d.deploy_services, ["Pricing API", "Market Feed"]),
    (d.deploy_app, ["Merit Desk"]),
    (d.deploy_excel, ["Desk Tools"]),
])
def test_each_job_has_its_name_budget_section_version_and_requirements(deployed, deploy, names):
    deploy()
    assert [job["name"] for job in deployed] == names
    for job in deployed:
        assert job["budget"] == "Trading desk"
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
    ("daily_curve", "Merit Desk Daily Curve", "Trading desk", "requirements.txt"),
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
