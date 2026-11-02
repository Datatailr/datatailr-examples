import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]


def test_deploy_module_imports_without_deploying():
    import merit_desk.deploy as d

    assert set(d.TARGETS) == {"workflows", "training", "services", "app", "excel", "all"}


def test_importing_deploy_loads_no_job_module():
    # A fresh interpreter, because other tests have imported the job modules already.
    code = ("import sys, merit_desk.deploy; "
            "print(sorted(m for m in sys.modules if m.startswith('merit_desk.')))")
    done = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "['merit_desk.deploy']"


def test_importing_setup_demo_changes_nothing_on_the_cluster(monkeypatch):
    import datatailr

    calls = []

    def record(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(datatailr.Group, "exists", record)
    monkeypatch.setattr(datatailr.Group, "add", record)
    monkeypatch.setattr(datatailr.Budget, "get", record)
    monkeypatch.setattr(datatailr.Budget, "add", record)
    monkeypatch.setattr(datatailr.KV, "put", record)
    monkeypatch.delitem(sys.modules, "merit_desk.admin.setup_demo", raising=False)

    from merit_desk.admin import setup_demo

    assert calls == []
    assert callable(setup_demo.main)


def test_unknown_target_stops_before_importing_a_job():
    # Run the script in a fresh interpreter, then list the merit_desk modules that it loaded.
    code = ("import runpy, sys\n"
            "sys.argv = ['merit_desk/deploy.py', 'nonsense']\n"
            "try:\n"
            "    runpy.run_path('merit_desk/deploy.py', run_name='__main__')\n"
            "except SystemExit as stop:\n"
            "    print('exit', stop.code)\n"
            "print(sorted(m for m in sys.modules if m.startswith('merit_desk')))\n")
    done = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert "Unknown target: nonsense" in done.stdout
    lines = done.stdout.strip().splitlines()
    assert "exit 1" in lines
    assert lines[-1] == "[]"  # no job module, and not even the merit_desk package, was loaded


def test_metadata_card_has_the_four_keys():
    card = json.loads((ROOT / "merit_desk" / "metadata.json").read_text(encoding="utf-8"))
    assert card == {
        "name": "Merit Desk",
        "description": "A small power trading desk on Datatailr: a daily forward curve from ECMWF "
                       "forecasts, a strategy sweep that fans out, GPU training, two services, "
                       "a FastAPI desk app that sends each calculation to Datatailr as a job, "
                       "and an Excel add-in.",
        "tags": ["energy", "power", "workflows", "dynamic dag", "schedules", "services", "fastapi",
                 "excel", "gpu", "joblib", "blob storage", "secrets", "demo"],
        "author": "Datatailr",
    }


@pytest.mark.parametrize("target", ["workflows", "training", "services", "app", "excel", "all"])
def test_every_target_is_documented_in_the_usage(target):
    import merit_desk.deploy as d

    assert f"python merit_desk/deploy.py {target}" in d.__doc__
    assert callable(d.TARGETS[target])


def test_readme_carries_the_ecmwf_credit_disclaimer_and_links():
    from merit_desk.common.weather import CREDIT

    readme = (ROOT / "merit_desk" / "README.md").read_text(encoding="utf-8")
    assert CREDIT in readme
    assert ("ECMWF does not accept any liability whatsoever for any error or omission in the "
            "data, their availability, or for any loss or damage arising from their use.") in readme
    assert "https://www.ecmwf.int/en/forecasts/datasets/open-data" in readme
    assert "https://apps.ecmwf.int/datasets/licences/general/" in readme
    assert "© 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)" in readme


def test_all_deploys_everything_except_the_training(monkeypatch):
    import merit_desk.deploy as d

    deployed = []
    for name in ("workflows", "training", "services", "app", "excel"):
        monkeypatch.setattr(d, f"deploy_{name}", lambda name=name: deployed.append(name))
    d.deploy_all()  # the targets are recorders now, so nothing is deployed
    assert deployed == ["workflows", "services", "app", "excel"]
    assert "except demand training" in d.__doc__
