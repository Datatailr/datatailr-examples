"""The notebooks: no stored outputs, and able to import the package from their own folder."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
NOTEBOOKS = ROOT / "merit_desk" / "notebooks"
PATHS = sorted(NOTEBOOKS.glob("*.ipynb"))


def read_cells(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))["cells"]


def source(cell: dict) -> str:
    return "".join(cell["source"])


def test_both_notebooks_are_there():
    assert [p.name for p in PATHS] == ["01_instant_run.ipynb", "02_tune_demand_model.ipynb"]


@pytest.mark.parametrize("path", PATHS, ids=lambda path: path.name)
def test_notebook_has_no_stored_outputs(path):
    for cell in read_cells(path):
        if cell["cell_type"] == "code":
            assert cell["outputs"] == [] and cell["execution_count"] is None


@pytest.mark.parametrize("path", PATHS, ids=lambda path: path.name)
def test_first_cell_makes_the_package_importable_from_the_notebook_folder(path):
    first = source(read_cells(path)[0])
    code = f"{first}\nimport merit_desk.common.demand_model, merit_desk.workflows.daily_curve"
    done = subprocess.run([sys.executable, "-c", code], cwd=NOTEBOOKS,  # a kernel starts here
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def test_instant_run_notebook_shows_the_run_and_waits_before_it_asks_for_a_result():
    cells = read_cells(NOTEBOOKS / "01_instant_run.ipynb")
    sources = [source(cell) for cell in cells]
    started = next(i for i, text in enumerate(sources) if "instant_run=True" in text)
    assert sources[started].splitlines()[-1] == "run"  # a last expression shows the widget
    asked = next(i for i, text in enumerate(sources) if "run.result(" in text)
    assert cells[asked - 1]["cell_type"] == "markdown"  # the note: run this when the run is done


def test_dev_requirements_list_cloudpickle_for_an_instant_run_from_a_notebook():
    names = (ROOT / "merit_desk" / "requirements-dev.txt").read_text(encoding="utf-8").split()
    assert "cloudpickle" in names


def test_dev_requirements_give_the_workstation_what_the_joblib_notebook_needs():
    # The workstation image keeps its own older SDK for a bare "datatailr", the joblib
    # workers need websockets, and they unpickle calls that refer to IPython (ipywidgets).
    names = (ROOT / "merit_desk" / "requirements-dev.txt").read_text(encoding="utf-8").split()
    assert "datatailr>=0.2.7" in names
    assert "websockets" in names and "ipywidgets" in names
