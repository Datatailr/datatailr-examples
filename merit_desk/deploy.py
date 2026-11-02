"""Deploy the Merit Desk jobs to Datatailr.

Run it from the repository root, with one target:

    python merit_desk/deploy.py workflows   # Daily Curve and Strategy Sweep
    python merit_desk/deploy.py training    # Demand Training, which needs a GPU node
    python merit_desk/deploy.py services    # Pricing API and Market Feed
    python merit_desk/deploy.py app         # Merit Desk, the FastAPI desk app
    python merit_desk/deploy.py excel       # Desk Tools, the Excel add-in
    python merit_desk/deploy.py all         # everything except demand training (the default)

"all" deploys everything except demand training. Deploy that with "training", after an
administrator starts a GPU node. The workflow has no schedule, so it runs once at deploy.

The budgets "Trading desk" and "Research" must exist first.
An administrator creates them with merit_desk/admin/setup_demo.py.
"""

import sys
from pathlib import Path

from datatailr import App, ExcelAddin, Resources, Service

ROOT = Path(__file__).parent
SECTION = "Trading"  # the launcher section for the app, the services and the add-in
BUDGET = "Trading desk"


def requirements(folder: str) -> str:
    return str(ROOT / folder / "requirements.txt")


def deploy_workflows():
    from merit_desk.workflows.daily_curve import daily_curve
    from merit_desk.workflows.strategy_sweep import strategy_sweep

    daily_curve()
    strategy_sweep()


def deploy_training():
    from merit_desk.workflows.demand_training import demand_training

    demand_training()


def deploy_services():
    from merit_desk.services import market_feed, pricing_api

    Service(
        name="Pricing API",  # other jobs reach it at http://pricing-api
        entrypoint=pricing_api.main,
        resources=Resources(memory="1g", cpu=0.5),
        python_version="3.12",
        python_requirements=requirements("services"),
        app_section=SECTION,
        budget=BUDGET,
    ).run()
    Service(
        name="Market Feed",  # http://market-feed
        entrypoint=market_feed.main,
        resources=Resources(memory="512m", cpu=0.25),
        python_version="3.12",
        python_requirements=requirements("services"),
        app_section=SECTION,
        budget=BUDGET,
    ).run()


def deploy_app():
    import merit_desk.desk_app.app as desk_app_entrypoint

    App(
        name="Merit Desk",
        entrypoint=desk_app_entrypoint,
        framework="fastapi",
        resources=Resources(memory="1g", cpu=0.5),
        python_version="3.12",
        python_requirements=requirements("desk_app"),
        app_section=SECTION,
        budget=BUDGET,
    ).run()


def deploy_excel():
    from merit_desk.excel.desk_tools import main

    ExcelAddin(
        name="Desk Tools",
        entrypoint=main,
        python_version="3.12",
        python_requirements=requirements("excel"),
        app_section=SECTION,
        budget=BUDGET,
    ).run()


def deploy_all():  # not the training: it runs once at deploy and waits for a GPU node
    deploy_workflows()
    deploy_services()
    deploy_app()
    deploy_excel()


TARGETS = {
    "workflows": deploy_workflows,
    "training": deploy_training,
    "services": deploy_services,
    "app": deploy_app,
    "excel": deploy_excel,
    "all": deploy_all,
}

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target not in TARGETS:
        print(f"Unknown target: {target}")
        print(__doc__)
        sys.exit(1)
    sys.path.append(str(ROOT.parent))  # so "python merit_desk/deploy.py" finds the package
    TARGETS[target]()
