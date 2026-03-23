from datatailr import App, Resources
from datatailr.logging import CYAN

import dashboard.app as dashboard_entrypoint
from workflows.tasks import create_backtest_workflow


def deploy_workflow():
    workflow_fn = create_backtest_workflow()
    print(CYAN("Deploying workflow definition..."))
    workflow_fn(save_only=True)


def deploy_app():
    app = App(
        name="Portfolio Backtesting Dashboard",
        entrypoint=dashboard_entrypoint,
        framework="flask",
        resources=Resources(memory="2g", cpu=1),
        python_requirements=[
            "flask",
            "gunicorn",
            "vectorbt",
            "pandas",
            "numpy",
            "yfinance",
        ],
    )
    print(CYAN("Deploying Flask dashboard app..."))
    app.run()


def deploy_all():
    # deploy_workflow()
    deploy_app()


if __name__ == "__main__":
    deploy_all()
