"""Deploy script for Smart Building Energy Intelligence demo."""

from __future__ import annotations

import pathlib
import sys

from datatailr import App, Resources, Service

current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from smart_building_energy.analytics_api.service import main as analytics_api_main
from smart_building_energy.compaction_workflow.deploy import hourly_compaction_workflow
from smart_building_energy.data_pipelines.deploy import processing_workflow
import smart_building_energy.dashboard.app as dashboard_entrypoint
from smart_building_energy.sensor_ingestor.service import main as sensor_ingestor_main

requirements_file = current_dir / "requirements.txt"
assert requirements_file.exists(), f"Requirements file not found: {requirements_file}"
requirements_file = str(requirements_file)


def collector_service() -> Service:
    return Service(
        name="Sensor Ingestor",
        entrypoint=sensor_ingestor_main,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=requirements_file,
    )


def analytics_service() -> Service:
    return Service(
        name="Building Analytics API",
        entrypoint=analytics_api_main,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=requirements_file,
    )


def dashboard_app() -> App:
    return App(
        name="Energy Intelligence Dashboard",
        entrypoint=dashboard_entrypoint,
        framework="flask",
        app_section="Smart Building Energy",
        resources=Resources(memory="2g", cpu=1),
        python_requirements=["flask", "requests", "gunicorn"],
    )


def deploy_collector() -> None:
    collector_service().run()


def deploy_api() -> None:
    analytics_service().run()


def deploy_dashboard() -> None:
    dashboard_app().run()


def deploy_processing_workflow() -> None:
    processing_workflow()


def deploy_compaction_workflow() -> None:
    hourly_compaction_workflow()


def deploy_all() -> None:
    deploy_collector()
    deploy_api()
    deploy_dashboard()
    deploy_processing_workflow()
    deploy_compaction_workflow()


if __name__ == "__main__":
    usage = """
python deploy.py <command>
Commands:
- all
- collector
- api
- dashboard
- workflow
- compaction
"""
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    cmd = sys.argv[1].strip().lower()
    if cmd == "all":
        deploy_all()
    elif cmd == "collector":
        deploy_collector()
    elif cmd == "api":
        deploy_api()
    elif cmd == "dashboard":
        deploy_dashboard()
    elif cmd == "workflow":
        deploy_processing_workflow()
    elif cmd == "compaction":
        deploy_compaction_workflow()
    else:
        print(f"Unknown command: {cmd}")
        print(usage)
        sys.exit(1)

