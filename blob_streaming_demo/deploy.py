"""Deploy the blob storage streaming demo on Datatailr.

Components:
    1. ``Parquet Mock API`` (Service) - serves streamed parquet files of
       arbitrary size; mimics an external data provider.
    2. ``Blob Streaming Demo`` (App, Streamlit) - calls the service and
       streams the response straight into Datatailr blob storage via
       ``dt blob put``, while showing live progress.

Usage:
    python deploy.py            # deploy both
    python deploy.py service    # service only
    python deploy.py app        # dashboard only
"""

from __future__ import annotations

import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
# Make the package importable when this file is run as a script.
sys.path.append(str(current_dir.parent))

from datatailr import App, Resources, Service  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

import blob_streaming_demo.dashboard.app as dashboard_entrypoint  # noqa: E402
from blob_streaming_demo.api_service.service import main as api_main  # noqa: E402


REQUIREMENTS = str(current_dir / "requirements.txt")
APP_SECTION = "Blob Storage Demo"


def deploy_service() -> None:
    print(CYAN("Deploying mock parquet API service..."))
    service = Service(
        name="Parquet Mock API",
        entrypoint=api_main,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=REQUIREMENTS,
    )
    service.run()


def deploy_dashboard() -> None:
    print(CYAN("Deploying blob streaming dashboard..."))
    app = App(
        name="Blob Streaming Demo",
        entrypoint=dashboard_entrypoint,
        framework="streamlit",
        resources=Resources(memory="2g", cpu=1),
        app_section=APP_SECTION,
        python_requirements=REQUIREMENTS,
    )
    app.run()


def deploy_all() -> None:
    deploy_service()
    deploy_dashboard()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "all":
        deploy_all()
    elif cmd == "service":
        deploy_service()
    elif cmd == "app":
        deploy_dashboard()
    else:
        print(__doc__)
        sys.exit(1)
