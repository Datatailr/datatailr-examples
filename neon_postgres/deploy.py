"""Deploy the Neon Postgres service on Datatailr.

Usage:
    python deploy.py

Environment variables read at deploy time and passed to the service:
    NEON_PG_PASSWORD   password of the Postgres superuser (default: cloud_admin)
    NEON_BUCKET        directory in blob storage that holds the database (default: neon)
"""

from __future__ import annotations

import os
import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
# Make the package importable when this file is run as a script.
sys.path.append(str(current_dir.parent))

from datatailr import Resources, Service  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

from neon_postgres.service.service import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
BUILD_SCRIPT_PRE = str(current_dir / "service" / "build_pre.sh")


def deploy() -> None:
    print(CYAN("Deploying the Neon Postgres service..."))
    service = Service(
        name="Neon Postgres",
        entrypoint=main,
        resources=Resources(memory="4g", cpu=2),
        python_requirements=REQUIREMENTS,
        build_script_pre=BUILD_SCRIPT_PRE,
        env_vars={
            "NEON_PG_PASSWORD": os.environ.get("NEON_PG_PASSWORD", "cloud_admin"),
            "NEON_BUCKET": os.environ.get("NEON_BUCKET", "neon"),
            # Upload WAL and layers at most one minute after they are written.
            "NEON_PARTIAL_BACKUP_TIMEOUT": "1m",
            "NEON_CHECKPOINT_TIMEOUT": "1m",
        },
    )
    service.run()


if __name__ == "__main__":
    deploy()
