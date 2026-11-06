"""Deploy the Neon Postgres service on Datatailr.

Usage:
    python deploy.py

Environment variables read at deploy time and passed to the service:
    NEON_PG_PASSWORD   password of the Postgres superuser; set it, the default
                       cloud_admin is public knowledge
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
from datatailr.logging import CYAN, YELLOW  # noqa: E402

from neon_postgres.service.service import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
BUILD_SCRIPT_PRE = str(current_dir / "service" / "build_pre.sh")


DEFAULT_PASSWORD = "cloud_admin"


def deploy() -> None:
    password = os.environ.get("NEON_PG_PASSWORD", DEFAULT_PASSWORD)
    if password == DEFAULT_PASSWORD:
        print(
            YELLOW(
                "WARNING: NEON_PG_PASSWORD is not set. The cloud_admin password will be the "
                "default from the README, and every job in the environment can reach port 5432."
            )
        )
    print(CYAN("Deploying the Neon Postgres service..."))
    service = Service(
        name="Neon Postgres",
        entrypoint=main,
        resources=Resources(memory="4g", cpu=2),
        python_requirements=REQUIREMENTS,
        build_script_pre=BUILD_SCRIPT_PRE,
        env_vars={
            "NEON_PG_PASSWORD": password,
            "NEON_BUCKET": os.environ.get("NEON_BUCKET", "neon"),
            # Upload WAL and layers at most one minute after they are written.
            "NEON_PARTIAL_BACKUP_TIMEOUT": "1m",
            "NEON_CHECKPOINT_TIMEOUT": "1m",
        },
    )
    service.run()


if __name__ == "__main__":
    deploy()
