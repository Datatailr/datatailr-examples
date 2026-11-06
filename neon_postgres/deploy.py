"""Deploy the Neon Postgres service on Datatailr.

Usage:
    NEON_BUCKET=my-database NEON_PG_PASSWORD='a-long-random-password' python deploy.py

Environment variables read at deploy time and passed to the service:
    NEON_BUCKET        directory in blob storage that holds this database (required;
                       every database needs its own)
    NEON_PG_PASSWORD   password of the Postgres superuser; set it, the default
                       cloud_admin is public knowledge
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
    bucket = os.environ.get("NEON_BUCKET", "").strip("/ ")
    if not bucket:
        raise SystemExit(
            "NEON_BUCKET is not set. Choose the blob storage directory that holds this database, "
            "for example: NEON_BUCKET=my-database python deploy.py"
        )
    password = os.environ.get("NEON_PG_PASSWORD", DEFAULT_PASSWORD)
    if password == DEFAULT_PASSWORD:
        print(
            YELLOW(
                "WARNING: NEON_PG_PASSWORD is not set. The cloud_admin password will be the "
                "default from the README, and every job in the environment can reach port 5432."
            )
        )
    print(CYAN(f"Deploying the Neon Postgres service with its data in blob storage under {bucket}/ ..."))
    service = Service(
        name="Neon Postgres",
        entrypoint=main,
        resources=Resources(memory="4g", cpu=2),
        python_requirements=REQUIREMENTS,
        build_script_pre=BUILD_SCRIPT_PRE,
        env_vars={
            "NEON_PG_PASSWORD": password,
            "NEON_BUCKET": bucket,
            # Upload WAL and layers at most one minute after they are written.
            "NEON_PARTIAL_BACKUP_TIMEOUT": "1m",
            "NEON_CHECKPOINT_TIMEOUT": "1m",
        },
    )
    service.run()


if __name__ == "__main__":
    deploy()
