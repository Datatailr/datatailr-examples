"""Deploy the Neon Postgres service and the explorer app on Datatailr.

Usage:
    NEON_BUCKET=my-database NEON_PG_PASSWORD='a-long-random-password' python deploy.py
    python deploy.py service     # the database only (needs the variables above)
    python deploy.py explorer    # the web app only

Environment variables read at deploy time and passed to the service:
    NEON_BUCKET        directory in blob storage that holds this database (required;
                       every database needs its own)
    NEON_PG_PASSWORD   password of the Postgres superuser; set it, the default
                       cloud_admin is public knowledge
    NEON_ROLE_SECRET   secret that the service and the explorer share; the service
                       derives the password of every per-user Postgres role from it.
                       Derived from NEON_PG_PASSWORD when not set. Set it to "off"
                       to deploy without the user and group sync.

Both jobs get NEON_ROLE_SECRET, so deploy them together or with the same value.
"""

from __future__ import annotations

import os
import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
# Make the package importable when this file is run as a script.
sys.path.append(str(current_dir.parent))

from datatailr import App, Resources, Service  # noqa: E402
from datatailr.logging import CYAN, YELLOW  # noqa: E402

import neon_postgres.explorer.app as explorer_entrypoint  # noqa: E402
from neon_postgres.access import ROLE_SECRET_VAR, derive_role_secret  # noqa: E402
from neon_postgres.service.service import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
EXPLORER_REQUIREMENTS = str(current_dir / "explorer" / "requirements.txt")
BUILD_SCRIPT_PRE = str(current_dir / "service" / "build_pre.sh")
APP_SECTION = "Neon Postgres"


DEFAULT_PASSWORD = "cloud_admin"


def role_secret() -> str | None:
    """The secret both jobs share, or None when the sync is switched off."""
    value = os.environ.get(ROLE_SECRET_VAR, "").strip()
    if value.lower() == "off":
        return None
    if value:
        return value
    return derive_role_secret(os.environ.get("NEON_PG_PASSWORD", DEFAULT_PASSWORD))


def role_env() -> dict[str, str]:
    secret = role_secret()
    return {ROLE_SECRET_VAR: secret} if secret else {}


def deploy_service() -> None:
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
    if role_secret() is None:
        print(YELLOW("NEON_ROLE_SECRET=off: platform users and groups will not become Postgres roles."))
    elif password == DEFAULT_PASSWORD and not os.environ.get(ROLE_SECRET_VAR):
        print(YELLOW("WARNING: the role secret is derived from the default password, so every per-user role password is public too."))
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
            **role_env(),
        },
    )
    service.run()


def deploy_explorer() -> None:
    print(CYAN("Deploying the Neon Postgres Explorer app..."))
    app = App(
        name="Neon Postgres Explorer",
        entrypoint=explorer_entrypoint,
        framework="fastapi",
        resources=Resources(memory="1g", cpu=1),
        app_section=APP_SECTION,
        python_requirements=EXPLORER_REQUIREMENTS,
        env_vars=role_env(),
    )
    app.run()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target not in ("all", "service", "explorer"):
        raise SystemExit(f"unknown target {target!r}: use service, explorer, or nothing for both")
    if target in ("all", "service"):
        deploy_service()
    if target in ("all", "explorer"):
        deploy_explorer()
