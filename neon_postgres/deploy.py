"""Deploy the Neon Postgres service and the explorer app on Datatailr.

Usage:
    NEON_BUCKET=my-database NEON_PG_PASSWORD='a-long-random-password' python deploy.py
    python deploy.py service     # the database only (needs the variables above)
    python deploy.py explorer    # the web app only

Environment variables read at deploy time:
    NEON_BUCKET            directory in blob storage that holds this database (required;
                           every database needs its own)
    NEON_PG_PASSWORD       password of the Postgres superuser; set it, the default
                           cloud_admin is public knowledge
    NEON_ENVIRONMENT       platform environment to deploy to: dev (default), pre or prod
    NEON_ROLE_SECRET_KEY   name of the secret, in the platform's secret store, from
                           which the service derives every per-user Postgres role
                           password (default neon-postgres/role-secret). The deploy
                           creates it with a random value when it is missing and
                           passes the name to both jobs, which read it at runtime.
    NEON_ROLE_SECRET       a value to store under that name, replacing any existing
                           one. "off" deploys without the user and group sync.
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
from neon_postgres import access  # noqa: E402
from neon_postgres.service.service import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
EXPLORER_REQUIREMENTS = str(current_dir / "explorer" / "requirements.txt")
BUILD_SCRIPT_PRE = str(current_dir / "service" / "build_pre.sh")
APP_SECTION = "Neon Postgres"


DEFAULT_PASSWORD = "cloud_admin"
ENVIRONMENT = os.environ.get("NEON_ENVIRONMENT", "dev").strip().lower() or "dev"


def role_sync_off() -> bool:
    return os.environ.get(access.ROLE_SECRET_VAR, "").strip().lower() == "off"


def role_env() -> dict[str, str]:
    """The variables both jobs get: the store key of the role secret, or nothing.

    The store keeps keys in lowercase, so the key is normalised the same way
    here and inside the jobs, and both sides name the same entry.
    """
    if role_sync_off():
        return {}
    key = access.normalize_key(os.environ.get(access.ROLE_SECRET_KEY_VAR, "")) or access.DEFAULT_ROLE_SECRET_KEY
    return {access.ROLE_SECRET_KEY_VAR: key}


def prepare_role_secret() -> None:
    """Create the role secret in the environment's secret store, unless it is there already.

    Runs ``dt kv put -s -e <environment>`` through the SDK. The value is random
    unless NEON_ROLE_SECRET holds one, in which case that value replaces the
    stored one, which is how you rotate every role password.
    """
    if role_sync_off():
        print(YELLOW("NEON_ROLE_SECRET=off: platform users and groups will not become Postgres roles."))
        return
    key = role_env()[access.ROLE_SECRET_KEY_VAR]
    value = os.environ.get(access.ROLE_SECRET_VAR, "").strip() or None
    stored_key, action = access.ensure_role_secret(ENVIRONMENT, key, value)
    if action == "created":
        print(CYAN(f"Created the role secret {stored_key!r} in the {ENVIRONMENT} secret store with a random value."))
    elif action == "stored":
        print(CYAN(f"Stored NEON_ROLE_SECRET as {stored_key!r} in the {ENVIRONMENT} secret store; the roles get new passwords at the next sync."))
    else:
        print(CYAN(f"The {ENVIRONMENT} secret store already holds the role secret {stored_key!r}; keeping it."))
    given = os.environ.get(access.ROLE_SECRET_KEY_VAR, "").strip()
    if given and given != stored_key:
        print(YELLOW(f"The store keeps keys in lowercase, so the secret is named {stored_key!r}, not {given!r}."))


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
    print(CYAN(f"Deploying the Neon Postgres service to {ENVIRONMENT} with its data in blob storage under {bucket}/ ..."))
    service = Service(
        name="Neon Postgres",
        entrypoint=main,
        environment=ENVIRONMENT,
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
    print(CYAN(f"Deploying the Neon Postgres Explorer app to {ENVIRONMENT}..."))
    app = App(
        name="Neon Postgres Explorer",
        entrypoint=explorer_entrypoint,
        environment=ENVIRONMENT,
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
    prepare_role_secret()
    if target in ("all", "service"):
        deploy_service()
    if target in ("all", "explorer"):
        deploy_explorer()
