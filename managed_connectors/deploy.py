"""Deploy the managed connector gateway and Integration Studio.

Usage::

    python deploy.py          # deploy the service, then the app
    python deploy.py service  # deploy only Connector Gateway
    python deploy.py app      # deploy only Integration Studio
"""

from __future__ import annotations

import os
import sys

import integration_studio_app.app as studio_entrypoint
from connector_gateway_service.app import main as gateway_main
from cryptography.fernet import Fernet
from datatailr import ACL, App, Group, Permission, Resources, Secrets, Service, User


APP_NAME = "integration-studio"
SERVICE_NAME = "connector-gateway"
APP_SECTION = "AI & Integrations"
MASTER_SECRET = os.environ.get(
    "INTEGRATION_STUDIO_MASTER_SECRET", "integration-studio/master-key"
)


def _deployment_identity() -> tuple[User, Group]:
    return User.signed_user(), Group("dtusers")


def _acl(owner: User, all_users: Group) -> ACL:
    return ACL(
        {
            Permission.READ: [owner, all_users],
            Permission.WRITE: [owner],
            Permission.OPERATE: [owner],
            Permission.ACCESS: [owner, all_users],
            Permission.PROMOTE: [owner],
        }
    )


def _check_master_secret() -> None:
    try:
        value = Secrets().get(MASTER_SECRET).strip().encode()
        Fernet(value)
    except Exception as exc:
        raise RuntimeError(
            f"Create a valid Fernet key named '{MASTER_SECRET}' in "
            "Datatailr Secrets Manager before deploying."
        ) from exc


def deploy_service() -> None:
    _check_master_secret()
    owner, all_users = _deployment_identity()
    gateway = Service(
        name=SERVICE_NAME,
        entrypoint=gateway_main,
        run_as=owner,
        resources=Resources(memory="1g", cpu=1),
        python_requirements=["flask", "requests", "cryptography"],
        env_vars={
            "CONNECTOR_GATEWAY_ADMINS": owner.name,
            "INTEGRATION_STUDIO_MASTER_SECRET": MASTER_SECRET,
        },
        acl=_acl(owner, all_users),
    )
    gateway.app_section = APP_SECTION
    gateway.run()


def deploy_app() -> None:
    _check_master_secret()
    owner, all_users = _deployment_identity()
    studio = App(
        name=APP_NAME,
        entrypoint=studio_entrypoint,
        framework="flask",
        run_as=owner,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=[
            "flask",
            "gunicorn",
            "requests",
            "cryptography",
        ],
        env_vars={
            "INTEGRATION_STUDIO_ADMINS": owner.name,
            "INTEGRATION_STUDIO_MASTER_SECRET": MASTER_SECRET,
        },
        acl=_acl(owner, all_users),
        app_section=APP_SECTION,
    )
    studio.run()


def deploy_all() -> None:
    deploy_service()
    deploy_app()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    actions = {"all": deploy_all, "service": deploy_service, "app": deploy_app}
    action = actions.get(command)
    if action is None:
        print(__doc__)
        raise SystemExit(1)
    action()
