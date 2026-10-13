"""Deploy the LLM platform: the LLM Gateway service and the LLM Admin app.

Usage::

    python deploy.py          # deploy the gateway, then the admin app
    python deploy.py gateway  # deploy only the LLM Gateway service
    python deploy.py admin    # deploy only the LLM Admin app

Build the UI first (``cd frontend && npm install && npm run build``); the build
output lands in ``llm_platform/admin_app/static`` and ships with the package.

The vLLM services themselves are *not* deployed here: the admin app creates
one per LLM at runtime from ``llm_platform/vllm_service``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import llm_platform.admin_app.app as admin_entrypoint
from datatailr import ACL, App, Group, Permission, Resources, Service, User
from llm_platform.common.naming import ADMIN_APP_NAME, APP_SECTION, GATEWAY_NAME
from llm_platform.gateway.app import main as gateway_main

_HERE = Path(__file__).resolve().parent
_PKG = _HERE / "llm_platform"
ALL_USERS_GROUP = "dtusers"


def _acl(owner: User) -> ACL:
    everyone = Group(ALL_USERS_GROUP)
    return ACL(
        {
            Permission.READ: [owner, everyone],
            Permission.WRITE: [owner],
            Permission.OPERATE: [owner],
            Permission.ACCESS: [owner, everyone],
            Permission.PROMOTE: [owner],
        }
    )


def deploy_gateway() -> None:
    owner = User.signed_user()
    gateway = Service(
        name=GATEWAY_NAME,
        entrypoint=gateway_main,
        run_as=owner,
        resources=Resources(memory="1g", cpu=1),
        python_requirements=str(_PKG / "gateway" / "requirements.txt"),
        env_vars={"LOG_BODIES": "true"},
        acl=_acl(owner),
        app_section=APP_SECTION,
    )
    gateway.run()


def deploy_admin() -> None:
    owner = User.signed_user()
    if not (_PKG / "admin_app" / "static" / "index.html").is_file():
        print("warning: UI not built (llm_platform/admin_app/static/index.html missing). Run `npm run build` in frontend/.")
    admin = App(
        name=ADMIN_APP_NAME,
        entrypoint=admin_entrypoint,
        framework="fastapi",
        run_as=owner,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=str(_PKG / "admin_app" / "requirements.txt"),
        env_vars={"LLM_PLATFORM_ADMINS": owner.name, "LLM_ACCESS_GROUP": ALL_USERS_GROUP},
        acl=_acl(owner),
        app_section=APP_SECTION,
    )
    admin.run()


def deploy_all() -> None:
    deploy_gateway()
    deploy_admin()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    actions = {"all": deploy_all, "gateway": deploy_gateway, "admin": deploy_admin}
    action = actions.get(command)
    if action is None:
        print(__doc__)
        raise SystemExit(1)
    action()
