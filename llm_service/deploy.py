"""Deploy the LLM service on Datatailr.

Usage:
    python deploy.py            # the service, then the admin app
    python deploy.py service    # only the service
    python deploy.py app        # only the admin app

Environment variables read at deploy time (all are optional):
    LLM_SERVICE_NAME    service name; the internal host name comes from it (default "LLM Service")
    LLM_DEFAULT_MODEL   model loaded at the first start (default Qwen/Qwen2.5-1.5B-Instruct)
    LLM_VRAM            GPU memory of the container (default 16g)
    LLM_MEMORY          memory of the container (default 24g)
    LLM_CPU             CPU count of the container (default 4)
    LLM_ADMIN_GROUP     platform group whose members are admins
    LLM_ACCESS_GROUP    platform group that gets read and access permission
    LLM_BLOB_ROOT       root directory in blob storage (default llm-service)
    LLM_HF_TOKEN_KEY    secret store key of the Hugging Face token, for gated models
    LLM_VLLM_ARGS       extra arguments for `vllm serve`
    LLM_PROMPT_RETENTION_DAYS  days to keep audit prompts (default: keep them)
    LLM_ENVIRONMENT     platform environment: dev (default), pre or prod
    LLM_ADMIN_APP_NAME  name of the admin app (default "LLM Service Admin")

The service sets `vram`, so the platform runs it on a GPU node. The platform
does not start GPU nodes. Start one before you deploy.

The platform signs the calls of the admin app as the user who deploys it. This
user must be an admin of the service.
"""

from __future__ import annotations

import os
import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
# Make the package importable when this file is run as a script.
sys.path.append(str(current_dir.parent))

from datatailr import ACL, App, Group, Permission, Resources, Service, User  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

from llm_service.deploy_config import (  # noqa: E402
    admin_app_name,
    deploy_targets,
    resources,
    service_env,
    service_url,
)
from llm_service.gateway.app import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
ADMIN_APP_REQUIREMENTS = str(current_dir / "admin_app" / "requirements.txt")
SERVICE_NAME = os.environ.get("LLM_SERVICE_NAME", "").strip() or "LLM Service"
ENVIRONMENT = os.environ.get("LLM_ENVIRONMENT", "dev").strip().lower() or "dev"
# vLLM compiles a small C module when it starts. The platform image has no compiler.
BUILD_SCRIPT_PRE = "apt-get update && apt-get install -y build-essential"


def deployment_acl(env=os.environ) -> ACL | None:
    """Return the ACL of the service, or None for the default ACL.

    Without LLM_ACCESS_GROUP, only the deploying user has access. With it, the
    owner and the group get read and access permission. Only the owner can
    change, operate or promote the service.
    """
    group_name = env.get("LLM_ACCESS_GROUP", "").strip()
    if not group_name:
        return None
    owner = User.signed_user()
    group = Group(group_name)
    return ACL(
        {
            Permission.READ: [owner, group],
            Permission.WRITE: [owner],
            Permission.OPERATE: [owner],
            Permission.ACCESS: [owner, group],
            Permission.PROMOTE: [owner],
        }
    )


def build_service(env=os.environ) -> Service:
    """Build the `Service` job from the environment variables."""
    arguments = {}
    acl = deployment_acl(env)
    if acl is not None:
        arguments["acl"] = acl
    return Service(
        name=SERVICE_NAME,
        entrypoint=main,
        environment=ENVIRONMENT,
        resources=Resources(**resources(env)),
        python_requirements=REQUIREMENTS,
        build_script_pre=BUILD_SCRIPT_PRE,
        env_vars=service_env(env),
        **arguments,
    )


def build_app(env=os.environ) -> App:
    """Build the admin app job. It calls the service by its internal URL."""
    import llm_service.admin_app.app as admin_app_module

    arguments = {}
    acl = deployment_acl(env)
    if acl is not None:
        arguments["acl"] = acl
    return App(
        name=admin_app_name(env),
        entrypoint=admin_app_module,
        framework="fastapi",
        environment=ENVIRONMENT,
        resources=Resources(memory="512m", cpu=0.5),
        python_requirements=ADMIN_APP_REQUIREMENTS,
        env_vars={"LLM_SERVICE_URL": service_url(SERVICE_NAME)},
        **arguments,
    )


if __name__ == "__main__":
    try:
        targets = deploy_targets(sys.argv[1:])
    except ValueError as error:
        print(error)
        sys.exit(2)
    if "service" in targets:
        print(CYAN(f"Deploying {SERVICE_NAME!r} to {ENVIRONMENT}. A GPU node must be running."))
        build_service().run()
    if "app" in targets:
        print(CYAN(f"Deploying {admin_app_name(os.environ)!r} to {ENVIRONMENT}."))
        build_app().run()
