"""Deploy the LLM service on Datatailr.

Usage:
    python deploy.py

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
    LLM_ENVIRONMENT     platform environment: dev (default), pre or prod

The service sets `vram`, so the platform runs it on a GPU node. The platform
does not start GPU nodes. Start one before you deploy.
"""

from __future__ import annotations

import os
import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
# Make the package importable when this file is run as a script.
sys.path.append(str(current_dir.parent))

from datatailr import ACL, Group, Permission, Resources, Service, User  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

from llm_service.deploy_config import resources, service_env  # noqa: E402
from llm_service.gateway.app import main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
SERVICE_NAME = os.environ.get("LLM_SERVICE_NAME", "").strip() or "LLM Service"
ENVIRONMENT = os.environ.get("LLM_ENVIRONMENT", "dev").strip().lower() or "dev"


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
        env_vars=service_env(env),
        **arguments,
    )


if __name__ == "__main__":
    print(CYAN(f"Deploying {SERVICE_NAME!r} to {ENVIRONMENT}. A GPU node must be running."))
    build_service().run()
