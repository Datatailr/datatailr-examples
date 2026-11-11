"""Pure helpers of the deploy script. They import nothing from `datatailr`."""

from __future__ import annotations

import re
import unicodedata
from typing import Mapping, Sequence

DEFAULT_MODEL = "Qwen/Qwen2.5-1.5B-Instruct"
DEFAULT_BLOB_ROOT = "llm-service"
DEFAULT_MEMORY = "24g"
DEFAULT_CPU = "4"
DEFAULT_VRAM = "16g"
DEFAULT_ADMIN_APP_NAME = "LLM Service Admin"
TARGETS = ("service", "app")
USAGE = "usage: python deploy.py [service|app]"

# Optional variables. The service gets each one only when it is set.
OPTIONAL_VARIABLES = ("LLM_ADMIN_GROUP", "LLM_HF_TOKEN_KEY", "LLM_VLLM_ARGS")


def _value(env: Mapping[str, str], name: str, default: str = "") -> str:
    """Return the stripped value of `name`, or `default` when it is empty or not set."""
    return env.get(name, "").strip() or default


def service_env(env: Mapping[str, str]) -> dict[str, str]:
    """Return the `LLM_*` variables that the service container needs."""
    result = {
        "LLM_DEFAULT_MODEL": _value(env, "LLM_DEFAULT_MODEL", DEFAULT_MODEL),
        "LLM_BLOB_ROOT": _value(env, "LLM_BLOB_ROOT", DEFAULT_BLOB_ROOT).strip("/") or DEFAULT_BLOB_ROOT,
    }
    for name in OPTIONAL_VARIABLES:
        value = _value(env, name)
        if value:
            result[name] = value
    return result


def resources(env: Mapping[str, str]) -> dict:
    """Return the container size as the arguments of `Resources`."""
    return {
        "memory": _value(env, "LLM_MEMORY", DEFAULT_MEMORY),
        "cpu": float(_value(env, "LLM_CPU", DEFAULT_CPU)),
        "vram": _value(env, "LLM_VRAM", DEFAULT_VRAM),
    }


def service_url(service_name: str) -> str:
    """Return the internal URL of the service. `LLM Service` gives `http://llm-service`.

    The platform applies NFKC before the lower case step, so full-width
    letters give the same ASCII host name.
    """
    name = unicodedata.normalize("NFKC", service_name)
    host = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return f"http://{host}"


def admin_app_name(env: Mapping[str, str]) -> str:
    """Return the name of the admin app job."""
    return _value(env, "LLM_ADMIN_APP_NAME", DEFAULT_ADMIN_APP_NAME)


def deploy_targets(argv: Sequence[str]) -> tuple[str, ...]:
    """Return the jobs to deploy for the arguments of `deploy.py`.

    No argument deploys the service first, then the app. Raise `ValueError`
    with the usage text for any other argument.
    """
    if not argv:
        return TARGETS
    if len(argv) == 1 and argv[0] in TARGETS:
        return (argv[0],)
    raise ValueError(USAGE)
