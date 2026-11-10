"""Pure helpers of the deploy script. They import nothing from `datatailr`."""

from __future__ import annotations

from typing import Mapping

DEFAULT_MODEL = "Qwen/Qwen2.5-1.5B-Instruct"
DEFAULT_BLOB_ROOT = "llm-service"
DEFAULT_MEMORY = "24g"
DEFAULT_CPU = "4"
DEFAULT_VRAM = "16g"

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
