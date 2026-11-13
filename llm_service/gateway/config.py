"""Configuration of the gateway, read from environment variables."""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .fit import parse_memory_mib

MAX_RETENTION_DAYS = 36500  # 100 years


def _retention_days(value: str | None, name: str = "LLM_PROMPT_RETENTION_DAYS") -> int | None:
    """Return the days to keep data, or None for no limit. Raise `ValueError` for a bad value."""
    text = (value or "").strip()
    if not text:
        return None
    days = int(text) if text.isascii() and text.isdigit() and len(text) <= 10 else 0
    if 1 <= days <= MAX_RETENTION_DAYS:
        return days
    raise ValueError(
        f"{name} must be a whole number from 1 to {MAX_RETENTION_DAYS}, "
        f"not {text[:40]!r}"
    )


@dataclass(frozen=True)
class Config:
    """Settings of one gateway process. Build it with `Config.from_env`."""

    default_model: str = "Qwen/Qwen2.5-1.5B-Instruct"
    blob_root: str = "llm-service"
    admin_group: str | None = None
    access_group: str | None = None
    hf_token_key: str | None = None
    vllm_args: list[str] = field(default_factory=list)
    owner: str = ""
    local_dir: Path = Path("/tmp/llm-service")
    engine_port: int = 8001
    flush_interval: float = 5.0
    flush_size: int = 100
    prompt_retention_days: int | None = None
    metrics_retention_days: int = 30
    gpu_memory_mib: int | None = None
    job_name: str = "unknown"
    environment: str = "unknown"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Config":
        """Read the settings from `env`. Missing values use the defaults."""
        return cls(
            default_model=env.get("LLM_DEFAULT_MODEL") or cls.default_model,
            blob_root=(env.get("LLM_BLOB_ROOT") or cls.blob_root).strip("/"),
            admin_group=env.get("LLM_ADMIN_GROUP") or None,
            access_group=env.get("LLM_ACCESS_GROUP") or None,
            hf_token_key=env.get("LLM_HF_TOKEN_KEY") or None,
            vllm_args=shlex.split(env.get("LLM_VLLM_ARGS", "")),
            owner=env.get("DATATAILR_USER", ""),
            local_dir=Path(env.get("LLM_LOCAL_DIR") or "/tmp/llm-service"),
            engine_port=int(env.get("LLM_ENGINE_PORT") or 8001),
            prompt_retention_days=_retention_days(env.get("LLM_PROMPT_RETENTION_DAYS")),
            metrics_retention_days=(
                _retention_days(env.get("LLM_METRICS_RETENTION_DAYS"), "LLM_METRICS_RETENTION_DAYS")
                or cls.metrics_retention_days
            ),
            gpu_memory_mib=parse_memory_mib(env.get("LLM_VRAM")),
            job_name=env.get("DATATAILR_JOB_NAME") or cls.job_name,
            environment=env.get("DATATAILR_JOB_ENVIRONMENT") or cls.environment,
        )
