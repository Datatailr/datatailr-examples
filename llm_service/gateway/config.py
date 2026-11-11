"""Configuration of the gateway, read from environment variables."""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class Config:
    """Settings of one gateway process. Build it with `Config.from_env`."""

    default_model: str = "Qwen/Qwen2.5-1.5B-Instruct"
    blob_root: str = "llm-service"
    admin_group: str | None = None
    hf_token_key: str | None = None
    vllm_args: list[str] = field(default_factory=list)
    owner: str = ""
    local_dir: Path = Path("/tmp/llm-service")
    engine_port: int = 8001
    flush_interval: float = 5.0
    flush_size: int = 100

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Config":
        """Read the settings from `env`. Missing values use the defaults."""
        return cls(
            default_model=env.get("LLM_DEFAULT_MODEL") or cls.default_model,
            blob_root=(env.get("LLM_BLOB_ROOT") or cls.blob_root).strip("/"),
            admin_group=env.get("LLM_ADMIN_GROUP") or None,
            hf_token_key=env.get("LLM_HF_TOKEN_KEY") or None,
            vllm_args=shlex.split(env.get("LLM_VLLM_ARGS", "")),
            owner=env.get("DATATAILR_USER", ""),
            local_dir=Path(env.get("LLM_LOCAL_DIR") or "/tmp/llm-service"),
            engine_port=int(env.get("LLM_ENGINE_PORT") or 8001),
        )
