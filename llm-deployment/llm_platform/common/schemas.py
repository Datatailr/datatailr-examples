"""Pydantic models for the LLM platform registry and API payloads."""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from llm_platform.common.naming import (
    service_display_name,
    service_internal_name,
    service_internal_url,
    slugify,
)

MIN_DISK_SIZE_GB = 50


def now() -> float:
    return time.time()


class DeploymentState(str, Enum):
    SAVING = "saving"
    PROVISIONING_NODE = "provisioning_node"
    WAITING_NODE = "waiting_node"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


ACTIVE_STATES = {
    DeploymentState.SAVING,
    DeploymentState.PROVISIONING_NODE,
    DeploymentState.WAITING_NODE,
    DeploymentState.STARTING,
    DeploymentState.RUNNING,
}


class Event(BaseModel):
    ts: float = Field(default_factory=now)
    level: Literal["info", "warning", "error"] = "info"
    message: str
    deployment: str | None = None


class DeploymentSpec(BaseModel):
    """What the admin asked for. Changing the spec re-saves the service."""

    name: str
    model: str = Field(description="Hugging Face model id, e.g. Qwen/Qwen2.5-0.5B-Instruct")
    environment: str = "dev"
    instance_type: str = Field(description="Cloud GPU instance type used when a node must be created")
    disk_size: int = Field(default=100, ge=MIN_DISK_SIZE_GB)
    vram: str = "12g"
    cpu: float = 2
    memory: str = "16g"
    extra_args: str = Field(default="", description="Extra `vllm serve` arguments")
    hf_token_secret: str = Field(default="", description="Name of a Datatailr secret holding a Hugging Face token")
    description: str = ""

    @field_validator("name")
    @classmethod
    def _name_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name is required")
        slugify(value)
        return value

    @field_validator("model")
    @classmethod
    def _model_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("model is required")
        return value


class Deployment(BaseModel):
    slug: str
    spec: DeploymentSpec
    desired: Literal["running", "stopped"] = "running"
    state: DeploymentState = DeploymentState.SAVING
    state_since: float = Field(default_factory=now)
    created_at: float = Field(default_factory=now)
    created_by: str = ""
    updated_at: float = Field(default_factory=now)
    service_version: int | None = None
    run_id: int | None = None
    node_id: str | None = None
    node_name: str | None = None
    last_error: str | None = None
    pending_model: str | None = None
    spec_dirty: bool = Field(default=True, description="True when the spec changed since the service was last saved")
    build_nonce: int = Field(default=0, description="Bumped to force an image rebuild after a failed build")
    retry_requested: bool = False
    events: list[Event] = Field(default_factory=list)

    # ---- derived -------------------------------------------------------
    @property
    def service_name(self) -> str:
        return service_display_name(self.slug)

    @property
    def internal_name(self) -> str:
        return service_internal_name(self.slug)

    @property
    def internal_url(self) -> str:
        return service_internal_url(self.slug)

    def transition(self, state: DeploymentState, message: str | None = None, level: str = "info") -> None:
        if state != self.state:
            self.state = state
            self.state_since = now()
        self.updated_at = now()
        if message:
            self.add_event(message, level=level)

    def add_event(self, message: str, level: str = "info") -> None:
        self.events.append(Event(level=level, message=message, deployment=self.slug))  # type: ignore[arg-type]
        if len(self.events) > 200:
            self.events = self.events[-200:]

    def public_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["service_name"] = self.service_name
        data["internal_name"] = self.internal_name
        data["internal_url"] = self.internal_url
        return data


class ManagedNode(BaseModel):
    """A GPU node this platform created, remembered so the UI can label it."""

    node_id: str | None = None
    name: str | None = None
    instance_type: str
    environment: str
    disk_size: int
    created_at: float = Field(default_factory=now)
    created_for: str
    created_by: str = ""
    request_result: Any = None


class Registry(BaseModel):
    version: int = 1
    updated_at: float = Field(default_factory=now)
    deployments: dict[str, Deployment] = Field(default_factory=dict)
    managed_nodes: list[ManagedNode] = Field(default_factory=list)
    events: list[Event] = Field(default_factory=list)

    def add_event(self, message: str, level: str = "info", deployment: str | None = None) -> None:
        self.events.append(Event(level=level, message=message, deployment=deployment))  # type: ignore[arg-type]
        if len(self.events) > 500:
            self.events = self.events[-500:]

    def running_models(self) -> dict[str, Deployment]:
        """Model aliases (slug and HF model id) -> deployment, for running LLMs."""
        table: dict[str, Deployment] = {}
        for dep in self.deployments.values():
            if dep.state != DeploymentState.RUNNING:
                continue
            table[dep.slug] = dep
            table.setdefault(dep.spec.model, dep)
        return table


# ---- API payloads --------------------------------------------------------


class CreateDeploymentRequest(DeploymentSpec):
    start: bool = True


class ChangeModelRequest(BaseModel):
    model: str
    extra_args: str | None = None
    vram: str | None = None
    hf_token_secret: str | None = None


class NewNodeRequest(BaseModel):
    instance_type: str
    environment: str = "dev"
    disk_size: int = Field(default=100, ge=MIN_DISK_SIZE_GB)
    num_instances: int = Field(default=1, ge=1, le=4)


class ConfirmNodeRequest(BaseModel):
    confirm_name: str
