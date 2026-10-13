"""Runtime creation and inspection of the per-LLM vLLM services.

The admin app builds a ``datatailr.Service`` for each deployment at runtime
("build-then-call", like ``agent_deployment``). Because the entrypoint is
imported from the shipped ``llm_platform`` package, Datatailr packages this
very package into the vLLM image.
"""

from __future__ import annotations

import inspect
import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
from datatailr import ACL, Group, Permission, Resources, Service, User
from datatailr.logging import DatatailrLogger
from datatailr.wrapper import resolve_cli_command_prefix

from llm_platform.common.dt_nodes import GPU_MEMORY_GIB, parse_size_gib
from llm_platform.common.schemas import Deployment
from llm_platform.vllm_service.main import main as vllm_main

log = DatatailrLogger(__name__).get_logger()

_VLLM_DIR = Path(__file__).resolve().parent.parent / "vllm_service"
VLLM_REQUIREMENTS = str(_VLLM_DIR / "requirements.txt")
VLLM_BUILD_SCRIPT = str(_VLLM_DIR / "build_script_pre.sh")
VLLM_PYTHON_VERSION = os.environ.get("LLM_VLLM_PYTHON_VERSION", "3.12")
VLLM_SPEC = os.environ.get("LLM_VLLM_SPEC", "")  # e.g. "vllm==0.31.0"; empty = latest
# vLLM is installed at first start (see vllm_service/main.py). To persist it and the
# model weights on the node's /mnt disk the service must run as root, which needs
# `dt user modify root --users <admin>` first; then set LLM_SERVICE_RUN_AS=root on
# the admin app. By default the service runs as the deploying user and re-installs
# on every restart.
VLLM_RUN_AS = os.environ.get("LLM_SERVICE_RUN_AS", "")
ACCESS_GROUP = os.environ.get("LLM_ACCESS_GROUP", "dtusers")


def _acl(owner: User) -> ACL:
    everyone = Group(ACCESS_GROUP)
    return ACL(
        {
            Permission.READ: [owner, everyone],
            Permission.WRITE: [owner],
            Permission.OPERATE: [owner],
            Permission.ACCESS: [owner, everyone],
            Permission.PROMOTE: [owner],
        }
    )


def _gpu_memory_utilization(spec) -> str | None:
    """vLLM ``--gpu-memory-utilization`` so the model stays within the requested vram."""
    total = GPU_MEMORY_GIB.get(spec.instance_type or "")
    if not total or not spec.vram:
        return None
    try:
        vram_gib = parse_size_gib(spec.vram)
    except ValueError:
        return None
    frac = min(0.95, max(0.1, vram_gib / total))
    return f"{frac:.2f}"


def _resources_support_vram() -> bool:
    try:
        return "vram" in inspect.signature(Resources.__init__).parameters or "vram" in getattr(
            Resources, "__dataclass_fields__", {}
        )
    except (TypeError, ValueError):
        return False


class _GpuService(Service):
    """Service that requests GPU memory on SDKs predating ``Resources(vram=...)``.

    Newer SDKs put ``"vram": "<size>"`` into the job JSON themselves; this
    subclass does the same when running on an older SDK so the scheduler still
    places the job on a GPU node.
    """

    _vram: str | None = None

    def to_dict(self):  # type: ignore[override]
        job_dict = super().to_dict()
        if self._vram and "vram" not in job_dict:
            job_dict["vram"] = self._vram
        return job_dict


def _build_resources(spec) -> tuple[Resources, str | None]:
    """Return (Resources, vram-to-inject-manually-or-None)."""
    if not spec.vram:
        return Resources(memory=spec.memory, cpu=spec.cpu), None
    if _resources_support_vram():
        return Resources(memory=spec.memory, cpu=spec.cpu, vram=spec.vram), None
    return Resources(memory=spec.memory, cpu=spec.cpu), spec.vram


def build_service(dep: Deployment) -> Service:
    """Create a fresh Service definition for the deployment's current spec."""
    spec = dep.spec
    owner = User.signed_user()
    resources, manual_vram = _build_resources(spec)
    env_vars: dict[str, str] = {
        "LLM_MODEL": spec.model,
        "LLM_SERVED_NAME": dep.slug,
        "LLM_EXTRA_ARGS": spec.extra_args or "",
        "LLM_PLATFORM_SLUG": dep.slug,
    }
    if spec.hf_token_secret:
        env_vars["HF_TOKEN_SECRET"] = spec.hf_token_secret
    if VLLM_SPEC:
        env_vars["LLM_VLLM_SPEC"] = VLLM_SPEC
    mem_util = _gpu_memory_utilization(spec)
    if mem_util:
        env_vars["LLM_GPU_MEM_UTIL"] = mem_util
    service = _GpuService(
        name=dep.service_name,
        entrypoint=vllm_main,
        environment=spec.environment,
        run_as=User(VLLM_RUN_AS) if VLLM_RUN_AS and VLLM_RUN_AS != owner.name else owner,
        resources=resources,
        python_version=VLLM_PYTHON_VERSION,
        python_requirements=VLLM_REQUIREMENTS,
        build_script_pre=_build_script(dep.build_nonce),
        env_vars=env_vars,
        acl=_acl(owner),
    )
    service._vram = manual_vram
    return service


def _build_script(nonce: int) -> str:
    """Build script content, with a nonce comment when a rebuild must be forced.

    The scheduler skips the image build when the new version's image definition
    (python version, requirements, build scripts, bundle) equals the previous
    one, even if that previous build failed. Changing a comment is enough to
    force a rebuild.
    """
    script = Path(VLLM_BUILD_SCRIPT).read_text()
    if nonce:
        script = script.rstrip("\n") + f"\n# rebuild-token: {nonce}\n"
    return script


def open_service(dep: Deployment) -> Service:
    """Reopen the latest saved version of the deployment's service."""
    return Service(name=dep.service_name, entrypoint=vllm_main, environment=dep.spec.environment, get_existing=True)


def job_definition(dep: Deployment) -> dict[str, Any] | None:
    """Latest job definition row (state: created|ready, last_build_succeeded...)."""
    rows = Service.ls(environment=dep.spec.environment, filter_by=f"name = {dep.internal_name}")
    if not rows:
        return None
    rows.sort(key=lambda r: r.get("version", 0))
    return rows[-1]


def latest_run(dep: Deployment) -> dict[str, Any] | None:
    try:
        service = open_service(dep)
    except Exception:  # noqa: BLE001
        return None
    runs = service.runs(refresh=True) or []
    if not runs:
        return None

    def key(run: dict[str, Any]) -> float:
        value = run.get("submit_time") or run.get("start_time") or 0
        return value.timestamp() if hasattr(value, "timestamp") else float(value or 0)

    runs.sort(key=key)
    return runs[-1]


def read_logs(dep: Deployment, run_id: int, stderr: bool = False, lines: int = 400) -> str:
    service = open_service(dep)
    return service.logs(run_id=run_id, stderr=stderr, lines=lines) or ""


def build_logs(dep: Deployment, stream: str = "stdout", tail_lines: int = 60) -> str:
    """Tail of the image build log (``dt job get <name> --build-logs stdout|stderr``)."""
    prefix = resolve_cli_command_prefix()
    if prefix is None:
        return ""
    cmd = prefix + ["job", "get", dep.internal_name, "--environment", dep.spec.environment, "--build-logs", stream]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return f"<build log unavailable: {exc}>"
    text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    return "\n".join(lines[-tail_lines:])


def build_failure_summary(dep: Deployment) -> str:
    """Short human-readable reason for a failed build, extracted from the build log."""
    text = build_logs(dep, "stdout", tail_lines=400) or build_logs(dep, "stderr", tail_lines=400)
    errors = [ln for ln in text.splitlines() if "ERROR" in ln or "error:" in ln.lower()]
    if errors:
        return errors[-1].strip()[:500]
    tail = text.splitlines()[-3:]
    return " | ".join(ln.strip() for ln in tail)[:500] if tail else "see the build logs in the Datatailr UI"


def health_url(dep: Deployment) -> str:
    return f"{dep.internal_url}/job/{dep.spec.environment}/{dep.internal_name}/health"


def check_health(dep: Deployment, timeout: float = 5.0) -> bool:
    try:
        response = httpx.get(health_url(dep), timeout=timeout)
        return response.status_code == 200
    except httpx.HTTPError:
        return False
