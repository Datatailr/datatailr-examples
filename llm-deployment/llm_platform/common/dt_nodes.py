"""Compute node helpers built on ``datatailr.wrapper.dt__Node``.

GPU nodes are not managed by the autoscaler: they must be created explicitly.
This module wraps the ``dt node`` commands the orchestrator and the admin API
need, and ships the node template (``gpu_node_template.json``) used for
``dt node new``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from importlib.resources import files
from typing import Any

from datatailr.logging import DatatailrLogger
from datatailr.wrapper import dt__Node, resolve_cli_command_prefix

from llm_platform.common.schemas import MIN_DISK_SIZE_GB

log = DatatailrLogger(__name__).get_logger()

_node_client = dt__Node()

# Total GPU memory (GiB) of known GPU instance types. Used to decide whether an
# existing node can host one more LLM. Unknown types are treated as "one LLM per node".
GPU_MEMORY_GIB: dict[str, int] = {
    "Standard_NC4as_T4_v3": 16,
    "Standard_NC8as_T4_v3": 16,
    "Standard_NC16as_T4_v3": 16,
    "Standard_NC64as_T4_v3": 64,
    "Standard_NC24ads_A100_v4": 80,
    "Standard_NC48ads_A100_v4": 160,
    "Standard_NC96ads_A100_v4": 320,
    "Standard_NC40ads_H100_v5": 94,
    "Standard_NC80adis_H100_v5": 188,
    "Standard_NV36ads_A10_v5": 24,
    "Standard_NV72ads_A10_v5": 48,
    "g4dn.xlarge": 16,
    "g4dn.2xlarge": 16,
    "g4dn.12xlarge": 64,
    "g5.xlarge": 24,
    "g5.2xlarge": 24,
    "g5.12xlarge": 96,
    "p3.2xlarge": 16,
    "p4d.24xlarge": 320,
}


def parse_size_gib(value: str) -> float:
    """Parse a Datatailr size string (``"512m"``, ``"16g"``, bytes) into GiB."""
    value = str(value).strip().lower()
    units = {"k": 1 / 1024**2, "m": 1 / 1024, "g": 1, "t": 1024}
    for suffix, factor in units.items():
        if value.endswith(suffix):
            return float(value[:-1]) * factor
    return float(value) / 1024**3


def _run_dt(args: list[str], timeout: float = 60.0) -> Any:
    """Run a ``dt`` command and parse its JSON output.

    Used for subcommands the ``dt__Node`` wrapper cannot run as needed:
    ``instance-types`` and ``new`` declare no ``json`` option (they already print
    JSON; the in-cluster CLI rejects ``--json`` with a usage error) and ``new``
    needs a longer timeout than the wrapper's fixed 30 seconds.
    """
    prefix = resolve_cli_command_prefix()
    if prefix is None:
        raise RuntimeError("Datatailr CLI ('dt') is not available")
    result = subprocess.run(prefix + args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        raise RuntimeError(_strip_ansi(result.stderr.strip() or result.stdout.strip()) or f"{' '.join(args)} failed")
    text = result.stdout.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def load_node_template() -> list[dict[str, Any]]:
    raw = files("llm_platform.common").joinpath("gpu_node_template.json").read_text(encoding="utf-8")
    return json.loads(raw)


def build_node_request(instance_type: str, environment: str, disk_size: int, num_instances: int = 1) -> list[dict[str, Any]]:
    template = load_node_template()
    entry = dict(template[0]) if template else {}
    entry.update(
        {
            "type": instance_type,
            "node_class": "gpu",
            "environment": environment,
            "num_instances": int(num_instances),
            "disk_size": max(int(disk_size), MIN_DISK_SIZE_GB),
        }
    )
    return [entry]


# ---- reads -----------------------------------------------------------------


def list_nodes() -> list[dict[str, Any]]:
    result = _node_client.ls(json=True)
    return result if isinstance(result, list) else []


_instance_type_cache: tuple[float, list[dict[str, Any]]] | None = None


def instance_types(refresh: bool = False) -> list[dict[str, Any]]:
    global _instance_type_cache
    if not refresh and _instance_type_cache and time.time() - _instance_type_cache[0] < 3600:
        return _instance_type_cache[1]
    result = _run_dt(["node", "instance-types"])
    types = result if isinstance(result, list) else []
    _instance_type_cache = (time.time(), types)
    return types


def gpu_instance_types() -> list[dict[str, Any]]:
    out = []
    for item in instance_types():
        if item.get("has_gpu") or "gpu" in str(item.get("node_class", "")).lower():
            enriched = dict(item)
            enriched["gpu_memory_gib"] = GPU_MEMORY_GIB.get(item.get("name", ""))
            out.append(enriched)
    out.sort(key=lambda t: (t.get("price") or 0))
    return out


def is_gpu_node(node: dict[str, Any], gpu_type_names: set[str] | None = None) -> bool:
    if str(node.get("node_class", "")).lower() == "gpu":
        return True
    if gpu_type_names and node.get("type") in gpu_type_names:
        return True
    return False


def node_is_ready(node: dict[str, Any]) -> bool:
    """A node accepts jobs once the cloud reports it running and the node daemon registered."""
    status = str(node.get("status", "")).lower()
    cpu = node.get("cpu") or {}
    return status == "running" and float(cpu.get("total_cpus") or 0) > 0


def find_node(nodes: list[dict[str, Any]], node_id_or_name: str) -> dict[str, Any] | None:
    for node in nodes:
        if node_id_or_name in {node.get("id"), node.get("instance_id"), node.get("name")}:
            return node
    return None


# ---- mutations ---------------------------------------------------------------


def new_nodes(instance_type: str, environment: str, disk_size: int, num_instances: int = 1, timeout: float = 180.0) -> Any:
    """Provision GPU node(s). Uses the CLI directly to allow a longer timeout than the wrapper's 30s."""
    request = build_node_request(instance_type, environment, disk_size, num_instances)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(request, handle)
        path = handle.name
    try:
        log.info("Provisioning node: %s", json.dumps(request))
        return _run_dt(["node", "new", f"file://{path}"], timeout=timeout)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def start_node(node_id: str) -> Any:
    return _node_client.start(node_id)


def stop_node(node_id: str) -> Any:
    return _node_client.stop(node_id)


def terminate_node(node_id: str) -> Any:
    return _node_client.terminate(node_id)
