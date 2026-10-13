"""Datatailr service entrypoint that runs ``vllm serve``.

The admin app creates one Service per LLM from this module, passing the model
and serving options as environment variables:

- ``LLM_MODEL``         Hugging Face model id (required)
- ``LLM_SERVED_NAME``   name clients use in the ``model`` field (the deployment slug)
- ``LLM_EXTRA_ARGS``    extra ``vllm serve`` arguments (shell-split)
- ``LLM_GPU_MEM_UTIL``  fraction of GPU memory vLLM may use (derived from the
                        deployment's ``vram`` and the node's GPU size)
- ``LLM_VLLM_SPEC``     pip requirement for vLLM (default ``vllm``; e.g. ``vllm==0.31.0``)
- ``HF_TOKEN_SECRET``   optional Datatailr secret name holding a Hugging Face token

Why vLLM is installed at start-up rather than baked into the image
------------------------------------------------------------------
vLLM plus the CUDA wheels it depends on is ~10 GB. That is larger than the
cluster's image registry accepts in one layer (the push fails), so the service
image stays small and vLLM is installed on first start into a virtualenv.

Storage: ``/mnt`` is node-local storage that survives container restarts, but
it is only writable when the service runs as root (``LLM_SERVICE_RUN_AS=root``
on the admin app, which requires ``dt user modify root --users <admin>``).
Otherwise the install goes to container-local storage and is redone on every
restart. Model weights are cached in the same place. Either way the data lives
on the node's disk, which is why GPU nodes are created with at least 50 GB.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

PERSISTENT_ROOT = Path(os.environ.get("LLM_PLATFORM_MNT", "/mnt/llm_platform"))
EPHEMERAL_ROOT = Path("/tmp/llm_platform")
DEFAULT_VLLM_SPEC = "vllm"


def _log(msg: str) -> None:
    print(f"[llm_platform] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def _writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test"
        probe.write_text("ok")
        probe.unlink()
        return True
    except OSError:
        return False


def storage_root() -> Path:
    """Node-persistent storage if writable, else container-local storage."""
    if _writable(PERSISTENT_ROOT):
        return PERSISTENT_ROOT
    _log(
        f"{PERSISTENT_ROOT} is not writable (service runs as {os.environ.get('USER') or os.getuid()}); "
        f"using container-local {EPHEMERAL_ROOT} - vLLM and model weights will be fetched again after a restart. "
        "Run the service as root (LLM_SERVICE_RUN_AS=root) to persist them on the node."
    )
    EPHEMERAL_ROOT.mkdir(parents=True, exist_ok=True)
    return EPHEMERAL_ROOT


def _load_hf_token() -> None:
    secret_name = os.environ.get("HF_TOKEN_SECRET", "").strip()
    if not secret_name or os.environ.get("HF_TOKEN"):
        return
    try:
        from datatailr import Secrets

        token = Secrets().get(secret_name)
        if token:
            os.environ["HF_TOKEN"] = token
            os.environ["HUGGING_FACE_HUB_TOKEN"] = token
    except Exception as exc:  # noqa: BLE001
        print(f"[llm_platform] could not read secret {secret_name!r}: {exc}", file=sys.stderr, flush=True)


# ---- runtime installation ----------------------------------------------------------


def _uv_command() -> list[str]:
    exe = shutil.which("uv")
    if exe:
        return [exe]
    if importlib.util.find_spec("uv") is not None:
        return [sys.executable, "-m", "uv"]
    raise RuntimeError("uv is not installed in the service image (add 'uv' to requirements.txt)")


def _run(cmd: list[str], **env: str) -> None:
    _log("$ " + " ".join(shlex.quote(c) for c in cmd))
    full_env = {**os.environ, **env}
    subprocess.run(cmd, check=True, env=full_env)


def ensure_vllm_python(root: Path, spec: str = DEFAULT_VLLM_SPEC) -> str:
    """Return a Python interpreter that has vLLM importable, installing it if needed."""
    if importlib.util.find_spec("vllm") is not None:
        _log("vLLM is available in the service image")
        return sys.executable

    venv_root = root / "venvs"
    uv_cache = root / "uv_cache"
    pyver = f"py{sys.version_info.major}.{sys.version_info.minor}"
    key = hashlib.sha1(f"{spec}|{pyver}".encode()).hexdigest()[:10]
    venv = venv_root / f"vllm-{pyver}-{key}"
    python = venv / "bin" / "python"
    ready = venv / ".ready"

    venv_root.mkdir(parents=True, exist_ok=True)
    uv_cache.mkdir(parents=True, exist_ok=True)
    lock_path = venv_root / f".{venv.name}.lock"
    with open(lock_path, "w") as lock:
        _log(f"waiting for install lock {lock_path}")
        fcntl.flock(lock, fcntl.LOCK_EX)  # another LLM service on this node may be installing
        if ready.exists() and python.exists():
            _log(f"reusing cached vLLM environment {venv}")
            return str(python)

        tmp = venv.with_name(venv.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(venv, ignore_errors=True)
        uv = _uv_command()
        started = time.time()
        _log(f"installing {spec!r} into {venv} (first start on this node; this takes a few minutes)")
        _run([*uv, "venv", "--python", sys.executable, str(tmp)], UV_CACHE_DIR=str(uv_cache))
        _run(
            [*uv, "pip", "install", "--python", str(tmp / "bin" / "python"), spec],
            UV_CACHE_DIR=str(uv_cache),
            UV_LINK_MODE="hardlink",
        )
        (tmp / ".ready").write_text(spec + "\n")
        tmp.rename(venv)
        _log(f"vLLM installed in {time.time() - started:.0f}s")
        return str(python)


# ---- vllm command -------------------------------------------------------------------


def build_command(python: str, port: int, hf_cache: Path) -> list[str]:
    model = os.environ.get("LLM_MODEL", "").strip()
    if not model:
        raise RuntimeError("LLM_MODEL is not set")
    served_name = os.environ.get("LLM_SERVED_NAME", "").strip() or model
    cmd = [
        python,
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--model",
        model,
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--served-model-name",
        *dict.fromkeys([served_name, model]),  # both aliases, de-duplicated
        "--download-dir",
        str(hf_cache),
    ]
    extra = shlex.split(os.environ.get("LLM_EXTRA_ARGS", "").strip() or "")
    mem_util = os.environ.get("LLM_GPU_MEM_UTIL", "").strip()
    if mem_util and "--gpu-memory-utilization" not in extra:
        cmd.extend(["--gpu-memory-utilization", mem_util])
    cmd.extend(extra)
    return cmd


def main(port):
    root = storage_root()
    hf_cache = root / "hf_cache"
    hf_cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HF_HOME", str(hf_cache))
    os.environ.setdefault("HF_HUB_CACHE", str(hf_cache))
    os.environ.setdefault("VLLM_NO_USAGE_STATS", "1")
    _load_hf_token()
    python = ensure_vllm_python(root, os.environ.get("LLM_VLLM_SPEC", "").strip() or DEFAULT_VLLM_SPEC)
    cmd = build_command(python, int(port), hf_cache)
    _log("starting: " + " ".join(shlex.quote(c) for c in cmd))
    # Replace this process so the platform's restart policy applies to vLLM itself.
    os.execv(cmd[0], cmd)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", "8000")))
