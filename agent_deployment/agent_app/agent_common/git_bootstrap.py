"""Shared git bootstrap for every agent (main and sub) -- see specification §5.

On startup each agent container clones the shared repository over SSH using a
deploy key retrieved from the Datatailr Secrets Manager, pinning host-key
verification via a KV-stored ``known_hosts`` entry (never disabling checking).

Safety rules (kv-and-secrets):
  - the SSH key and any tokens are **never** logged, printed, or echoed;
  - ``StrictHostKeyChecking=no`` is disallowed -- host keys are pinned via KV.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

# KV / Secret key names (overridable via env for testing).
SSH_KEY_SECRET = os.environ.get("AGENT_GIT_SSH_KEY_SECRET", "agent_git_ssh_key")
GIT_TOKEN_SECRET = os.environ.get("AGENT_GIT_TOKEN_SECRET", "agent_git_token")
REPO_URL_KV = os.environ.get("AGENT_GIT_REPO_URL_KV", "agent_git_repo_url")
KNOWN_HOSTS_KV = os.environ.get("AGENT_GIT_KNOWN_HOSTS_KV", "agent_git_known_hosts")
DEFAULT_BRANCH_KV = os.environ.get("AGENT_GIT_DEFAULT_BRANCH_KV", "agent_git_default_branch")

# Git identity used for agent-authored commits.
GIT_AUTHOR_NAME = os.environ.get("AGENT_GIT_AUTHOR_NAME", "SWE Agent")
GIT_AUTHOR_EMAIL = os.environ.get("AGENT_GIT_AUTHOR_EMAIL", "swe-agent@datatailr.local")


class GitBootstrapError(RuntimeError):
    """Raised when git access cannot be configured (missing key/config)."""


# --------------------------------------------------------------------------- #
# Config access (lazy Datatailr imports; tolerant off-platform)
# --------------------------------------------------------------------------- #
def _secret(key: str) -> Optional[str]:
    try:
        from datatailr import Secrets

        return Secrets().get(key)
    except Exception:
        return None


def _kv(key: str) -> Optional[str]:
    try:
        from datatailr import KV

        value = KV().get(key)
        if isinstance(value, str):
            return value
        if value is not None:
            return str(value)
    except Exception:
        return None
    return None


def git_config_available() -> bool:
    """True if the SSH key and repo URL are both present (so cloning can work).

    Lets callers (e.g. the main App) skip git bootstrap gracefully in
    environments where the secrets/KV are not configured, rather than crash.
    """
    return bool(_secret(SSH_KEY_SECRET)) and bool(_kv(REPO_URL_KV))


def repo_url() -> Optional[str]:
    return _kv(REPO_URL_KV)


def default_branch() -> str:
    return _kv(DEFAULT_BRANCH_KV) or "main"


def git_token() -> Optional[str]:
    """The git-host API token used to open PRs. Never log the return value."""
    return _secret(GIT_TOKEN_SECRET)


# --------------------------------------------------------------------------- #
# SSH + identity configuration
# --------------------------------------------------------------------------- #
def _configure_ssh() -> str:
    """Write the deploy key and pinned known_hosts, set ``GIT_SSH_COMMAND``.

    Returns the key path. The key material is written with 0600 perms and is
    never logged.
    """
    key = _secret(SSH_KEY_SECRET)
    if not key:
        raise GitBootstrapError(
            f"SSH deploy key secret '{SSH_KEY_SECRET}' is not configured. "
            "Create it in the Datatailr Secrets Manager UI."
        )

    ssh_dir = os.path.expanduser("~/.ssh")
    os.makedirs(ssh_dir, mode=0o700, exist_ok=True)

    key_path = os.path.join(ssh_dir, "id_ed25519")
    with open(key_path, "w", encoding="utf-8") as fh:
        fh.write(key if key.endswith("\n") else key + "\n")
    os.chmod(key_path, 0o600)

    known_hosts = _kv(KNOWN_HOSTS_KV)
    ssh_opts = f"ssh -i {key_path} -o IdentitiesOnly=yes"
    if known_hosts:
        known_hosts_path = os.path.join(ssh_dir, "known_hosts")
        with open(known_hosts_path, "w", encoding="utf-8") as fh:
            fh.write(known_hosts if known_hosts.endswith("\n") else known_hosts + "\n")
        os.chmod(known_hosts_path, 0o644)
        ssh_opts += f" -o UserKnownHostsFile={known_hosts_path} -o StrictHostKeyChecking=yes"
    # If no pinned known_hosts is configured we intentionally do NOT fall back
    # to StrictHostKeyChecking=no; git will fail closed on an unknown host,
    # which is the safe default per §14.

    os.environ["GIT_SSH_COMMAND"] = ssh_opts
    return key_path


def _configure_identity(workdir: str) -> None:
    for name, value in (
        ("user.name", GIT_AUTHOR_NAME),
        ("user.email", GIT_AUTHOR_EMAIL),
    ):
        try:
            subprocess.run(
                ["git", "-C", workdir, "config", name, value],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            pass


def _run_git(args: list[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    """Run a git command, raising ``GitBootstrapError`` with sanitized output.

    stderr is included in the error, but git never echoes the key material, so
    this is safe. We never place the token/key on the command line.
    """
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=os.environ,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise GitBootstrapError(
            f"git {' '.join(args)} failed (exit {proc.returncode}): "
            f"{proc.stderr.strip()[:500]}"
        )
    return proc


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def bootstrap_git(
    workdir: str,
    *,
    depth: int = 50,
    base_branch: Optional[str] = None,
    work_branch: Optional[str] = None,
) -> str:
    """Clone the shared repo into ``workdir`` and configure identity.

    - ``base_branch``: branch to check out after clone (defaults to the KV
      ``agent_git_default_branch``). The clone is shallow (``--depth``).
    - ``work_branch``: if given, create and switch to this new branch off
      ``base_branch`` (used by sub-agents, §10).

    Returns ``workdir``. Raises ``GitBootstrapError`` on any failure.
    """
    url = repo_url()
    if not url:
        raise GitBootstrapError(
            f"Repository URL KV '{REPO_URL_KV}' is not configured."
        )

    _configure_ssh()

    base = base_branch or default_branch()
    os.makedirs(os.path.dirname(workdir) or ".", exist_ok=True)

    already_cloned = os.path.isdir(os.path.join(workdir, ".git"))
    if not already_cloned:
        _run_git(
            [
                "clone",
                "--depth",
                str(depth),
                "--branch",
                base,
                url,
                workdir,
            ]
        )
    else:
        # Refresh an existing checkout so the main agent's workspace tracks the
        # latest base branch between uses.
        _run_git(["-C", workdir, "fetch", "--depth", str(depth), "origin", base])
        _run_git(["-C", workdir, "checkout", base])
        _run_git(["-C", workdir, "reset", "--hard", f"origin/{base}"])

    _configure_identity(workdir)

    if work_branch:
        _run_git(["-C", workdir, "checkout", "-B", work_branch])

    return workdir


def ensure_workspace_repo(workdir: str) -> Optional[str]:
    """Best-effort clone/refresh for the main agent's per-user workspace.

    Returns ``workdir`` on success, or ``None`` if git is not configured (so
    the App can keep serving pi without a repo in dev/unconfigured setups).
    """
    if not git_config_available():
        return None
    try:
        return bootstrap_git(workdir)
    except GitBootstrapError:
        return None
