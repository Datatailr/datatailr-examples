"""Shared git bootstrap for every agent (main and sub) -- see specification §5.

On startup each agent container clones the shared repository over SSH using a
deploy key retrieved from the Datatailr Secrets Manager, pinning host-key
verification via a KV-stored ``known_hosts`` entry (never disabling checking).

If the ``agent_git_known_hosts`` KV is not configured, the bootstrap falls back
to fetching the git host's public keys once via ``ssh-keyscan`` (trust-on-first-
use) so cloning works out of the box with only the SSH key secret. Pinning the
host key explicitly via KV is still recommended and takes precedence.

Safety rules (kv-and-secrets):
  - the SSH key and any tokens are **never** logged, printed, or echoed;
  - ``StrictHostKeyChecking=no`` is disallowed -- host keys are either pinned
    via KV or captured (and then verified) via ``ssh-keyscan``.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from typing import Optional

log = logging.getLogger("agent_app.git_bootstrap")

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
# Host-key resolution (for SSH host verification without disabling it)
# --------------------------------------------------------------------------- #
# scp-like remote: ``[user@]host:path`` (github's ``git@github.com:org/repo.git``).
_SCP_LIKE_RE = re.compile(r"^(?:[^@/]+@)?(?P<host>[^:/]+):(?!/)")


def parse_git_host(url: Optional[str]) -> tuple[Optional[str], int]:
    """Extract ``(host, port)`` from a git remote URL for host-key scanning.

    Handles the scp-like form (``git@github.com:org/repo.git``) and the
    ``ssh://[user@]host[:port]/path`` form. Returns ``(None, 22)`` when the host
    cannot be determined (e.g. an ``https://`` URL, which needs no host key).
    """
    if not url:
        return None, 22
    if url.startswith("ssh://"):
        authority = url[len("ssh://") :].split("/", 1)[0]
        if "@" in authority:
            authority = authority.split("@", 1)[1]
        host, sep, port = authority.partition(":")
        if sep and port.isdigit():
            return host or None, int(port)
        return host or None, 22
    match = _SCP_LIKE_RE.match(url)
    if match:
        return match.group("host"), 22
    return None, 22


def _scan_host_keys(host: str, port: int = 22) -> Optional[str]:
    """Fetch a host's public SSH keys via ``ssh-keyscan`` (trust-on-first-use).

    Returns the ``known_hosts`` lines, or ``None`` if the tool is unavailable or
    the scan fails. Host keys are public information; nothing sensitive is
    handled here.
    """
    if not shutil.which("ssh-keyscan"):
        return None
    try:
        proc = subprocess.run(
            ["ssh-keyscan", "-T", "10", "-p", str(port), host],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:  # noqa: BLE001
        return None
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout
    return None


# --------------------------------------------------------------------------- #
# SSH + identity configuration
# --------------------------------------------------------------------------- #
def _configure_ssh(host: Optional[str] = None, port: int = 22) -> str:
    """Write the deploy key + a known_hosts file, and set ``GIT_SSH_COMMAND``.

    Host-key verification stays enabled (``StrictHostKeyChecking=yes``). The
    known_hosts is sourced, in order of preference, from the ``agent_git_known_hosts``
    KV (explicit pin), or -- if that is unset -- from a one-time ``ssh-keyscan``
    of the git host (trust-on-first-use). ``GIT_SSH_COMMAND`` is exported so any
    subsequent ``git`` invocation (including ones the agent runs itself) reuses
    the same key and known_hosts. Returns the key path; key material is written
    with 0600 perms and is never logged.
    """
    key = _secret(SSH_KEY_SECRET)
    if not key:
        raise GitBootstrapError(
            f"SSH deploy key secret '{SSH_KEY_SECRET}' is not configured. "
            "Create it in the Datatailr Secrets Manager UI."
        )

    if not key.startswith("-----BEGIN OPENSSH PRIVATE KEY-----"):
        key = f"-----BEGIN OPENSSH PRIVATE KEY-----\n{key}\n-----END OPENSSH PRIVATE KEY-----"
    ssh_dir = os.path.expanduser("~/.ssh")
    os.makedirs(ssh_dir, mode=0o700, exist_ok=True)

    key_path = os.path.join(ssh_dir, "id_ed25519")
    with open(key_path, "w", encoding="utf-8") as fh:
        fh.write(key if key.endswith("\n") else key + "\n")
    os.chmod(key_path, 0o600)

    known_hosts_path = os.path.join(ssh_dir, "known_hosts")
    known_hosts = _kv(KNOWN_HOSTS_KV)
    if not known_hosts and host:
        # No explicit pin: capture the host key once so verification can stay on
        # rather than failing closed on an unknown host.
        known_hosts = _scan_host_keys(host, port)
        if known_hosts:
            log.info(
                "captured host key for %s via ssh-keyscan (set the '%s' KV to "
                "pin it explicitly)",
                host,
                KNOWN_HOSTS_KV,
            )
        else:
            log.warning(
                "no '%s' KV configured and ssh-keyscan of %s failed; git will "
                "fail host-key verification. Set the KV to the host's known_hosts.",
                KNOWN_HOSTS_KV,
                host,
            )

    ssh_opts = f"ssh -i {key_path} -o IdentitiesOnly=yes"
    if known_hosts:
        with open(known_hosts_path, "w", encoding="utf-8") as fh:
            fh.write(known_hosts if known_hosts.endswith("\n") else known_hosts + "\n")
        os.chmod(known_hosts_path, 0o644)
        ssh_opts += (
            f" -o UserKnownHostsFile={known_hosts_path} -o StrictHostKeyChecking=yes"
        )
    # If we still have no known_hosts we intentionally do NOT fall back to
    # StrictHostKeyChecking=no; git fails closed on an unknown host, which is the
    # safe default per §14.

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

    host, port = parse_git_host(url)
    _configure_ssh(host, port)

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
        log.info(
            "git access not configured (need '%s' secret + '%s' KV); serving pi "
            "against an empty workspace",
            SSH_KEY_SECRET,
            REPO_URL_KV,
        )
        return None
    try:
        return bootstrap_git(workdir)
    except GitBootstrapError as exc:
        # Surface the reason (sanitized -- git never echoes key material) so a
        # failed clone is diagnosable instead of silently yielding an empty dir.
        log.warning("git workspace bootstrap failed: %s", exc)
        return None
