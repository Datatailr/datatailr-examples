"""Run the `pi` CLI inside a pseudo-terminal (PTY).

This powers the interactive terminal UI: instead of driving pi in JSON mode and
re-rendering events, we attach pi to a real PTY and stream its raw terminal
bytes to the browser (xterm.js), forwarding keystrokes back. That gives the
genuine CLI experience -- colours, spinners, thinking, tool output, interactive
approval prompts and slash commands -- with no event bridging.

The app's WebSocket endpoint owns the asyncio bridging; this module only deals
with spawning, resizing and tearing down the PTY-backed process.
"""

from __future__ import annotations

import fcntl
import os
import pty
import signal
import struct
import subprocess
import termios
from typing import Optional

from agent_app import pi_runner


def _set_winsize(fd: int, rows: int, cols: int) -> None:
    try:
        winsize = struct.pack("HHHH", rows, cols, 0, 0)
        fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)
    except OSError:
        pass


def set_winsize(master_fd: int, rows: int, cols: int) -> None:
    """Resize the PTY (called when the browser terminal resizes)."""
    _set_winsize(master_fd, max(1, rows), max(1, cols))


def spawn(
    *,
    session_dir: str,
    workspace_dir: Optional[str] = None,
    model: Optional[str] = None,
    session_id: Optional[str] = None,
    cols: int = 80,
    rows: int = 24,
    extra_env: Optional[dict[str, str]] = None,
) -> tuple[subprocess.Popen, int]:
    """Start pi attached to a PTY and return (process, master_fd).

    `session_dir` isolates the user's sessions and `workspace_dir` isolates the
    directory the agent's file/bash tools operate in (defaults to the shared
    workspace). `session_id` (optional) resumes an existing conversation. The
    slave end is closed in the parent after the child inherits it, so reading
    EOF on the master reliably signals exit.
    """
    workspace_dir = workspace_dir or pi_runner.PI_WORKSPACE_DIR
    os.makedirs(workspace_dir, exist_ok=True)
    os.makedirs(session_dir, exist_ok=True)

    master_fd, slave_fd = pty.openpty()
    _set_winsize(master_fd, rows, cols)

    argv: list[str] = ["pi", "--session-dir", session_dir, "-a"]
    provider, model_id = pi_runner.split_model(model)
    if provider:
        argv += ["--provider", provider]
    if model_id:
        argv += ["--model", model_id]
    if session_id:
        argv += ["--session", session_id]

    env = pi_runner._pi_env()
    env["TERM"] = "xterm-256color"
    env["PI_CODING_AGENT_SESSION_DIR"] = session_dir
    env["FORCE_COLOR"] = "1"
    env["COLUMNS"] = str(cols)
    env["LINES"] = str(rows)
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items() if v is not None})

    proc = subprocess.Popen(
        argv,
        cwd=workspace_dir,
        env=env,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        # New session/process group so we can signal the whole tree on teardown.
        preexec_fn=os.setsid,
        close_fds=True,
    )
    os.close(slave_fd)
    return proc, master_fd


def read(master_fd: int, size: int = 65536) -> bytes:
    """Blocking read of available PTY output; b'' on EOF/closed."""
    try:
        return os.read(master_fd, size)
    except OSError:
        return b""


def write(master_fd: int, data: bytes) -> None:
    """Write keystrokes to the PTY, ignoring errors after the process exits."""
    try:
        os.write(master_fd, data)
    except OSError:
        pass


def terminate(proc: subprocess.Popen, master_fd: int) -> None:
    """Close the PTY and stop the process group (SIGTERM, then SIGKILL)."""
    try:
        os.close(master_fd)
    except OSError:
        pass
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, OSError):
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass
