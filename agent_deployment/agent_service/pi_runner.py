"""Wrapper around the `pi` coding-agent CLI.

The service drives pi non-interactively via `pi --mode json`, which streams all
session events as newline-delimited JSON objects to stdout (see
https://pi.dev/docs/latest -> JSON event stream mode). We parse those events to
recover the session id, the final assistant reply and token/cost usage.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Optional


# Where pi stores its config and sessions inside the container. We pin these so
# the service always knows where to read history from and can persist it.
PI_AGENT_DIR = os.environ.get("PI_CODING_AGENT_DIR", os.path.expanduser("~/.pi/agent"))
PI_SESSION_DIR = os.environ.get(
    "PI_CODING_AGENT_SESSION_DIR", os.path.join(PI_AGENT_DIR, "sessions")
)
# Directory the agent operates in (full read/write/edit/bash tools act here).
PI_WORKSPACE_DIR = os.environ.get("PI_WORKSPACE_DIR", "/tmp/agent_workspace")

# Hard ceiling so a runaway agent turn cannot block the worker forever.
PI_TIMEOUT_SECONDS = int(os.environ.get("PI_TIMEOUT_SECONDS", "600"))


@dataclass
class PiResult:
    """Outcome of a single `pi` invocation."""

    session_id: Optional[str]
    reply: str
    usage: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    raw_stderr: str = ""


def _pi_env() -> dict[str, str]:
    """Environment for the pi subprocess.

    OPENAI_API_KEY is expected to already be set in the process environment by
    the service at startup (loaded from Datatailr Secrets). HOME is pinned so
    that `~/.pi` resolves to the same place the service reads from.
    """
    env = dict(os.environ)
    env.setdefault("HOME", os.path.expanduser("~"))
    # Avoid version checks / telemetry network calls from a headless server.
    env.setdefault("PI_OFFLINE", "1")
    env.setdefault("PI_SKIP_VERSION_CHECK", "1")
    return env


def _extract_text(content: Any) -> str:
    """Join the text blocks of an assistant message content array."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "".join(parts)


def run_pi(
    message: str,
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    session_name: Optional[str] = None,
) -> PiResult:
    """Run pi once with `message` and return the parsed result.

    If `session_id` is given, the conversation continues that session;
    otherwise pi creates a new one and we capture the new id from the session
    header event.
    """
    os.makedirs(PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(PI_SESSION_DIR, exist_ok=True)

    argv: list[str] = [
        "pi",
        "--mode",
        "json",
        "--session-dir",
        PI_SESSION_DIR,
        # Trust the (empty) workspace folder so non-interactive runs don't skip
        # resources or stall waiting on a prompt.
        "-a",
    ]
    if model:
        argv += ["--model", model]
    if session_id:
        argv += ["--session", session_id]
    if session_name:
        argv += ["--name", session_name]
    # The prompt is the trailing positional argument.
    argv.append(message)

    proc = subprocess.run(
        argv,
        cwd=PI_WORKSPACE_DIR,
        env=_pi_env(),
        capture_output=True,
        text=True,
        timeout=PI_TIMEOUT_SECONDS,
    )

    events: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            # Non-JSON noise (rare) is ignored; the reply is reconstructed from
            # the structured events below.
            continue

    parsed_session_id: Optional[str] = session_id
    last_reply = ""
    usage_total = {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": 0.0,
    }

    for event in events:
        etype = event.get("type")
        if etype == "session" and event.get("id"):
            parsed_session_id = event["id"]
        elif etype in ("message_end", "turn_end"):
            message_obj = event.get("message") or {}
            if message_obj.get("role") == "assistant":
                text = _extract_text(message_obj.get("content"))
                if text.strip():
                    last_reply = text
                usage = message_obj.get("usage") or {}
                for key in ("input", "output", "cacheRead", "cacheWrite", "totalTokens"):
                    usage_total[key] += int(usage.get(key, 0) or 0)
                cost = usage.get("cost") or {}
                usage_total["cost"] += float(cost.get("total", 0) or 0)

    if not last_reply and proc.returncode != 0:
        last_reply = (
            "The agent failed to produce a response. "
            f"(exit code {proc.returncode})"
        )

    return PiResult(
        session_id=parsed_session_id,
        reply=last_reply,
        usage=usage_total,
        events=events,
        raw_stderr=proc.stderr,
    )
