"""Wrapper around the `pi` coding-agent CLI.

The app drives pi non-interactively via `pi --mode json` (for the HTTP API),
which streams all session events as newline-delimited JSON objects to stdout
(see https://pi.dev/docs/latest -> JSON event stream mode). We parse those
events to recover the session id, the final assistant reply and token/cost
usage. The interactive terminal uses `pty_runner` instead.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


# Where pi stores its config and sessions inside the container. We pin these so
# the app always knows where to read history from and can persist it.
PI_AGENT_DIR = os.environ.get("PI_CODING_AGENT_DIR", os.path.expanduser("~/.pi/agent"))
# Root for session storage; per-user sessions live in <root>/<user>/.
PI_SESSION_DIR = os.environ.get(
    "PI_CODING_AGENT_SESSION_DIR", os.path.join(PI_AGENT_DIR, "sessions")
)
# Global agent skills directory (~/.agents) that pi also discovers.
AGENTS_DIR = os.environ.get("AGENTS_DIR", os.path.expanduser("~/.agents"))
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
    the app at startup (loaded from Datatailr Secrets). HOME is pinned so that
    `~/.pi` resolves to the same place the app reads from.
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


def _build_argv(
    message: str,
    session_id: Optional[str],
    model: Optional[str],
    session_name: Optional[str],
    session_dir: str,
    thinking: Optional[str],
) -> list[str]:
    argv: list[str] = [
        "pi",
        "--mode",
        "json",
        "--session-dir",
        session_dir,
        # Trust the (empty) workspace folder so non-interactive runs don't skip
        # resources or stall waiting on a prompt.
        "-a",
    ]
    if model:
        argv += ["--model", model]
    if thinking and thinking.lower() != "off":
        argv += ["--thinking", thinking]
    if session_id:
        argv += ["--session", session_id]
    if session_name:
        argv += ["--name", session_name]
    # The prompt is the trailing positional argument.
    argv.append(message)
    return argv


def _new_usage() -> dict[str, Any]:
    return {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": 0.0,
    }


def _accumulate_usage(usage_total: dict[str, Any], usage: dict[str, Any]) -> None:
    for key in ("input", "output", "cacheRead", "cacheWrite", "totalTokens"):
        usage_total[key] += int(usage.get(key, 0) or 0)
    usage_total["cost"] += float((usage.get("cost") or {}).get("total", 0) or 0)


def run_pi(
    message: str,
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    session_name: Optional[str] = None,
    session_dir: Optional[str] = None,
    thinking: Optional[str] = None,
) -> PiResult:
    """Run pi once with `message` and return the parsed result.

    `session_dir` selects where sessions are stored/read (used to isolate
    sessions per user). If `session_id` is given, the conversation continues
    that session; otherwise pi creates a new one and we capture the new id from
    the session header event.
    """
    session_dir = session_dir or PI_SESSION_DIR
    os.makedirs(PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(session_dir, exist_ok=True)

    argv = _build_argv(message, session_id, model, session_name, session_dir, thinking)

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
    usage_total = _new_usage()

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
                _accumulate_usage(usage_total, message_obj.get("usage") or {})

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


def stream_pi(
    message: str,
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    session_name: Optional[str] = None,
    session_dir: Optional[str] = None,
    thinking: Optional[str] = None,
) -> Iterator[dict[str, Any]]:
    """Run pi and yield normalized events as they happen (for live streaming).

    Yields dicts shaped like:
      {"type": "session", "session_id": str}
      {"type": "thinking", "delta": str}
      {"type": "text", "delta": str}
      {"type": "tool_start", "name": str, "id": str}
      {"type": "tool_end", "name": str, "id": str, "is_error": bool}
      {"type": "error", "detail": str}
      {"type": "done", "session_id": str, "reply": str, "usage": dict}  (last)
    """
    session_dir = session_dir or PI_SESSION_DIR
    os.makedirs(PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(session_dir, exist_ok=True)

    argv = _build_argv(message, session_id, model, session_name, session_dir, thinking)

    parsed_session_id: Optional[str] = session_id
    last_reply = ""
    usage_total = _new_usage()

    err_file = tempfile.TemporaryFile(mode="w+")
    proc = subprocess.Popen(
        argv,
        cwd=PI_WORKSPACE_DIR,
        env=_pi_env(),
        stdout=subprocess.PIPE,
        stderr=err_file,
        text=True,
        bufsize=1,
    )

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = event.get("type")
            if etype == "session" and event.get("id"):
                parsed_session_id = event["id"]
                yield {"type": "session", "session_id": parsed_session_id}
            elif etype == "message_update":
                ame = event.get("assistantMessageEvent") or {}
                atype = ame.get("type")
                delta = ame.get("delta")
                if delta and atype == "text_delta":
                    yield {"type": "text", "delta": delta}
                elif delta and atype == "thinking_delta":
                    yield {"type": "thinking", "delta": delta}
            elif etype == "tool_execution_start":
                yield {
                    "type": "tool_start",
                    "name": event.get("toolName"),
                    "id": event.get("toolCallId"),
                }
            elif etype == "tool_execution_end":
                yield {
                    "type": "tool_end",
                    "name": event.get("toolName"),
                    "id": event.get("toolCallId"),
                    "is_error": bool(event.get("isError")),
                }
            elif etype in ("message_end", "turn_end"):
                msg = event.get("message") or {}
                if msg.get("role") == "assistant":
                    text = _extract_text(msg.get("content"))
                    if text.strip():
                        last_reply = text
                    _accumulate_usage(usage_total, msg.get("usage") or {})
        proc.wait()
    except Exception as exc:  # noqa: BLE001
        try:
            proc.kill()
        except Exception:
            pass
        yield {"type": "error", "detail": str(exc)}
    finally:
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception:
            pass

    if not last_reply and proc.returncode not in (0, None):
        try:
            err_file.seek(0)
            tail = err_file.read()[-500:]
        except Exception:
            tail = ""
        yield {
            "type": "error",
            "detail": f"pi exited with code {proc.returncode}. {tail}".strip(),
        }
    try:
        err_file.close()
    except Exception:
        pass

    yield {
        "type": "done",
        "session_id": parsed_session_id,
        "reply": last_reply,
        "usage": usage_total,
    }
