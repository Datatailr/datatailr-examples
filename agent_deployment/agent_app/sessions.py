"""Read and aggregate pi session history from a session store directory.

Pi writes each session as a JSONL file (see
https://pi.dev/docs/latest/session-format): the first line is a `session`
header, subsequent lines are tree entries. We only need the linear list of
entries here -- enough to render a transcript and compute activity statistics.

All functions take an explicit `session_dir` so the caller can scope reads to a
single user's session store. Blob persistence lives in `blob_sync`.
"""

from __future__ import annotations

import glob
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Optional


# --------------------------------------------------------------------------- #
# Low-level file parsing
# --------------------------------------------------------------------------- #
def _session_files(session_dir: str) -> list[str]:
    """All session JSONL files under the (possibly nested) session dir."""
    pattern = os.path.join(session_dir, "**", "*.jsonl")
    flat = os.path.join(session_dir, "*.jsonl")
    return sorted(set(glob.glob(pattern, recursive=True)) | set(glob.glob(flat)))


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return entries


def _text_from_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            parts.append(block.get("text", ""))
        elif block.get("type") == "image":
            parts.append("[image]")
    return "".join(parts)


def _parse_ts(value: Any) -> Optional[datetime]:
    """Best-effort parse of either ISO-8601 strings or unix-ms numbers."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


# --------------------------------------------------------------------------- #
# Per-session views
# --------------------------------------------------------------------------- #
def _summarize_session(path: str) -> dict[str, Any]:
    entries = _read_jsonl(path)
    header = next((e for e in entries if e.get("type") == "session"), {})
    session_id = header.get("id") or os.path.splitext(os.path.basename(path))[0]

    name: Optional[str] = None
    first_user: Optional[str] = None
    message_count = 0
    tokens = 0
    cost = 0.0
    created = _parse_ts(header.get("timestamp"))
    last_active = created

    for entry in entries:
        etype = entry.get("type")
        ts = _parse_ts(entry.get("timestamp"))
        if ts:
            last_active = ts if last_active is None else max(last_active, ts)
        if etype == "session_info" and entry.get("name"):
            name = entry["name"]
        elif etype == "message":
            msg = entry.get("message") or {}
            role = msg.get("role")
            if role in ("user", "assistant"):
                message_count += 1
            if role == "user" and first_user is None:
                first_user = _text_from_content(msg.get("content")).strip()
            if role == "assistant":
                usage = msg.get("usage") or {}
                tokens += int(usage.get("totalTokens", 0) or 0)
                cost += float((usage.get("cost") or {}).get("total", 0) or 0)

    title = name or (first_user[:80] if first_user else "(untitled session)")
    return {
        "id": session_id,
        "file": os.path.basename(path),
        "name": title,
        "created": created.isoformat() if created else None,
        "last_active": last_active.isoformat() if last_active else None,
        "message_count": message_count,
        "tokens": tokens,
        "cost": round(cost, 6),
    }


def list_sessions(session_dir: str) -> list[dict[str, Any]]:
    """Summary of every stored session, newest activity first."""
    summaries = [_summarize_session(p) for p in _session_files(session_dir)]
    summaries.sort(key=lambda s: s.get("last_active") or "", reverse=True)
    return summaries


def _find_file_for_session(session_id: str, session_dir: str) -> Optional[str]:
    for path in _session_files(session_dir):
        entries = _read_jsonl(path)
        header = next((e for e in entries if e.get("type") == "session"), {})
        if header.get("id") == session_id:
            return path
        if os.path.splitext(os.path.basename(path))[0].endswith(session_id):
            return path
    return None


def get_transcript(session_id: str, session_dir: str) -> Optional[dict[str, Any]]:
    """Linear transcript (user/assistant/tool messages) for one session."""
    path = _find_file_for_session(session_id, session_dir)
    if not path:
        return None

    entries = _read_jsonl(path)
    messages: list[dict[str, Any]] = []
    for entry in entries:
        if entry.get("type") != "message":
            continue
        msg = entry.get("message") or {}
        role = msg.get("role")
        ts = _parse_ts(entry.get("timestamp"))
        if role in ("user", "assistant"):
            tool_calls = []
            if isinstance(msg.get("content"), list):
                tool_calls = [
                    b.get("name")
                    for b in msg["content"]
                    if isinstance(b, dict) and b.get("type") == "toolCall"
                ]
            messages.append(
                {
                    "role": role,
                    "text": _text_from_content(msg.get("content")),
                    "tool_calls": tool_calls,
                    "timestamp": ts.isoformat() if ts else None,
                }
            )
        elif role == "toolResult":
            messages.append(
                {
                    "role": "tool",
                    "tool_name": msg.get("toolName"),
                    "is_error": bool(msg.get("isError")),
                    "text": _text_from_content(msg.get("content")),
                    "timestamp": ts.isoformat() if ts else None,
                }
            )

    summary = _summarize_session(path)
    return {"session": summary, "messages": messages}


# --------------------------------------------------------------------------- #
# Aggregate statistics across all sessions
# --------------------------------------------------------------------------- #
def aggregate_stats(session_dir: str) -> dict[str, Any]:
    totals = {
        "sessions": 0,
        "messages": 0,
        "user_messages": 0,
        "assistant_messages": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "total_tokens": 0,
        "cost": 0.0,
    }
    tool_usage: Counter[str] = Counter()
    model_usage: Counter[str] = Counter()
    activity_by_day: dict[str, int] = defaultdict(int)

    for path in _session_files(session_dir):
        entries = _read_jsonl(path)
        if not entries:
            continue
        totals["sessions"] += 1
        for entry in entries:
            if entry.get("type") != "message":
                continue
            msg = entry.get("message") or {}
            role = msg.get("role")
            ts = _parse_ts(entry.get("timestamp"))
            if ts:
                activity_by_day[ts.date().isoformat()] += 1
            if role in ("user", "assistant"):
                totals["messages"] += 1
            if role == "user":
                totals["user_messages"] += 1
            elif role == "assistant":
                totals["assistant_messages"] += 1
                if msg.get("model"):
                    model_usage[str(msg["model"])] += 1
                usage = msg.get("usage") or {}
                totals["input_tokens"] += int(usage.get("input", 0) or 0)
                totals["output_tokens"] += int(usage.get("output", 0) or 0)
                totals["cache_read_tokens"] += int(usage.get("cacheRead", 0) or 0)
                totals["cache_write_tokens"] += int(usage.get("cacheWrite", 0) or 0)
                totals["total_tokens"] += int(usage.get("totalTokens", 0) or 0)
                totals["cost"] += float((usage.get("cost") or {}).get("total", 0) or 0)
                if isinstance(msg.get("content"), list):
                    for block in msg["content"]:
                        if isinstance(block, dict) and block.get("type") == "toolCall":
                            tool_usage[block.get("name", "unknown")] += 1

    totals["cost"] = round(totals["cost"], 6)
    timeline = [
        {"date": day, "messages": count}
        for day, count in sorted(activity_by_day.items())
    ]
    return {
        "totals": totals,
        "tool_usage": [
            {"tool": name, "count": count} for name, count in tool_usage.most_common()
        ],
        "model_usage": [
            {"model": name, "count": count} for name, count in model_usage.most_common()
        ],
        "timeline": timeline,
    }
