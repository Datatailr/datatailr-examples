"""``spawn_subagent`` -- a bash-callable helper the main agent's pi uses to
delegate a scoped task to a sub-agent (§6 point 2).

It is installed on ``PATH`` (see ``build_script_pre.sh``) as ``spawn_subagent``
so pi can call it from its bash tool, e.g.::

    spawn_subagent --title "Add retry to HTTP client" \
        --instructions "Wrap requests in urllib3 Retry; add a unit test." \
        --done "retry/backoff added" --done "unit test passes" \
        --files src/client.py

The helper POSTs the brief to the App's own ``/subagents`` endpoint on
localhost; the coordinator enforces all limits and returns a spawn result or a
refusal message, which is printed for pi to read. Parent session/user context
is supplied by the App via ``SWE_PARENT_SESSION`` / ``SWE_USER`` env vars
injected into the pi process.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def _orchestrator_url() -> str:
    base = os.environ.get("SWE_ORCH_URL")
    if base:
        return base.rstrip("/")
    port = os.environ.get("SWE_APP_PORT") or os.environ.get("PORT") or "8080"
    return f"http://127.0.0.1:{port}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="spawn_subagent",
        description="Delegate a scoped task to a sub-agent on Datatailr.",
    )
    parser.add_argument("--title", required=True, help="Short task title.")
    parser.add_argument(
        "--instructions", required=True, help="Full pi prompt / brief for the sub-agent."
    )
    parser.add_argument(
        "--done",
        action="append",
        default=[],
        help="A definition-of-done item (repeatable).",
    )
    parser.add_argument(
        "--files",
        default="",
        help="Comma-separated focus files (optional hints).",
    )
    parser.add_argument("--branch", default=None, help="Work branch (optional).")
    parser.add_argument(
        "--no-pr", action="store_true", help="Push a branch but do not open a PR."
    )
    parser.add_argument(
        "--no-push", action="store_true", help="Do not push (local edits only)."
    )
    args = parser.parse_args(argv)

    brief = {
        "title": args.title,
        "instructions": args.instructions,
        "definition_of_done": [d for d in args.done if d.strip()],
        "files": [f.strip() for f in args.files.split(",") if f.strip()],
        "branch": args.branch,
        "may_push": not args.no_push,
        "may_open_pr": not args.no_pr,
    }
    payload = {
        "briefs": [brief],
        "parent_id": os.environ.get("SWE_PARENT_SESSION"),
        "created_by": os.environ.get("SWE_USER"),
        "session_id": os.environ.get("SWE_PARENT_SESSION"),
        "depth": int(os.environ.get("SWE_DEPTH", "0")),
    }

    url = f"{_orchestrator_url()}/subagents"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        print(f"spawn_subagent failed (HTTP {exc.code}): {detail}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"spawn_subagent failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(body, indent=2))
    results = body.get("results", [])
    if results and all(r.get("refused") for r in results):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
