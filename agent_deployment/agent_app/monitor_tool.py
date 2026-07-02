"""``check_subagents`` -- a bash-callable helper the main agent's pi uses to
monitor the sub-agents it has spawned (specification §6, §13).

It is installed on ``PATH`` (see ``build_script_pre.sh``) as ``check_subagents``
so pi can call it from its bash tool, e.g.::

    check_subagents                 # compact status table for this session
    check_subagents --id <sid>      # full assignment + result for one sub-agent
    check_subagents --json          # raw JSON (for programmatic reads)

The helper GETs the App's own orchestration API on localhost. Listing is scoped
to the parent session via ``SWE_PARENT_SESSION`` (injected into the pi process
by the App) so pi sees exactly the sub-agents it launched. Fold-back of finished
sub-agents still happens automatically; this is for on-demand checks.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def _orchestrator_url() -> str:
    base = os.environ.get("SWE_ORCH_URL")
    if base:
        return base.rstrip("/")
    port = os.environ.get("SWE_APP_PORT") or os.environ.get("PORT") or "8080"
    return f"http://127.0.0.1:{port}"


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fmt_table(subs: list[dict]) -> str:
    if not subs:
        return "No sub-agents spawned from this session yet."
    subs = sorted(subs, key=lambda s: s.get("launched_at") or "", reverse=True)
    rows = [("SUB-AGENT", "TASK", "STATE", "STATUS", "TURNS", "COST", "PR")]
    for s in subs:
        state = (s.get("status") or s.get("state")) if s.get("reported") else s.get("state")
        cost = s.get("cost") or 0.0
        rows.append(
            (
                str(s.get("subagent_id", ""))[-28:],
                str(s.get("title", ""))[:32],
                str(state or "launched"),
                str(s.get("status") or "-"),
                str(s.get("turns") or 0),
                f"${float(cost):.3f}",
                str(s.get("pr_url") or "-"),
            )
        )
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in rows]

    # Surface problems (PR failures, warnings) beneath the table so they are
    # never silent -- a pushed branch with no PR, a failed clone, etc.
    notes: list[str] = []
    for s in subs:
        sid = str(s.get("subagent_id", ""))[-28:]
        problems: list[str] = []
        if s.get("pr_error"):
            problems.append(str(s["pr_error"]))
        for w in s.get("warnings") or []:
            if str(w) not in problems:
                problems.append(str(w))
        for p in problems:
            notes.append(f"  ! {sid}: {p}")
    if notes:
        lines.append("")
        lines.append("Warnings / errors:")
        lines.extend(notes)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_subagents",
        description="Show the status of sub-agents spawned from this session.",
    )
    parser.add_argument("--id", default=None, help="Show one sub-agent's assignment + result.")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON.")
    args = parser.parse_args(argv)

    base = _orchestrator_url()
    if args.id:
        url = f"{base}/subagents/{urllib.parse.quote(args.id, safe='')}"
    else:
        params = {}
        parent = os.environ.get("SWE_PARENT_SESSION")
        user = os.environ.get("SWE_USER")
        if parent:
            params["parent_id"] = parent
        if user:
            params["user"] = user
        query = urllib.parse.urlencode(params)
        url = f"{base}/subagents" + (f"?{query}" if query else "")

    try:
        body = _get(url)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        print(f"check_subagents failed (HTTP {exc.code}): {detail}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"check_subagents failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(body, indent=2))
        return 0

    if args.id:
        entry = body.get("entry") or {}
        result = body.get("result")
        print(_fmt_table([entry]) if entry else "Sub-agent not found.")
        if result:
            print("\n--- result ---")
            print(json.dumps(result, indent=2))
        return 0

    print(_fmt_table(body.get("subagents") or []))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
