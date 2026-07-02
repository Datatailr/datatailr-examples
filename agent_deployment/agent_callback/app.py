"""SWE Agent Callback service (§10).

A tiny Flask service reachable from batch containers at ``http://swe-agent-callback``
(internal hostname). A sub-agent optionally notifies it on completion; the
service records the notification to Blob and nudges the main agent to harvest
promptly. It is intentionally minimal and stateless: if it is not deployed, the
system falls back to poll-only harvesting with no loss of correctness.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

from flask import Flask, jsonify, request

app = Flask(__name__)

# Internal hostname of the main agent App (normalized from its display name
# "SWE Main Agent"). Override via env if the app is renamed.
MAIN_AGENT_URL = os.environ.get("SWE_MAIN_AGENT_URL", "http://swe-main-agent")
CALLBACK_BLOB_PREFIX = os.environ.get("AGENT_RUNS_PREFIX", "agent_runs")


def _blob():
    try:
        from datatailr import Blob

        return Blob()
    except Exception:
        return None


@app.route("/health")
def health():
    return "OK\n"


@app.route("/notify", methods=["POST"])
def notify():
    """Record a sub-agent completion notification and nudge the coordinator."""
    payload = request.get_json(silent=True) or {}
    subagent_id = payload.get("subagent_id")
    if not subagent_id:
        return jsonify({"ok": False, "error": "missing subagent_id"}), 400

    record = {
        "subagent_id": subagent_id,
        "status": payload.get("status"),
        "pr_url": payload.get("pr_url"),
        "received_at": time.time(),
    }
    blob = _blob()
    if blob is not None:
        try:
            blob.put(
                f"{CALLBACK_BLOB_PREFIX}/{subagent_id}/callback.json",
                json.dumps(record).encode("utf-8"),
            )
        except Exception:
            pass

    # Best-effort wake-up to the main agent (harvest now instead of next poll).
    nudged = False
    try:
        url = f"{MAIN_AGENT_URL.rstrip('/')}/subagents/{subagent_id}/callback"
        req = urllib.request.Request(url, data=b"{}", method="POST",
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        nudged = True
    except Exception:
        pass

    return jsonify({"ok": True, "recorded": blob is not None, "nudged": nudged})


def main(port):
    app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
