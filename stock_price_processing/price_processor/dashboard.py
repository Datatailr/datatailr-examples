import json
import logging
import os
import time
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template, request

log = logging.getLogger(__name__)

_user_info_cache: dict[str, tuple[str, str]] = {}


def _resolve_user_display_name_and_email(username: str) -> tuple[str, str]:
    if username in _user_info_cache:
        return _user_info_cache[username]
    display_name = username
    email = ""
    try:
        from datatailr import User

        user = User(username)
        display_name = getattr(user, "name", None) or getattr(user, "username", None) or username
        email = getattr(user, "email", None) or ""
    except Exception as exc:
        log.debug("Could not resolve User for %r: %s", username, exc)

    _user_info_cache[username] = (display_name, email)
    return display_name, email


def get_user_ribbon_context() -> dict:
    raw = request.headers.get("x-datatailr-user")
    if not raw or not raw.strip():
        return {
            "user_ribbon_show": False,
            "user_display_name": "",
            "user_email": "",
        }
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.warning("Invalid x-datatailr-user header (not JSON)")
        return {
            "user_ribbon_show": False,
            "user_display_name": "",
            "user_email": "",
        }

    username = payload.get("name")
    if not username:
        return {
            "user_ribbon_show": False,
            "user_display_name": "",
            "user_email": "",
        }

    display_name, email = _resolve_user_display_name_and_email(str(username))
    return {
        "user_ribbon_show": True,
        "user_display_name": display_name,
        "user_email": email,
    }


_PKG = Path(__file__).parent
_TEMPLATES_DIR = str(_PKG / "templates")

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")

if _job_type == "workstation":
    _PREFIX = f"/workstation/{_env}/{_job}/ide/proxy/5070/"
elif _env and _job:
    _PREFIX = f"/job/{_env}/{_job}/"
else:
    _PREFIX = "/"

app = Flask(__name__, template_folder=_TEMPLATES_DIR)

if _job_type in ("workstation", ""):
    PRICE_PROCESSOR_URL = os.environ.get("PRICE_PROCESSOR_URL", "http://localhost:8081")
else:
    PRICE_PROCESSOR_URL = os.environ.get("PRICE_PROCESSOR_URL", "http://price-processor")


def _app_path(suffix: str = "") -> str:
    base = _PREFIX.rstrip("/")
    suf = suffix.strip("/")
    if not suf:
        return f"{base}/" if base else "/"
    return f"{base}/{suf}" if base else f"/{suf}"


def _common_template_ctx() -> dict:
    ctx = get_user_ribbon_context()
    ctx["api_processor_stream"] = _app_path("api/processor/stream")
    ctx["api_processor_analytics"] = _app_path("api/processor/analytics")
    ctx["api_processor_stats"] = _app_path("api/processor/stats")
    ctx["api_processor_topology"] = _app_path("api/processor/topology")
    return ctx


@app.route("/")
def processor_dashboard():
    return render_template("processor.html", **_common_template_ctx())


@app.route("/api/processor/stream")
def processor_stream_proxy():
    def generate():
        while True:
            try:
                with requests.get(
                    f"{PRICE_PROCESSOR_URL}/stream",
                    stream=True,
                    timeout=(5, None),
                    headers={"Accept": "text/event-stream"},
                ) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                        if chunk:
                            yield chunk
            except Exception as exc:
                log.warning("Processor SSE proxy error: %s - retrying in 2s", exc)
                yield 'event: error\ndata: {"error":"processor unavailable"}\n\n'
                time.sleep(2)

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/processor/analytics")
def processor_analytics_proxy():
    try:
        r = requests.get(f"{PRICE_PROCESSOR_URL}/analytics", timeout=15)
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("processor_analytics_proxy: %s", exc)
        return jsonify({"error": str(exc)}), 502


@app.route("/api/processor/stats")
def processor_stats_proxy():
    try:
        r = requests.get(f"{PRICE_PROCESSOR_URL}/stats", timeout=15)
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("processor_stats_proxy: %s", exc)
        return jsonify({"error": str(exc)}), 502


@app.route("/api/processor/topology")
def processor_topology_proxy():
    try:
        r = requests.get(f"{PRICE_PROCESSOR_URL}/topology", timeout=15)
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("processor_topology_proxy: %s", exc)
        return jsonify({"error": str(exc)}), 502


@app.route("/health")
def health_check():
    return "OK\n"


if __name__ == "__main__":
    app.run(debug=True, port=5070)
