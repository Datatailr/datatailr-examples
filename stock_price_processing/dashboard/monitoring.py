import json
import logging
import os
import time
from pathlib import Path

import requests
from flask import Flask, render_template, Response, request

log = logging.getLogger(__name__)

# Cache User lookups by platform username (from x-datatailr-user) to avoid repeated SDK calls.
_user_info_cache: dict[str, tuple[str, str]] = {}


def _resolve_user_display_name_and_email(username: str) -> tuple[str, str]:
    """Return (display_name, email) for a platform username; uses datatailr.User once per username."""
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
    """
    Build template variables for the header ribbon.
    Uses X-Datatailr-User JSON header when present; otherwise local / off-platform.
    """
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
_STATIC_DIR = str(_PKG / "static")
_TEMPLATES_DIR = str(_PKG / "templates")

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")

if _job_type == "workstation":
    _PREFIX = f"/workstation/{_env}/{_job}/ide/proxy/5050/"
elif _env and _job:
    _PREFIX = f"/job/{_env}/{_job}/"
else:
    _PREFIX = "/"

app = Flask(__name__, template_folder=_TEMPLATES_DIR, static_folder=_STATIC_DIR)

PRICE_SERVER_URL = "http://price-server"
if _job_type in ("workstation", ""):
    PRICE_SERVER_URL = "http://localhost:8080"


@app.route("/")
def index():
    stream_url = _PREFIX.rstrip("/") + "/stream" if _PREFIX != "/" else "/stream"
    user_ctx = get_user_ribbon_context()
    return render_template(
        "index.html",
        stream_url=stream_url,
        **user_ctx,
    )


@app.route("/__health_check__.html")
def health_check():
    return "OK\n"


@app.route("/stream")
def stream_proxy():
    """Relay the SSE stream from the price server so the browser stays same-origin."""
    def generate():
        while True:
            try:
                with requests.get(
                    f"{PRICE_SERVER_URL}/stream",
                    stream=True,
                    timeout=(5, None),
                    headers={"Accept": "text/event-stream"},
                ) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                        if chunk:
                            yield chunk
            except Exception as exc:
                log.warning("SSE upstream error: %s – retrying in 2s", exc)
                yield f"event: error\ndata: upstream unavailable\n\n"
                time.sleep(2)

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    app.run(debug=True, port=5050)
