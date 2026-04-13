import json
import logging
import os
import time
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, Response, request

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

def _app_path(suffix: str = "") -> str:
    """Browser-absolute path (includes /job/.../ prefix when deployed on Datatailr)."""
    base = _PREFIX.rstrip("/")
    suf = suffix.strip("/")
    if not suf:
        return f"{base}/" if base else "/"
    return f"{base}/{suf}" if base else f"/{suf}"


def _common_template_ctx() -> dict:
    ctx = get_user_ribbon_context()
    ctx["monitor_url"] = _app_path("")
    ctx["tickers_url"] = _app_path("tickers")
    ctx["api_tickers_url"] = _app_path("api/tickers")
    return ctx


@app.route("/")
def index():
    user_ctx = _common_template_ctx()
    return render_template(
        "index.html",
        stream_url=_app_path("stream"),
        **user_ctx,
    )


@app.route("/tickers")
def tickers_page():
    return render_template("tickers.html", **_common_template_ctx())


@app.route("/health")
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


@app.route("/api/tickers", methods=["GET"])
def api_tickers_list():
    """Proxy: GET price-server /tickers."""
    try:
        r = requests.get(f"{PRICE_SERVER_URL}/tickers", timeout=15)
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("api_tickers_list: %s", exc)
        return jsonify({"error": str(exc)}), 502


@app.route("/api/tickers", methods=["POST"])
def api_tickers_add():
    """Proxy: PUT price-server /add/{ticker} with price & vol query params."""
    data = request.get_json(silent=True) or {}
    raw = (data.get("ticker") or data.get("symbol") or "").strip()
    if not raw:
        return jsonify({"error": "ticker is required"}), 400
    ticker = raw.upper()
    try:
        price = float(data.get("price", 100.0))
        vol = float(data.get("vol", 0.25))
    except (TypeError, ValueError):
        return jsonify({"error": "price and vol must be numbers"}), 400
    try:
        r = requests.put(
            f"{PRICE_SERVER_URL}/add/{ticker}",
            params={"price": price, "vol": vol},
            timeout=15,
        )
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("api_tickers_add: %s", exc)
        return jsonify({"error": str(exc)}), 502


@app.route("/api/tickers/<ticker>", methods=["DELETE"])
def api_tickers_remove(ticker: str):
    """Proxy: PUT price-server /remove/{ticker}."""
    sym = ticker.strip().upper()
    if not sym:
        return jsonify({"error": "invalid ticker"}), 400
    try:
        r = requests.put(f"{PRICE_SERVER_URL}/remove/{sym}", timeout=15)
        return app.response_class(
            response=r.content,
            status=r.status_code,
            mimetype="application/json",
        )
    except requests.RequestException as exc:
        log.warning("api_tickers_remove: %s", exc)
        return jsonify({"error": str(exc)}), 502


if __name__ == "__main__":
    app.run(debug=True, port=5050)
