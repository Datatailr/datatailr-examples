import logging
import os
import time
from pathlib import Path

import requests
from flask import Flask, render_template, Response, url_for

log = logging.getLogger(__name__)

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
    return render_template("index.html", stream_url=stream_url)


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
