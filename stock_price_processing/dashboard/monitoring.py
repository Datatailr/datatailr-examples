import os
from importlib.resources import files
from pathlib import Path

from flask import Flask, jsonify

_PKG = Path(__file__).parent
_STATIC_DIR = str(Path(str(_PKG.joinpath("static"))))
_TEMPLATES_DIR = str(Path(str(_PKG.joinpath("templates"))))

app = Flask(__name__, template_folder=_TEMPLATES_DIR, static_folder=_STATIC_DIR)

PRICE_SERVER_URL = "http://price-server"
if os.getenv("DATATAILR_JOB_TYPE", "workstation") == "workstation":
    PRICE_SERVER_URL = "http://localhost:8080"


@app.route("/")
def index():
    from flask import render_template
    return render_template("index.html", price_server_url=PRICE_SERVER_URL)


@app.route("/__health_check__.html")
def health_check():
    return "OK\n"


@app.route("/config")
def config():
    return jsonify({"price_server_url": PRICE_SERVER_URL})


if __name__ == "__main__":
    app.run(debug=True, port=5050)
