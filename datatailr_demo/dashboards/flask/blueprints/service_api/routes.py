# datatailr_demo/dashboards/flask/blueprints/service_api/routes.py
# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************

from flask import Blueprint, jsonify, render_template, request

try:
    import requests as req
    from datatailr.wrapper import dt__Job
    from datatailr import construct_remote_base_url
    DATATAILR_AVAILABLE = True
except ImportError:
    DATATAILR_AVAILABLE = False

service_api_bp = Blueprint("service_api", __name__)

def _get_job_client():
    if DATATAILR_AVAILABLE:
        return dt__Job()
    return None

job_client = _get_job_client()


@service_api_bp.route("/service-api")
def service_api():
    return render_template("service_api.html", page="service_api")


@service_api_bp.route("/api/services")
def api_services():
    if not job_client:
        return jsonify([])
    return _process_request(_get_services)

def _get_services():
    runs = job_client.runs(filter="type = service and state = running")
    names = [r["job_name"] for r in runs]
    return jsonify([{"name": n} for n in names])


@service_api_bp.route("/api/service-openapi")
def api_service_openapi():
    service_name = request.args.get("name")
    if not service_name:
        return jsonify({"error": "name is required"}), 400
    return _process_request(lambda: _get_service_openapi(service_name))

def _get_service_openapi(service_name):
    base_url = construct_remote_base_url(service_name)
    resp = req.get(f"{base_url}/openapi.json", timeout=5)
    return jsonify(resp.json())

def _process_request(func):
    try:
        return func()
    except Exception as e:
        return jsonify({"error": str(e)}), 500