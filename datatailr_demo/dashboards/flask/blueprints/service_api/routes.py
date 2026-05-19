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

"""Routes for the service API blueprint."""

from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

try:
    import requests as req
    from datatailr import construct_remote_base_url  # type: ignore
    from datatailr.wrapper import dt__Job  # type: ignore

    DATATAILR_AVAILABLE = True
except ImportError:
    DATATAILR_AVAILABLE = False


service_api_bp = Blueprint("service_api", __name__)
"""Blueprint for the service API"""


def _get_job_client():
    if DATATAILR_AVAILABLE:
        return dt__Job()
    return None


job_client = _get_job_client()


@service_api_bp.route("/service-api")
def service_api():
    """Render the service API page."""
    return render_template("service_api.html", page="service_api")


@service_api_bp.route("/api/services")
def api_services():
    """Get the list of services endpoint."""
    if not job_client:
        return jsonify([])
    return _process_request(_get_services)


def _get_services():
    """Get the list of services."""
    runs = job_client.runs(filter="type = service and state = running")
    names = [r["job_name"] for r in runs]
    return jsonify([{"name": n} for n in names])


@service_api_bp.route("/api/service-openapi")
def api_service_openapi():
    """Get the OpenAPI specification for a service endpoint."""
    service_name = request.args.get("name")
    if not service_name:
        return jsonify({"error": "name is required"}), 400
    return _process_request(lambda: _get_service_openapi(service_name))


def _get_health_status(base_url: str) -> dict:
    """Get the health status of a service."""
    try:
        health_resp = req.get(f"{base_url}/health", timeout=5)
        return {"status": "healthy", "code": health_resp.status_code}
    except Exception:  # pylint: disable=broad-exception-caught
        return {"status": "unreachable", "code": None}


def _get_openapi_spec(base_url: str) -> dict | None:
    """Get the OpenAPI specification for a service."""
    try:
        resp = req.get(f"{base_url}/openapi.json", timeout=5)
        resp.raise_for_status()
        spec = resp.json()
        spec["servers"] = [{"url": base_url}]
        return spec
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def _get_service_openapi(service_name: str):
    """Get the OpenAPI specification and health status for a service."""
    base_url = construct_remote_base_url(service_name)
    return jsonify(
        {
            "spec": _get_openapi_spec(base_url),
            "health": _get_health_status(base_url),
        }
    )


def _process_request(func):
    """Process a request."""
    try:
        return func()
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
