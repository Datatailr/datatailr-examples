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

from typing import Any, Callable

from flask import Blueprint, Response, jsonify, render_template

try:
    from datatailr import get_dt_env  # type: ignore
    from datatailr.wrapper import dt__Job  # type: ignore

    DATATAILR_AVAILABLE = True
except ImportError:
    DATATAILR_AVAILABLE = False

ResponseOrError = Response | tuple[Response, int]
RequestFunc = Callable[..., Response]
RequestArgs = tuple[Any, ...]

service_api_bp = Blueprint("service_api", __name__)
"""Blueprint for the service API"""


def _get_job_client() -> Any:
    if DATATAILR_AVAILABLE:
        return dt__Job()
    return None


job_client = _get_job_client()


@service_api_bp.route("/service-api")
def service_api() -> str:
    """Render the service API page."""
    try:
        env = str(get_dt_env())
    except Exception:  # pylint: disable=broad-exception-caught
        env = "dev"
    return render_template("service_api.html", page="service_api", env=env)


@service_api_bp.route("/api/services")
def api_services() -> ResponseOrError:
    """Get the list of services endpoint."""
    if not job_client:
        return jsonify([])
    filter_string = "type = service and state = running"
    return _process_request(
        _get_services,
        args=(filter_string,),
    )


def _get_services(filter_string: str) -> Response:
    """
    Get the list of services.

    Args:
        filter_string (str): The filter string to use to get the list
        of services.

    Returns:
        Response: A response containing the list of services.
    """
    runs = job_client.runs(filter=filter_string)
    names = [r["job_name"] for r in runs]
    return jsonify([{"name": n} for n in names])


def _process_request(
    func: RequestFunc,
    args: RequestArgs = (),
) -> ResponseOrError:
    """
    Process a request.

    Args:
        func (RequestFunc): The function to process the request.
    Returns:
        ResponseOrError: A response or a tuple of a response and an error code.
    """
    try:
        return func(*args)
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
