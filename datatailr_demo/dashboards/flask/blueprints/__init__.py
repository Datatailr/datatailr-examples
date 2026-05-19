# datatailr_demo/dashboards/flask/blueprints/__init__.py
# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************

"""Blueprints for the Flask application."""
from .service_api import service_api_bp

__all__ = ["service_api_bp"]
