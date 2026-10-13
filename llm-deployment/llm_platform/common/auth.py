"""Request identity and admin gating.

The platform's ingress sets ``X-Datatailr-User`` on every request after it has
validated the caller's session cookie or API token. ``User.from_request``
turns that header into a ``User``. Admin rights are given to members of the
``admin`` group (plus the ``admin`` and ``root`` accounts).
"""

from __future__ import annotations

import os

from datatailr import User
from fastapi import HTTPException, Request

ADMIN_GROUP = "admin"
EXTRA_ADMINS = {name.strip() for name in os.environ.get("LLM_PLATFORM_ADMINS", "").split(",") if name.strip()}
# Local development only: identity to assume when the platform header is absent.
# Never set this on a deployed job.
DEV_USER = os.environ.get("LLM_PLATFORM_DEV_USER", "").strip() if not os.environ.get("DATATAILR_JOB_NAME") else ""


def current_user(request: Request) -> User:
    user = User.from_request(request)
    if user is None and DEV_USER:
        user = User.get(DEV_USER)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def is_admin(user: User) -> bool:
    if user.name in {"admin", "root"} or user.name in EXTRA_ADMINS:
        return True
    try:
        return ADMIN_GROUP in (user.groups or [])
    except Exception:  # noqa: BLE001
        return False


def require_admin(request: Request) -> User:
    user = current_user(request)
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Administrator access required")
    return user
