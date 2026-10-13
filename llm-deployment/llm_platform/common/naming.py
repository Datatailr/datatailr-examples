"""Naming helpers shared by the admin app, the gateway and the vLLM service.

Datatailr derives a service's internal hostname from its display name by
lowercasing it and replacing every non-alphanumeric run with a hyphen. Every
LLM deployment gets a short *slug*; its service is displayed as ``LLM <slug>``
and is therefore reachable at ``http://llm-<slug>`` inside the cluster.
"""

from __future__ import annotations

import re

SERVICE_PREFIX = "llm"
GATEWAY_NAME = "LLM Gateway"
ADMIN_APP_NAME = "LLM Admin"
APP_SECTION = "AI & Integrations"

STATE_BLOB = "llm_platform/state/registry.json"
LOGS_PREFIX = "llm_platform/logs/"


def normalize_name(name: str) -> str:
    """Mirror the platform's hostname normalization for job display names."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def slugify(value: str, max_length: int = 32) -> str:
    """Turn a free-form deployment name into a DNS-friendly slug."""
    slug = normalize_name(value)
    slug = re.sub(r"-{2,}", "-", slug)[:max_length].strip("-")
    if not slug:
        raise ValueError("Name must contain at least one letter or digit")
    return slug


def service_display_name(slug: str) -> str:
    return f"{SERVICE_PREFIX.upper()} {slug}"


def service_internal_name(slug: str) -> str:
    return normalize_name(service_display_name(slug))


def service_internal_url(slug: str) -> str:
    return f"http://{service_internal_name(slug)}"


def gateway_internal_name() -> str:
    return normalize_name(GATEWAY_NAME)
