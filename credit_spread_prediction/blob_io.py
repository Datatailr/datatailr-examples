"""Blob helpers with lightweight compatibility guards."""

from __future__ import annotations

from typing import Any


def normalize_key(path: str) -> str:
    p = (path or "").strip()
    if not p:
        return "/"
    return p if p.startswith("/") else f"/{p}"


def blob_put(blob: Any, key: str, payload: bytes) -> None:
    key = normalize_key(key)
    if hasattr(blob, "put_blob"):
        blob.put_blob(key, payload)
    else:
        blob.put(key, payload)


def blob_get(blob: Any, key: str) -> bytes:
    key = normalize_key(key)
    if hasattr(blob, "get_blob"):
        return blob.get_blob(key)
    return blob.get(key)


def blob_exists(blob: Any, key: str) -> bool:
    key = normalize_key(key)
    if hasattr(blob, "exists"):
        try:
            return bool(blob.exists(key))
        except Exception:
            pass
    try:
        blob_get(blob, key)
        return True
    except Exception:
        return False

