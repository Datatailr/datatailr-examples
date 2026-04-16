"""FRED API client for time-series observations."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests

from credit_spread_prediction.config import FRED_BASE_URL


@dataclass(frozen=True)
class FredSeriesRequest:
    series_id: str
    observation_start: str = "1990-01-01"
    observation_end: str | None = None
    frequency: str | None = None


def _resolve_fred_api_key() -> str:
    env_key = (
        os.environ.get("FRED_API_KEY", "").strip()
        or os.environ.get("FRED_APIKEY", "").strip()
    )
    if env_key:
        return env_key

    try:
        from datatailr import Secrets

        secret_val = Secrets().get("fred_api_key")
        return str(secret_val).strip()
    except Exception:
        return ""


def require_fred_api_key() -> None:
    api_key = _resolve_fred_api_key()
    if not api_key:
        raise RuntimeError(
            "Missing FRED API key. Add secret `fred_api_key` in Datatailr Secrets Manager "
            "or set env var `FRED_API_KEY`."
        )


def _build_params(req: FredSeriesRequest) -> dict[str, Any]:
    params: dict[str, Any] = {
        "series_id": req.series_id,
        "file_type": "json",
        "observation_start": req.observation_start,
        "sort_order": "asc",
    }
    if req.observation_end:
        params["observation_end"] = req.observation_end
    if req.frequency:
        params["frequency"] = req.frequency
    api_key = _resolve_fred_api_key()
    if not api_key:
        require_fred_api_key()
        api_key = _resolve_fred_api_key()
    params["api_key"] = api_key
    return params


def fetch_observations(req: FredSeriesRequest, timeout_sec: int = 20) -> dict[str, Any]:
    url = f"{FRED_BASE_URL}/series/observations"
    response = requests.get(url, params=_build_params(req), timeout=timeout_sec)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    if "observations" not in payload:
        raise ValueError(f"Invalid FRED response for {req.series_id}: missing observations.")
    return payload


def observations_to_frame(series_id: str, payload: dict[str, Any]) -> pd.DataFrame:
    obs = payload.get("observations", [])
    frame = pd.DataFrame(obs)
    if frame.empty:
        return pd.DataFrame(columns=["date", series_id])
    frame = frame.loc[:, ["date", "value"]].copy()
    frame.rename(columns={"value": series_id}, inplace=True)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame[series_id] = pd.to_numeric(frame[series_id], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame = frame.sort_values("date")
    return frame


def today_iso() -> str:
    return date.today().isoformat()

