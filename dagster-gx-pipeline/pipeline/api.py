from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from pipeline.settings import settings

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


def fetch_market_data(date: datetime, deterministic: bool) -> dict[str, Any]:
    if deterministic:
        payload = json.loads(Path(settings.sample_api_payload).read_text())
        payload["fetched_at"] = date.replace(tzinfo=timezone.utc).isoformat()
        return payload

    params = {"vs_currency": "usd", "days": 1, "interval": "daily"}
    response = requests.get(COINGECKO_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return payload
