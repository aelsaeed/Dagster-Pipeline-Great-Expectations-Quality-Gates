from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
REQUIRED_SERIES = ("prices", "market_caps", "total_volumes")


class MarketDataError(RuntimeError):
    """Raised when upstream market data cannot satisfy the ingestion contract."""


def _validate_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise MarketDataError("CoinGecko payload must be a JSON object")

    validated = dict(payload)
    for series_name in REQUIRED_SERIES:
        points = validated.get(series_name)
        if not isinstance(points, Sequence) or isinstance(points, (str, bytes)) or not points:
            raise MarketDataError(f"Payload field '{series_name}' must contain data points")
        for point in points:
            if (
                not isinstance(point, Sequence)
                or isinstance(point, (str, bytes))
                or len(point) != 2
                or not all(isinstance(value, (int, float)) for value in point)
            ):
                raise MarketDataError(
                    f"Payload field '{series_name}' contains an invalid [timestamp, value] point"
                )
    return validated


def _retrying_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _restrict_to_partition_window(
    payload: object,
    partition_start: datetime,
) -> dict[str, Any]:
    """Normalize CoinGecko's inclusive range response to a half-open daily window."""

    validated = _validate_payload(payload)
    start_ms = int(partition_start.timestamp() * 1_000)
    end_ms = int((partition_start + timedelta(days=1)).timestamp() * 1_000)
    for series_name in REQUIRED_SERIES:
        validated[series_name] = [
            point for point in validated[series_name] if start_ms <= point[0] < end_ms
        ]
    return _validate_payload(validated)


def fetch_market_data(
    partition_time: datetime,
    *,
    deterministic: bool,
    fixture_path: Path,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Load a recorded payload or fetch the exact UTC partition window."""

    partition_start = partition_time.astimezone(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if deterministic:
        try:
            payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise MarketDataError(f"Unable to load fixture {fixture_path}: {error}") from error
        validated = _validate_payload(payload)
        validated["fetched_at"] = partition_start.isoformat()
        return validated

    owned_session = session is None
    client = session or _retrying_session()
    headers: dict[str, str] = {}
    if api_key := os.getenv("COINGECKO_API_KEY"):
        headers["x-cg-demo-api-key"] = api_key
    try:
        request_params: dict[str, str | int] = {
            "vs_currency": "usd",
            "from": int(partition_start.timestamp()),
            "to": int((partition_start + timedelta(days=1)).timestamp()),
        }
        response = client.get(
            COINGECKO_RANGE_URL,
            params=request_params,
            headers=headers,
            timeout=(5, 30),
        )
        response.raise_for_status()
        validated = _restrict_to_partition_window(response.json(), partition_start)
        validated["fetched_at"] = datetime.now(UTC).isoformat()
        return validated
    except (requests.RequestException, ValueError) as error:
        raise MarketDataError(f"CoinGecko request failed: {error}") from error
    finally:
        if owned_session:
            client.close()
