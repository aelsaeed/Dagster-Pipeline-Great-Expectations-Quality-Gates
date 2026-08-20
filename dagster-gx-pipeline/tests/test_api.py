from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import requests
from conftest import PARTITION
from dagster_gx_pipeline.api import COINGECKO_RANGE_URL, MarketDataError, fetch_market_data
from dagster_gx_pipeline.assets import payload_to_frame


class FakeResponse:
    def __init__(self, payload: object, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error

    def raise_for_status(self) -> None:
        if self.error:
            raise self.error

    def json(self) -> object:
        return self.payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((url, kwargs))
        return self.response

    def close(self) -> None:
        self.closed = True


def test_payload_normalization_merges_and_sorts_all_points(
    valid_payload: dict[str, Any],
) -> None:
    payload = dict(valid_payload)
    payload["prices"] = list(reversed(valid_payload["prices"]))
    payload["total_volumes"] = list(reversed(valid_payload["total_volumes"]))

    frame = payload_to_frame(payload, PARTITION)

    assert len(frame) == 4
    assert frame["timestamp_ms"].is_monotonic_increasing
    assert frame["price_usd"].tolist() == [62_000.0, 62_500.0, 61_800.0, 63_000.0]
    assert frame["market_cap_usd"].tolist()[1] == 1_210_000_000_000.0
    assert frame["volume_usd"].tolist()[-1] == 27_500_000_000.0
    assert frame["partition_date"].unique().tolist() == [PARTITION]


def test_payload_normalization_rejects_timestamp_outside_partition(
    valid_payload: dict[str, Any],
) -> None:
    payload = dict(valid_payload)
    payload["prices"] = [*valid_payload["prices"], [1714608000000, 64_000.0]]

    with pytest.raises(MarketDataError, match="do not match partition"):
        payload_to_frame(payload, PARTITION)


def test_deterministic_fetch_uses_fixture_without_calling_session(
    tmp_path: Path,
    valid_payload: dict[str, Any],
) -> None:
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps(valid_payload), encoding="utf-8")
    session = FakeSession(FakeResponse(valid_payload))

    payload = fetch_market_data(
        datetime(2024, 5, 1, 17, tzinfo=UTC),
        deterministic=True,
        fixture_path=fixture,
        session=cast(requests.Session, session),
    )

    assert session.calls == []
    assert payload["prices"] == valid_payload["prices"]
    assert payload["fetched_at"] == "2024-05-01T00:00:00+00:00"


def test_live_fetch_uses_exact_partition_window_and_injected_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    valid_payload: dict[str, Any],
) -> None:
    monkeypatch.setenv("COINGECKO_API_KEY", "demo-key")
    inclusive_payload = dict(valid_payload)
    for series_name, boundary_value in (
        ("prices", 64_000.0),
        ("market_caps", 1_230_000_000_000.0),
        ("total_volumes", 28_000_000_000.0),
    ):
        points = cast(list[list[float]], valid_payload[series_name])
        inclusive_payload[series_name] = [*points, [1714608000000, boundary_value]]
    session = FakeSession(FakeResponse(inclusive_payload))

    payload = fetch_market_data(
        datetime(2024, 5, 1, 17, tzinfo=UTC),
        deterministic=False,
        fixture_path=tmp_path / "unused.json",
        session=cast(requests.Session, session),
    )

    assert len(session.calls) == 1
    url, request = session.calls[0]
    assert url == COINGECKO_RANGE_URL
    assert request["params"] == {
        "vs_currency": "usd",
        "from": 1714521600,
        "to": 1714608000,
    }
    assert request["headers"] == {"x-cg-demo-api-key": "demo-key"}
    assert request["timeout"] == (5, 30)
    assert not session.closed
    assert payload["prices"] == valid_payload["prices"]
    assert payload["market_caps"] == valid_payload["market_caps"]
    assert payload["total_volumes"] == valid_payload["total_volumes"]
    datetime.fromisoformat(cast(str, payload["fetched_at"]))


def test_fetch_wraps_fixture_and_request_errors(
    tmp_path: Path,
    valid_payload: dict[str, Any],
) -> None:
    broken_fixture = tmp_path / "broken.json"
    broken_fixture.write_text("not-json", encoding="utf-8")
    with pytest.raises(MarketDataError, match="Unable to load fixture"):
        fetch_market_data(
            datetime(2024, 5, 1, tzinfo=UTC),
            deterministic=True,
            fixture_path=broken_fixture,
        )

    session = FakeSession(FakeResponse(valid_payload, requests.Timeout("too slow")))
    with pytest.raises(MarketDataError, match="CoinGecko request failed"):
        fetch_market_data(
            datetime(2024, 5, 1, tzinfo=UTC),
            deterministic=False,
            fixture_path=tmp_path / "unused.json",
            session=cast(requests.Session, session),
        )
