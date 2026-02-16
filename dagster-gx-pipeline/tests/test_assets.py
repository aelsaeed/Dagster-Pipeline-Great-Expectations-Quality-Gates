from __future__ import annotations

import importlib
from pathlib import Path

from dagster import build_asset_context


def test_assets_end_to_end(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "pipeline.duckdb"))

    project_root = Path(__file__).resolve().parents[1]
    sample_src = project_root / "data" / "sample_api_payload.json"
    (tmp_path / "sample_api_payload.json").write_text(sample_src.read_text())

    import pipeline.settings as settings

    importlib.reload(settings)

    import pipeline.assets as assets
    import pipeline.io as io

    importlib.reload(io)
    importlib.reload(assets)

    context = build_asset_context(partition_key="2024-05-01", asset_config={"deterministic": True})
    raw_df = assets.raw_asset(context)
    cleaned_df = assets.cleaned_asset(context, raw_df)
    agg_df = assets.agg_asset(context, cleaned_df)

    assert not raw_df.empty
    assert not cleaned_df.empty
    assert not agg_df.empty
