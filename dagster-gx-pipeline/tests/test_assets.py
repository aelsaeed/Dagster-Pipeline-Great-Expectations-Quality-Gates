from __future__ import annotations

import importlib

from dagster import build_asset_context


def test_assets_end_to_end(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "pipeline.duckdb"))

    import pipeline.settings as settings

    importlib.reload(settings)

    import pipeline.io as io
    import pipeline.assets as assets

    importlib.reload(io)
    importlib.reload(assets)

    context = build_asset_context(partition_key="2024-05-01", op_config={"deterministic": True})
    raw_df = assets.raw_asset(context)
    cleaned_df = assets.cleaned_asset(context, raw_df)
    agg_df = assets.agg_asset(context, cleaned_df)

    assert not raw_df.empty
    assert not cleaned_df.empty
    assert not agg_df.empty
