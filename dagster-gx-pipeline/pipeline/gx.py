from __future__ import annotations

from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import FileDataContext

from pipeline.settings import settings


def get_context() -> FileDataContext:
    settings.expectations_dir.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(project_root_dir=str(settings.expectations_dir))
    return context


def _get_or_create_datasource(context: FileDataContext) -> Any:
    datasource_names = {ds["name"] for ds in context.list_datasources()}
    if "pandas_runtime" in datasource_names:
        return context.get_datasource("pandas_runtime")
    return context.sources.add_pandas("pandas_runtime")


def _get_batch_request(context: FileDataContext, asset_name: str, dataframe: pd.DataFrame) -> Any:
    datasource = _get_or_create_datasource(context)
    existing_assets = {asset.name for asset in datasource.assets}
    if asset_name in existing_assets:
        data_asset = datasource.get_asset(asset_name)
    else:
        data_asset = datasource.add_dataframe_asset(asset_name)
    return data_asset.build_batch_request(dataframe=dataframe)


def run_checkpoint(
    context: FileDataContext,
    checkpoint_name: str,
    suite_name: str,
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    batch_request = _get_batch_request(context, checkpoint_name, dataframe)
    checkpoint = SimpleCheckpoint(
        name=checkpoint_name,
        data_context=context,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )
    result = checkpoint.run()
    context.build_data_docs(site_names=["local_site"])
    return result.to_json_dict()


def ensure_expectation_suites(context: FileDataContext) -> None:
    expected_files = [
        settings.expectations_dir / "expectations" / "cleaned_suite.json",
        settings.expectations_dir / "expectations" / "agg_suite.json",
    ]
    for suite_path in expected_files:
        if suite_path.exists():
            suite = context.get_expectation_suite(suite_path.stem)
            context.add_or_update_expectation_suite(suite)


def latest_validation_path() -> Path | None:
    validations_dir = settings.expectations_dir / "validations"
    if not validations_dir.exists():
        return None
    validation_files = sorted(validations_dir.rglob("*.json"))
    return validation_files[-1] if validation_files else None
