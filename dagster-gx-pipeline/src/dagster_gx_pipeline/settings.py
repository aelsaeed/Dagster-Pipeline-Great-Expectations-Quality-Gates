from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    """Filesystem locations for one local pipeline environment."""

    runtime_dir: Path
    data_dir: Path
    reports_dir: Path
    duckdb_path: Path
    gx_project_dir: Path
    dagster_home: Path
    fixture_dir: Path

    @property
    def gx_context_dir(self) -> Path:
        return self.gx_project_dir / "gx"

    @property
    def data_docs_dir(self) -> Path:
        return self.gx_context_dir / "uncommitted" / "data_docs" / "local_site"

    @classmethod
    def from_env(cls) -> Settings:
        package_dir = Path(__file__).resolve().parent
        source_project_dir = package_dir.parents[1]
        default_runtime = (
            source_project_dir / ".artifacts"
            if (source_project_dir / "pyproject.toml").exists()
            else Path.cwd() / ".dagster-gx-pipeline"
        )
        runtime_dir = Path(os.getenv("PIPELINE_RUNTIME_DIR", default_runtime)).resolve()
        data_dir = Path(os.getenv("PIPELINE_DATA_DIR", runtime_dir / "data")).resolve()
        reports_dir = Path(os.getenv("PIPELINE_REPORTS_DIR", runtime_dir / "reports")).resolve()
        return cls(
            runtime_dir=runtime_dir,
            data_dir=data_dir,
            reports_dir=reports_dir,
            duckdb_path=Path(
                os.getenv("PIPELINE_DUCKDB_PATH", data_dir / "market_quality.duckdb")
            ).resolve(),
            gx_project_dir=Path(
                os.getenv("PIPELINE_GX_PROJECT_DIR", runtime_dir / "great_expectations")
            ).resolve(),
            dagster_home=Path(os.getenv("DAGSTER_HOME", runtime_dir / "dagster")).resolve(),
            fixture_dir=package_dir / "fixtures",
        )

    def ensure_runtime_dirs(self) -> None:
        for path in (
            self.runtime_dir,
            self.data_dir,
            self.reports_dir,
            self.gx_project_dir,
            self.dagster_home,
        ):
            path.mkdir(parents=True, exist_ok=True)

        dagster_config = self.dagster_home / "dagster.yaml"
        if not dagster_config.exists():
            dagster_config.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")


def get_settings() -> Settings:
    """Resolve settings at call time so tests and deployments can override the environment."""

    return Settings.from_env()
