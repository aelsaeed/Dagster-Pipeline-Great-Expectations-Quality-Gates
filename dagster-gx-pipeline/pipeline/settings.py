from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_root: Path = Path(__file__).resolve().parents[1]
    data_dir: Path = Path(os.getenv("DATA_DIR", project_root / "data"))
    reports_dir: Path = Path(os.getenv("REPORTS_DIR", project_root / "reports"))
    expectations_dir: Path = Path(
        os.getenv("EXPECTATIONS_DIR", project_root / "expectations")
    )
    duckdb_path: Path = Path(os.getenv("DUCKDB_PATH", data_dir / "pipeline.duckdb"))
    sample_api_payload: Path = data_dir / "sample_api_payload.json"
    data_docs_dir: Path = reports_dir / "data_docs"


settings = Settings()
