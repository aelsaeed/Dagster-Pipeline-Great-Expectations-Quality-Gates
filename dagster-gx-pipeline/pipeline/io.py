from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import duckdb

from pipeline.settings import settings


@contextmanager
def duckdb_conn(path: Path = settings.duckdb_path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        yield conn
    finally:
        conn.close()
