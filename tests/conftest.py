"""Shared pytest fixtures.

`tmp_db` — redirects the `db` module's path constants at a temp location so
each test gets a fresh DuckDB file with no cross-test contamination.

Also clears the `load_app_config` lru_cache around every test so fixtures
that swap config.yaml content don't leak through to subsequent tests.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from finance_alpa import config as config_module
from finance_alpa import db as db_module


@pytest.fixture(autouse=True)
def _clear_config_cache():
    config_module.load_app_config.cache_clear()
    config_module.get_settings.cache_clear()
    yield
    config_module.load_app_config.cache_clear()
    config_module.get_settings.cache_clear()


@pytest.fixture
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data"
    db_path = data_dir / "test.duckdb"
    raw_dir = data_dir / "raw_emails"
    monkeypatch.setattr(db_module, "DB_PATH", db_path)
    monkeypatch.setattr(db_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(db_module, "RAW_EMAILS_DIR", raw_dir)
    return db_path
