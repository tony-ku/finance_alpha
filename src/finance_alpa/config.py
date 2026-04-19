"""Configuration loading: .env (secrets) + config.yaml (user data)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_PORTFOLIO_NAME = "Sample Portfolio"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "finance.duckdb"
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
RAW_EMAILS_DIR = DATA_DIR / "raw_emails"


class Settings(BaseSettings):
    """Secrets loaded from .env — all optional for Phase 1."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    imap_host: str = ""
    imap_port: int = 1143
    imap_use_ssl: bool = False
    imap_user: str = ""
    imap_app_password: str = ""

    fmp_api_key: str = ""
    finnhub_api_key: str = ""

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""


class Position(BaseModel):
    symbol: str
    shares: float
    cost_basis: float | None = None
    account: str | None = None

    @field_validator("symbol")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.strip().upper()


class Universe(BaseModel):
    watchlist: list[str] = Field(default_factory=list)

    @field_validator("watchlist")
    @classmethod
    def _upper_all(cls, v: list[str]) -> list[str]:
        return [s.strip().upper() for s in v if s and s.strip()]


class Feed(BaseModel):
    name: str
    url: str


class AlertRule(BaseModel):
    """User-defined alert rule.

    Supported `type` values:
      - upcoming_earnings  (days, scope)
      - fmp_rating_change  (scope)
      - reco_change        (scope, direction, optional threshold)
      - price_change       (scope, threshold_pct, direction)

    `scope` is one of: all, portfolio, watchlist.
    `notify` channels: desktop, email.
    """

    name: str
    type: str
    scope: str = "all"
    notify: list[str] = Field(default_factory=lambda: ["desktop"])
    days: int | None = None
    threshold_pct: float | None = None
    direction: str = "any"


class Portfolio(BaseModel):
    name: str
    positions: list[Position] = Field(default_factory=list)


class AppConfig(BaseModel):
    universe: Universe = Field(default_factory=Universe)
    portfolios: list[Portfolio] = Field(default_factory=list)
    sa_rss_feeds: list[Feed] = Field(default_factory=list)
    alerts: list[AlertRule] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _accept_legacy_portfolio_key(cls, data):
        """Accept the old flat `portfolio: [ ... ]` form by wrapping it in a
        single "Sample Portfolio" under the new `portfolios:` key."""
        if isinstance(data, dict) and "portfolio" in data and "portfolios" not in data:
            data = dict(data)
            data["portfolios"] = [
                {"name": DEFAULT_PORTFOLIO_NAME, "positions": data.pop("portfolio") or []}
            ]
        return data

    @property
    def all_positions(self) -> list[Position]:
        out: list[Position] = []
        for pf in self.portfolios:
            out.extend(pf.positions)
        return out

    def all_symbols(self) -> list[str]:
        seen: dict[str, None] = {}
        for s in self.universe.watchlist:
            seen[s] = None
        for p in self.all_positions:
            seen[p.symbol] = None
        return list(seen.keys())


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def load_app_config() -> AppConfig:
    if not CONFIG_PATH.exists():
        return AppConfig()
    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return AppConfig.model_validate(data)
