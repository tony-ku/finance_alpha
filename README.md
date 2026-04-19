# finance_alpa

A local financial workspace for tracking portfolios, screening stocks, reading market news, and getting alerts on threshold events. Runs entirely on your machine — **Python + Streamlit + DuckDB**, no cloud, no account required.

Data comes from Yahoo Finance, Seeking Alpha public RSS, Financial Modeling Prep, and Finnhub — all accessed through official APIs or public feeds. No scraping, no reseller endpoints.

## Features

- **US market conditions** on the home page — S&P 500, Dow 30, Nasdaq, Russell 2000, VIX, cached 60s.
- **Symbol lookup** — enter any ticker for a candlestick chart (1D · 5D · 1M · 3M · 6M · 1Y · 3Y · 5Y · 10Y · MAX), fundamentals, business summary, and any locally-stored articles that mention it.
- **Portfolio** — positions stored locally in DuckDB. Import from a broker CSV (Schwab, Fidelity, Vanguard, IBKR Flex, Robinhood auto-detected) or seed from `config.yaml`. Shows latest prices, per-position and total P/L.
- **Screener** — composite z-score ranking across FMP rating score, 3-month price momentum, and Finnhub analyst recommendation mean. Weights are user-tunable in the sidebar. CSV export.
- **News** — aggregated feed from SA public RSS and Finnhub company news. Filter by ticker, source, lookback; mark-as-read, hide-read, deduped by URL.
- **Alerts** — four rule types (`price_change`, `upcoming_earnings`, `fmp_rating_change`, `reco_change`). Fires to desktop notifications (plyer) and optional SMTP email. Dedup via payload keys; full audit log on the Alerts page.
- **Embedded scheduler** — APScheduler runs ingest jobs and the alerts evaluator in the same Streamlit process. No second terminal, no cron setup.

## Quickstart

Requires Python 3.11+.

```bash
git clone https://github.com/<you>/finance_alpa.git
cd finance_alpa

python -m venv .venv
source .venv/Scripts/activate          # Windows bash
# source .venv/bin/activate            # macOS / Linux
pip install -e .

cp .env.example .env                    # fill in any API keys (all optional)
# edit config.yaml with your tickers + portfolio

streamlit run src/finance_alpa/ui/app.py
```

Open http://localhost:8501. The embedded scheduler starts on first page load and kicks off an immediate ingest pass so data is fresh within seconds.

## Configuration

### `config.yaml`

```yaml
universe:
  watchlist: [SPY, QQQ, AAPL, MSFT, NVDA]

portfolio:
  - symbol: AAPL
    shares: 10
    cost_basis: 150.00
    account: Taxable

sa_rss_feeds:
  - name: SA Latest Articles
    url: https://seekingalpha.com/feed.xml

alerts:
  - name: Big daily move
    type: price_change
    scope: watchlist
    threshold_pct: 5.0
    direction: any
    notify: [desktop]
```

Alert rule schema:

| field | values |
|---|---|
| `type` | `price_change` · `upcoming_earnings` · `fmp_rating_change` · `reco_change` |
| `scope` | `all` · `portfolio` · `watchlist` |
| `notify` | list of `desktop`, `email` |
| `threshold_pct` | float — used by `price_change` |
| `days` | int — used by `upcoming_earnings` |
| `direction` | `up` · `down` · `any` — used by `price_change`, `reco_change` |

### `.env`

| Variable | Purpose |
|---|---|
| `FMP_API_KEY` | Financial Modeling Prep free tier (250 req/day). Unlocks FMP ratings, analyst estimates, earnings calendar, TTM fundamentals. |
| `FINNHUB_API_KEY` | Finnhub free tier (60 req/min). Unlocks company news + recommendation trends. |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` | Optional, for email alerts. |

Every ingest module no-ops gracefully when its key is absent — the app is fully usable with only Yahoo Finance + SA public RSS.

## Data sources

| Source | Used for | Access |
|---|---|---|
| [yfinance](https://github.com/ranaroussi/yfinance) | Daily OHLCV, index quotes, info/fundamentals | Unofficial Yahoo API — free, no key |
| Seeking Alpha public RSS | Headlines + ticker tags | Public RSS feeds |
| [Financial Modeling Prep](https://site.financialmodelingprep.com/) | Ratings, analyst estimates, earnings calendar, TTM metrics | API key |
| [Finnhub](https://finnhub.io/) | Company news, analyst recommendation trends | API key |

All access is through official APIs or public feeds. The project does **not** scrape seekingalpha.com, nor does it use third-party endpoints that resell scraped SA data.

## Architecture

```
       ┌─────────────────────┐          ┌─────────────────────────────┐
       │   Streamlit UI       │          │   Embedded APScheduler       │
       │   (read-only reader) │◀─reads──▶│   (sole writer)              │
       └─────────────────────┘          └──────────────┬──────────────┘
                   │                                    │
                   ▼                                    ▼
            ┌──────────────┐              ┌─────────────────────────────┐
            │   DuckDB     │◀─writes──────│   Ingest jobs                │
            │ data/*.duckdb│              │  · yfinance  (daily 17:00)   │
            └──────────────┘              │  · sa_rss    (every 30m)     │
                                          │  · fmp       (daily 17:30)   │
                                          │  · finnhub   (every 30m)     │
                                          │  · alerts    (every 5m)      │
                                          └─────────────────────────────┘
```

- DuckDB is the only persistence layer. Schema is created idempotently in [`src/finance_alpa/db.py`](src/finance_alpa/db.py) via `CREATE TABLE IF NOT EXISTS`.
- The scheduler runs in the same process as Streamlit by default; one terminal is all you need. For a headless host, run `python -m finance_alpa.scheduler` (blocking mode).
- All timestamps stored as naive UTC. Schedules are in America/New_York.

## Code layout

```
src/finance_alpa/
├── config.py          # pydantic-settings + YAML loader
├── db.py              # DuckDB connection + schema
├── scheduler.py       # APScheduler (embedded + blocking modes)
├── ingest/
│   ├── yfinance_quotes.py
│   ├── sa_rss.py
│   ├── fmp.py
│   └── finnhub_news.py
├── screeners/
│   └── rank.py        # composite z-score ranking
├── alerts/
│   ├── notify.py      # desktop + SMTP
│   └── rules.py       # rule evaluator
└── ui/
    ├── app.py                    # home: market, symbol lookup, status
    ├── _theme.py                 # CSS + bootstrap
    └── pages/
        ├── 1_Portfolio.py
        ├── 2_Screener.py
        ├── 3_News.py
        └── 4_Alerts.py
```

## Roadmap

- [ ] **IMAP + SA Premium email ingestion** — pull Wall Street Breakfast, stock ideas, Quant Rating alerts, and earnings previews from the user's inbox. Planned as template-specific BeautifulSoup parsers with golden-file tests for regression safety. Will support multiple providers (Gmail / Outlook / ProtonMail Bridge).
- [ ] **Screener backtesting** — walk-forward evaluation of composite weights against accumulated historical ratings + forward returns once enough snapshots are collected.
- [ ] **Ticker deep-linking** — click a symbol in Portfolio/Screener/News to jump to the home-page lookup with that ticker preselected.
- [ ] **Portfolio editor in UI** — add/remove positions without hand-editing `config.yaml`.
- [ ] **More sources** — Benzinga news, Tiingo fundamentals, optional Polygon integration.

## Disclaimer

This project is for **personal research and educational use** only. It is **not** financial, investment, tax, or legal advice. Nothing shown in the app constitutes a recommendation to buy, sell, or hold any security. Market data is provided by third parties and may be delayed, incomplete, or inaccurate — always verify critical numbers against primary sources (broker statements, issuer filings, exchange data). Past performance does not predict future results. You are solely responsible for your own investment decisions and for complying with the Terms of Service of every data provider used.

The authors and contributors provide this software AS IS and disclaim all warranties. See `LICENSE` for the full MIT disclaimer of warranty and limitation of liability.

## License

MIT License — see [`LICENSE`](LICENSE).

Copyright © 2026 Tony Ku.
