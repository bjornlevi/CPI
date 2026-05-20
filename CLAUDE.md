# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CPI is a Flask web application that visualizes and forecasts economic indices (Consumer Price Index, Wages, Construction Price Index, Production Price Index) for Iceland. It fetches data from Hagstofan (Iceland's statistics bureau) API, stores it in SQLite, performs trend analysis, and displays interactive dashboards.

**Key components:**
- **Frontend:** Flask with Jinja2 templates and static assets (HTML, JavaScript, CSS)
- **Backend:** Flask routes that query SQLite and serve context data to templates
- **Data pipeline:** Jobs that fetch from Hagstofan API, parse, and backfill historical data
- **Forecasting:** Linear regression models to predict future index values
- **Database:** SQLite stored in `cpi_app/data/cpi.sqlite`

## Getting Started

### Setup & Installation

```bash
# Create virtual environment and install dependencies
make install

# Initialize database (one-time fetch from Hagstofan to populate SQLite)
make get_data

# Optional: backfill historical data (e.g., 2005-2025)
python -m cpi_app.jobs.backfill_cpi --start 2005-01 --end 2025-08 --overwrite
python -m cpi_app.jobs.backfill_wages --start 2005-01 --end 2025-08 --overwrite
```

### Running the Application

```bash
# Development mode (Flask debug server at http://127.0.0.1:5000)
make dev-start
make dev-status
make dev-stop
make dev-restart

# Or run in foreground
make web

# Data fetching (runs daily via cron in production)
./run_cpi.sh
python -m cpi_app.jobs.fetch_all
```

### Production Deployment

```bash
# Uses gunicorn via wsgi.py
FLASK_APP=cpi_app.app:create_app gunicorn --bind 0.0.0.0:5000 wsgi:app
```

## Architecture

### Directory Structure

```
cpi_app/
├── app.py              # Flask app initialization, routes, and context builders
├── models.py           # SQLAlchemy ORM models (CPI, Wages, BCI, PPI tables)
├── jobs/               # Data ingestion scripts
│   ├── fetch_all.py    # Main job to fetch latest data from Hagstofan
│   ├── backfill_cpi.py
│   ├── backfill_wages.py
│   └── backfill_ppi_bci.py
├── pipelines/          # Data transformation and analysis
│   ├── cpi.py          # CPI parsing, trend fitting, sub-series handling
│   ├── wages.py
│   ├── bci.py
│   └── ppi.py
├── scripts/
│   └── Hagstofan/      # API client library for Hagstofan integration
├── templates/          # Jinja2 HTML templates (index, cpi, wages, bci, ppi, sub)
├── static/             # CSS, JavaScript, and static assets
└── data/               # SQLite database directory (cpi.sqlite)
```

### Data Models

SQLAlchemy models in `models.py`:

- **CPI tables:** `CPIActual` (historical data), `ForecastRun`, `ForecastPoint`, `CPISubMetric` (detailed category-level metrics)
- **Wages tables:** `WageActual`, `WageForecastRun`, `WageForecastPoint`
- **BCI (Construction) tables:** `BCIActual`, `BCIForecastRun`, `BCIForecastPoint`
- **PPI (Production) tables:** `PPIActual`, `PPIForecastRun`, `PPIForecastPoint`

Each index has three core tables:
1. **Actual** — historical values fetched from Hagstofan (date-indexed, unique constraint)
2. **ForecastRun** — metadata about a forecast batch (timestamp, horizon, notes)
3. **ForecastPoint** — individual predicted values (foreign key to run)

### Routes

Defined in `app.py:create_app()`:

- `GET /` — Homepage (24-month CPI + latest forecasts)
- `GET /cpi` — Full CPI dashboard (all history, forecasts, sub-categories, top movers)
- `GET /wages` — Wage index dashboard
- `GET /bci` — Construction price index dashboard
- `GET /ppi` — Production price index dashboard
- `GET /sub` — Detailed sub-metric explorer (curated + top-mover categories)
- `GET /health` — Health check endpoint

### Key Functions in app.py

- `_cpi_context()` — Builds context for CPI page: full history, latest forecast, sub-series (curated + top movers), change tables
- `_wages_context()` — Builds wage context with category selection
- `_bci_context()` — Builds BCI context
- `_ppi_context()` — Builds PPI context
- `_structured_change_table()` — Computes historic/current/projected MoM and YoY change statistics
- `build_cpi_subseries()` — Handles range selection for sub-metric drilling

### Data Flow

1. **Fetch:** `fetch_all.py` calls Hagstofan API via `Hagstofan/api_client.py` → raw JSON
2. **Parse:** Pipeline modules (cpi.py, wages.py, etc.) parse and transform into DataFrames
3. **Store:** Data inserted/updated in SQLite tables (one record per date/category/code)
4. **Forecast:** Linear regression models fit on recent data → future predictions stored in ForecastRun/Point tables
5. **Serve:** Routes query DB, compute statistics, and render templates with context

### Environment Variables

- `CPI_DB` — Path to SQLite database (default: `cpi_app/data/cpi.sqlite`)
- `CPI_CURATED_CODES` — Comma-separated list of ISNR codes to always show (default: `IS011,IS041,IS042,IS0451,IS0455,IS06,IS0722,IS111`)
- `FLASK_APP` — Set to `cpi_app.app:create_app` for Flask CLI
- Standard Flask env vars: `FLASK_ENV`, `FLASK_DEBUG`, `FLASK_RUN_HOST`, `FLASK_RUN_PORT`

### External Data Source

**Hagstofan API** (Statistics Iceland)
- Endpoint: `https://px.hagstofa.is:443/pxis/api/v1/is/`
- Provides: CPI (monthly), Wages (quarterly), BCI/PPI (monthly)
- Format: Hagstofan PX API (custom JSON structure)
- Client: `cpi_app/scripts/Hagstofan/api_client.py` handles authentication and parsing

## Common Tasks

### Add a new economic index

1. Create a new ORM model in `models.py` (Actual, ForecastRun, ForecastPoint tables)
2. Add a fetch/parse pipeline in `cpi_app/pipelines/<name>.py`
3. Add a backfill job in `cpi_app/jobs/backfill_<name>.py`
4. Import and integrate into `fetch_all.py`
5. Create context builder in `app.py` (e.g., `_<name>_context()`)
6. Add route and template

### Update forecast horizon

Change `FORECAST_MONTHS` in `app.py` (currently 6). Ensure all jobs produce the same horizon.

### Adjust CPI sub-metrics shown

Edit `CURATED_ISNR` in `app.py` (ISNR codes) or modify the "top movers" selection logic in `_cpi_context()`.

### Investigate data anomalies

Query the database directly:
```bash
sqlite3 cpi_app/data/cpi.sqlite
SELECT date, cpi, monthly_change FROM cpi_actuals ORDER BY date DESC LIMIT 12;
SELECT date, code, value, mom, yoy FROM cpi_sub_metrics WHERE date = '2025-08-01' ORDER BY code;
```

## Testing & Debugging

- **Logs:** Check `logs/cpi.log` for fetch/backfill output
- **Database:** SQLite file is at `cpi_app/data/cpi.sqlite` (use `sqlite3` CLI to inspect)
- **Flask debug:** Set `FLASK_DEBUG=1` and use `make dev-start` for live reload
- **API debugging:** The `Hagstofan` module will log request details if you add print statements

## Notes

- All dates stored as `Date` (not `DateTime`)
- Forecasts are "future-only" — ForecastPoint table contains no overlapping dates with CPIActual
- Monthly aggregation: data always normalized to month-start (e.g., 2025-08-01 for August 2025)
- MoM = month-over-month change from prior month; YoY = year-over-year from same month 12 months prior
- Sub-metrics (CPISubMetric) computed alongside main CPI in the fetch job and stored in DB
