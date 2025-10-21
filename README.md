# Charlie-TR1-DB: Multi-Modal Financial Forecasting Dataset Pipeline

A reproducible, end-to-end data pipeline for building multi-modal financial forecasting datasets using Metaflow for orchestration, PostgreSQL for structured metadata, and local filesystem storage for artifacts.

## 📋 Overview

This pipeline assembles financial market data from multiple sources into training-ready datasets for machine learning models. The system combines:

- **News sources**: Finnhub, Google News, NewsAPI, GDELT
- **Market data**: Yahoo Finance (OHLCV), EODHD options
- **Fundamentals**: Financial Modeling Prep (FMP), SimFin
- **Macro data**: FRED economic series, EODHD economic events
- **Technical indicators**: RSI, MACD, EMA, ATR, Bollinger Bands, Ichimoku
- **Label generation**: Volatility-adjusted composite signals (Algorithm S1)
- **LLM distillation**: Optional thesis generation for samples

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL 12+
- [UV](https://github.com/astral-sh/uv) package manager
- API keys for data sources (see Configuration section)

### Installation

```bash
# Clone or navigate to the project directory
cd /opt/T1

# Create UV virtual environment
uv venv

# Activate the environment
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv pip install -e .
```

### Database Setup

The database has already been initialized with the `charlie` schema. If you need to reinitialize:

```bash
# The database is ready with:
# - Database: charlie
# - User: charlie
# - Password: charliepass
# - Schema: charlie (with all tables, indexes, and views)

# To verify:
psql -h localhost -U charlie -d charlie -c "SELECT COUNT(*) FROM charlie.asset;"
```

### Configuration

#### Option 1: Using .env file (Recommended)

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual API keys:
   ```bash
   nano .env
   ```

3. Load environment variables:
   ```bash
   source load_env.sh
   ```

#### Option 2: Manual export

Set the following environment variables for API access:

```bash
# Data storage
export CHARLIE_DATA_ROOT="/opt/charlie_data"

# Database (defaults are already set)
export CHARLIE_DB_URL="postgresql+psycopg2://charlie:charliepass@localhost:5432/charlie"

# API Keys (required for real data fetching)
export FINNHUB_API_KEY="your_finnhub_key"
export FMP_API_KEY="your_fmp_key"
export EODHD_API_KEY="your_eodhd_key"
export FRED_API_KEY="your_fred_key"
export NEWSAPI_KEY="your_newsapi_key"
export SIMFIN_API_KEY="your_simfin_key"

# Optional: LLM for thesis distillation
export CHARLIE_LLM_PROVIDER="openai"
export CHARLIE_LLM_API_KEY="your_openai_key"
export CHARLIE_LLM_MODEL="gpt-4o-mini"
```

#### API Key Sources

- **Finnhub**: https://finnhub.io/register (Free: 60 calls/minute)
- **FRED**: https://fred.stlouisfed.org/docs/api/api_key.html (Free: Usually no limits)
- **FMP**: https://financialmodelingprep.com/developer/docs (Free: 250 calls/day)
- **EODHD**: https://eodhd.com/ (Free: 20 calls/day)
- **NewsAPI**: https://newsapi.org/register (Free: 1000 requests/day)
- **SimFin**: https://simfin.com/ (Free: Limited access)
- **OpenAI**: https://platform.openai.com/api-keys (Paid service)

### Running the Pipeline

```bash
# Single date run for one ticker
python charlie_tr1_flow.py run \
  --tickers AAPL \
  --as_of_date 2024-01-15 \
  --variation_count 20

# Multiple tickers over date range
python charlie_tr1_flow.py run \
  --tickers AAPL,NVDA,MSFT \
  --start_date 2024-01-01 \
  --end_date 2024-12-31 \
  --variation_count 20 \
  --seed 1234
```

## 📁 Project Structure

```
/opt/T1/
├── charlie_tr1_flow.py      # Main Metaflow pipeline
├── charlie.ddl               # Database schema definition
├── pyproject.toml            # UV project configuration
├── .python-version           # Python version for UV
├── README.md                 # This file
├── scripts/
│   └── init_db.sh           # Database initialization script
└── example_sources_meta.json # Example metadata format

/opt/charlie_data/            # Data storage root (created on first run)
├── raw/                      # Raw API responses
│   ├── finnhub/
│   ├── google_news/
│   ├── yahoo_price/
│   ├── fmp/
│   └── eodhd_options/
├── normalized/               # Cleaned and deduplicated data
│   ├── news/
│   ├── price_window/
│   └── ...
├── assembled/                # Prompt variations per (ticker, date)
├── labels/                   # Computed labels (Algorithm S1)
├── distilled_theses/         # LLM-generated reasoning
└── exports/
    └── parquet/              # Final dataset exports
```

## 🏗️ Pipeline Architecture

The Metaflow pipeline consists of the following steps:

### 1. **start**
- Validates parameters
- Creates storage directories
- Initializes pipeline_run database record
- Parallelizes by ticker

### 2. **ingest_raw**
- Fetches data from all configured APIs
- Stores raw JSON files
- Inserts metadata into PostgreSQL
- Implements rate limiting and retry logic

### 3. **normalize_dedupe**
- Deduplicates news articles by hash
- Normalizes timestamps to UTC
- Buckets news by temporal distance (0-3, 4-10, 11-30 days)
- Estimates token counts

### 4. **compute_technicals**
- Computes technical indicators using `ta` library
- Generates 15-day OHLCV windows
- Stores indicators in price_window table

### 5. **assemble_samples**
- Creates N variations per (ticker, date)
- Samples from available modalities
- Assembles prompts within token budget
- Records sources_meta for provenance

### 6. **generate_labels**
- Implements Algorithm S1 from research paper
- Computes forward returns at τ ∈ {3, 7, 15} days
- Normalizes by rolling volatility
- Quantizes into 5 classes

### 7. **distill_theses**
- Optional LLM distillation step
- Batch processes prompts
- Generates reasoning/thesis text
- Stores results with source model info

### 8. **export_parquet**
- Joins assembled_sample + sample_label + distilled_thesis
- Exports to Parquet format
- Partitions by ticker

## 📊 Database Schema

### Core Tables
- `asset` - Ticker symbols and company metadata
- `raw_news` - Primary news sources (Finnhub, Google News)
- `raw_news_alt` - Alternative news (NewsAPI, GDELT, Webz)
- `price_window` - OHLCV data with technical indicators
- `raw_fmp_fundamentals` - FMP financial statements
- `raw_eodhd_options` - Options chain data
- `raw_eodhd_economic_events` - Economic calendar
- `insider_txn` - Insider trading transactions
- `analyst_reco` - Analyst recommendations
- `macro_series` - FRED economic series
- `assembled_sample` - Generated prompt variations
- `sample_label` - Computed labels (Algorithm S1)
- `distilled_thesis` - LLM-generated reasoning
- `pipeline_run` - Pipeline execution metadata
- `audit_log` - System audit trail

### Materialized Views
- `label_distribution` - Label statistics by asset/date
- `source_summary` - Data source coverage summary
- `pipeline_run_summary` - Pipeline execution history

Refresh views with:
```sql
SELECT charlie.refresh_all_materialized_views();
```

## 🔍 Querying the Data

```sql
-- Check pipeline runs
SELECT * FROM charlie.pipeline_run_summary ORDER BY started_at DESC LIMIT 10;

-- View assembled samples for a ticker
SELECT sample_id, as_of_date, variation_id, prompt_tokens
FROM charlie.assembled_sample a
JOIN charlie.asset ast ON a.asset_id = ast.asset_id
WHERE ast.ticker = 'AAPL'
ORDER BY as_of_date DESC, variation_id
LIMIT 20;

-- Label distribution
SELECT * FROM charlie.label_distribution
WHERE ticker = 'AAPL'
ORDER BY as_of_date DESC, label_class;

-- Check data coverage
SELECT * FROM charlie.source_summary
WHERE ticker IN ('AAPL', 'NVDA')
ORDER BY as_of_date DESC;
```

## 🧪 Testing

```bash
# Test database connection
psql -h localhost -U charlie -d charlie -c "SELECT COUNT(*) FROM charlie.asset;"

# Test single-ticker pipeline (when APIs are implemented)
python charlie_tr1_flow.py run \
  --tickers AAPL \
  --as_of_date 2024-06-01 \
  --variation_count 5

# Verify output
ls -lah /opt/charlie_data/exports/parquet/AAPL/
```

## 📝 Data Provenance

Every assembled sample includes `sources_meta` JSON tracking:
```json
{
  "news": {"count": 10, "sources": ["finnhub", "google_news"]},
  "news_alt": {"count": 4, "sources": ["gdelt", "newsapi"]},
  "technicals": {"window_days": 15, "indicators": ["RSI", "MACD", "EMA", "ATR"]},
  "fundamentals": {"sources": ["simfin", "fmp"], "latest_report": "2024-12-31"},
  "macro": {"sources": ["fred", "eodhd"], "series_included": ["CPI", "GDP", "FEDFUNDS"]},
  "insider": {"transactions": 12, "mspr": 0.34},
  "analyst": {"count": 8, "mean_rating": "Buy"},
  "options": {"included": true, "records": 20, "avg_iv": 0.28},
  "distillation": {"included": true, "model": "gpt-4o-mini"}
}
```

## 🚀 Future Enhancements

- Implement S3Storage backend for cloud storage
- Deploy on AWS Batch with Metaflow
- Add real-time ingestion pipeline
- Support incremental updates
- Integrate with model training workflows
- Add monitoring and alerting
- Implement automated backups

## 📄 License

[Specify your license]

## 👥 Contributors

[List contributors]

## 📚 References

- Product Requirements Document: `/opt/T1/data_flow_design.md`
- Database Schema: `/opt/T1/charlie.ddl`
- Example Metadata: `/opt/T1/example_sources_meta.json`