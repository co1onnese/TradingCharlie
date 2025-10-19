# 🚀 Quick Start Guide - Charlie TR1-DB Pipeline

## ✅ What's Been Implemented

### Core Infrastructure (100%)
- ✅ PostgreSQL database `charlie` with 16 tables, 36 indexes
- ✅ UV-based Python environment with `pyproject.toml`
- ✅ Complete Metaflow pipeline structure (8 steps)
- ✅ Storage abstraction layer (LocalStorage)
- ✅ All 'tauric' references cleaned up

### Real API Integrations (3 of 8)
- ✅ **Yahoo Finance** - OHLCV data (yfinance)
- ✅ **Finnhub** - Company news with retry logic
- ✅ **FRED** - Economic data (fredapi)
- ⚠️ FMP, SimFin, NewsAPI, EODHD - Still stubbed

### Enhanced Features
- ✅ **UPSERT operations** - Idempotent database writes
- ✅ **Technical indicators** - Full `ta` library integration (RSI, MACD, Bollinger, Ichimoku, ATR)
- ✅ **Label generation** - Algorithm S1 fully implemented
- ⚠️ Sample assembly - Needs real modality sampling
- ⚠️ LLM distillation - Returns stub text

---

## 🏃 Running the Pipeline

### 1. Install UV and Dependencies

```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.cargo/env

# Navigate to project
cd /opt/T1

# Create virtual environment
uv venv

# Activate
source .venv/bin/activate

# Install all dependencies
uv pip install -e .
```

### 2. Set API Keys

```bash
# Required for real data
export FINNHUB_API_KEY="your_finnhub_key_here"
export FRED_API_KEY="your_fred_key_here"

# Optional (not yet implemented)
export FMP_API_KEY="your_fmp_key"
export NEWSAPI_KEY="your_newsapi_key"
```

### 3. Run a Test Pipeline

```bash
# Single ticker, single date (recommended for first run)
python charlie_tr1_flow.py run \
  --tickers AAPL \
  --as_of_date 2024-06-15 \
  --variation_count 3 \
  --seed 1234

# The pipeline will:
# 1. Fetch OHLCV data from Yahoo Finance ✅
# 2. Fetch news from Finnhub ✅ (if API key set)
# 3. Fetch macro data from FRED ✅ (if API key set)
# 4. Compute technical indicators (RSI, MACD, etc.) ✅
# 5. Generate 3 prompt variations
# 6. Compute labels using Algorithm S1 ✅
# 7. Export to Parquet
```

### 4. Check Results

```bash
# Check data directory
ls -lah /opt/charlie_data/

# Check database
psql -h localhost -U charlie -d charlie <<EOF
SELECT COUNT(*) FROM charlie.asset;
SELECT COUNT(*) FROM charlie.price_window;
SELECT * FROM charlie.pipeline_run ORDER BY started_at DESC LIMIT 5;
