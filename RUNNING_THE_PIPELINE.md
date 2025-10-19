# Running the Charlie TR1-DB Pipeline

## Environment Setup Complete!

All dependencies are installed and the pipeline is ready to run.

## Quick Start

### Option 1: Using the test script

```bash
cd /opt/T1
./run_test.sh
```

### Option 2: Manual run

```bash
cd /opt/T1
source .venv/bin/activate

export FINNHUB_API_KEY="d3q932pr01qgab53n6e0d3q932pr01qgab53n6eg"
export FRED_API_KEY="11f51c6532d73a8ca6011d2107a13e6f"
export USERNAME="charlie"

python3 charlie_tr1_flow.py run \
    --tickers AAPL \
    --as_of_date 2024-06-15 \
    --variation_count 3 \
    --seed 1234
```

## What the Pipeline Will Do

1. **start** - Initialize and validate parameters
2. **ingest_raw** - Fetch data from APIs:
   - Yahoo Finance: OHLCV data (15 trading days)
   - Finnhub: Company news (past 30 days)
   - FRED: Economic indicators
3. **normalize_dedupe** - Clean and deduplicate news
4. **compute_technicals** - Calculate RSI, MACD, Bollinger Bands, etc.
5. **assemble_samples** - Create 3 prompt variations
6. **generate_labels** - Compute labels using Algorithm S1
7. **distill_theses** - Generate LLM summaries (currently stubbed)
8. **export_parquet** - Export final dataset

## Expected Output

Data will be stored in:
- `/opt/charlie_data/raw/` - Raw API responses
- `/opt/charlie_data/normalized/` - Cleaned data
- `/opt/charlie_data/assembled/` - Prompt variations
- `/opt/charlie_data/exports/parquet/` - Final datasets

Database records in:
- `charlie.price_window` - OHLCV and technicals
- `charlie.raw_news` - News articles
- `charlie.assembled_sample` - Generated samples
- `charlie.sample_label` - Labels
- `charlie.pipeline_run` - Execution metadata

## Checking Results

```bash
# View pipeline runs
psql -h localhost -U charlie -d charlie <<SQL
SELECT run_name, status, started_at, finished_at 
FROM charlie.pipeline_run 
ORDER BY started_at DESC LIMIT 5;
SQL

# Check data counts
psql -h localhost -U charlie -d charlie <<SQL
SELECT 
  (SELECT COUNT(*) FROM charlie.price_window) as price_records,
  (SELECT COUNT(*) FROM charlie.raw_news) as news_records,
  (SELECT COUNT(*) FROM charlie.assembled_sample) as samples;
SQL

# View technical indicators
psql -h localhost -U charlie -d charlie <<SQL
SELECT as_of_date, 
       technicals->'latest'->>'rsi_14' as rsi,
       technicals->'latest'->>'macd' as macd
FROM charlie.price_window 
WHERE asset_id = (SELECT asset_id FROM charlie.asset WHERE ticker = 'AAPL')
ORDER BY as_of_date DESC LIMIT 10;
SQL
```

## Troubleshooting

### "No data returned"
- The date 2024-06-15 is a Saturday - markets are closed
- Try a weekday: `--as_of_date 2024-06-14` (Friday)

### Rate limits
- Finnhub free tier: 60 calls/minute
- FRED: Usually no limits on free tier
- Yahoo Finance: No API key needed, generally reliable

### Check logs
The pipeline logs to stdout, watch for:
- "Successfully fetched X trading days"
- "Fetched X news items from Finnhub"
- "Stored X OHLCV records"

## Advanced Usage

### Multiple tickers
```bash
python3 charlie_tr1_flow.py run \
    --tickers AAPL,NVDA,MSFT \
    --as_of_date 2024-06-14 \
    --variation_count 5
```

### Date range
```bash
python3 charlie_tr1_flow.py run \
    --tickers AAPL \
    --start_date 2024-06-01 \
    --end_date 2024-06-14 \
    --variation_count 10
```

## What's Working

- ✅ Yahoo Finance OHLCV data
- ✅ Finnhub company news  
- ✅ FRED economic data
- ✅ Technical indicators (ta library)
- ✅ Label generation (Algorithm S1)
- ✅ UPSERT operations (idempotent)
- ✅ Parquet export

## What's Stubbed

- ⚠️ Google News (returns empty)
- ⚠️ FMP fundamentals (returns empty)
- ⚠️ EODHD options (returns empty)
- ⚠️ LLM distillation (returns stub text)

The pipeline will run successfully and fetch real data from Yahoo, Finnhub, and FRED!
