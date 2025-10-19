#!/bin/bash
# Database initialization script for Charlie-TR1-DB
# This script initializes the PostgreSQL database schema with all required tables, indexes, and optimizationsns

set -e

# Configuration (override with environment variables)
DB_HOST="${CHARLIE_DB_HOST:-localhost}"
DB_PORT="${CHARLIE_DB_PORT:-5432}"
DB_NAME="${CHARLIE_DB_NAME:-charlie}"
DB_USER="${CHARLIE_DB_USER:-charlie}"
DB_PASSWORD="${CHARLIE_DB_PASSWORD:-charliepass}"

echo "================================================"
echo "Charlie-TR1-DB Database Initialization"
echo "================================================"
echo "Host: $DB_HOST:$DB_PORT"
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "================================================"

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "ERROR: psql command not found. Please install PostgreSQL client."
    exit 1
fi

# Test database connection
echo "Testing database connection..."
export PGPASSWORD="$DB_PASSWORD"
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1; then
    echo "ERROR: Could not connect to database. Please check your connection settings."
    exit 1
fi
echo "✓ Database connection successful"

# Initialize schema from DDL file
echo ""
echo "Initializing database schema..."
DDL_FILE="/opt/T1/charlie.ddl"
if [ ! -f "$DDL_FILE" ]; then
    echo "ERROR: DDL file not found at $DDL_FILE"
    exit 1
fi

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$DDL_FILE"
echo "✓ Schema initialized"

# Add additional indexes for performance (from PRD section 14.1)
echo ""
echo "Adding performance indexes..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
-- Additional composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_price_window_asset_date ON charlie.price_window (asset_id, as_of_date);
CREATE INDEX IF NOT EXISTS idx_sample_label_sample ON charlie.sample_label (sample_id);
CREATE INDEX IF NOT EXISTS idx_distilled_thesis_sample ON charlie.distilled_thesis (sample_id);

-- Indexes for insider and analyst tables
CREATE INDEX IF NOT EXISTS idx_insider_txn_asset_date ON charlie.insider_txn (asset_id, filing_date);
CREATE INDEX IF NOT EXISTS idx_analyst_reco_asset_date ON charlie.analyst_reco (asset_id, reco_date);

-- Hash indexes for dedupe_hash lookups (faster for equality checks)
CREATE INDEX IF NOT EXISTS idx_raw_news_dedupe ON charlie.raw_news USING HASH (dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_raw_news_alt_dedupe ON charlie.raw_news_alt USING HASH (dedupe_hash);

-- GIN index for JSONB searches
CREATE INDEX IF NOT EXISTS idx_assembled_sources_meta ON charlie.assembled_sample USING GIN (sources_meta);

-- Partial indexes for active/relevant records
CREATE INDEX IF NOT EXISTS idx_raw_news_relevant ON charlie.raw_news (asset_id, published_at) WHERE is_relevant = true;

EOF
echo "✓ Performance indexes created"

# Create materialized views (from PRD section 14.3)
echo ""
echo "Creating materialized views..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
-- Label distribution summary
DROP MATERIALIZED VIEW IF EXISTS charlie.label_distribution CASCADE;
CREATE MATERIALIZED VIEW charlie.label_distribution AS
SELECT
  a.asset_id,
  ast.ticker,
  a.as_of_date,
  sl.label_class,
  COUNT(*) as count,
  AVG(sl.composite_signal) as avg_signal,
  MIN(sl.composite_signal) as min_signal,
  MAX(sl.composite_signal) as max_signal,
  STDDEV(sl.composite_signal) as stddev_signal
FROM charlie.sample_label sl
JOIN charlie.assembled_sample a ON sl.sample_id = a.sample_id
JOIN charlie.asset ast ON a.asset_id = ast.asset_id
WHERE sl.label_class IS NOT NULL
GROUP BY a.asset_id, ast.ticker, a.as_of_date, sl.label_class;

CREATE INDEX idx_label_dist_asset_date ON charlie.label_distribution (asset_id, as_of_date);

-- Source metadata summary
DROP MATERIALIZED VIEW IF EXISTS charlie.source_summary CASCADE;
CREATE MATERIALIZED VIEW charlie.source_summary AS
SELECT
  a.asset_id,
  ast.ticker,
  a.as_of_date,
  COUNT(DISTINCT a.sample_id) as sample_count,
  AVG(a.prompt_tokens) as avg_tokens,
  (sources_meta->>'news')::text as news_info,
  (sources_meta->>'technicals')::text as technicals_info,
  (sources_meta->>'fundamentals')::text as fundamentals_info
FROM charlie.assembled_sample a
JOIN charlie.asset ast ON a.asset_id = ast.asset_id
GROUP BY a.asset_id, ast.ticker, a.as_of_date, sources_meta;

CREATE INDEX idx_source_summary_asset_date ON charlie.source_summary (asset_id, as_of_date);

-- Pipeline run summary
DROP MATERIALIZED VIEW IF EXISTS charlie.pipeline_run_summary CASCADE;
CREATE MATERIALIZED VIEW charlie.pipeline_run_summary AS
SELECT
  run_id,
  run_name,
  run_type,
  started_at,
  finished_at,
  EXTRACT(EPOCH FROM (finished_at - started_at)) / 60 as duration_minutes,
  status,
  seed,
  (meta->>'tickers')::text as tickers,
  (meta->>'dates')::text as dates
FROM charlie.pipeline_run
ORDER BY started_at DESC;

EOF
echo "✓ Materialized views created"

# Create refresh function for materialized views
echo ""
echo "Creating materialized view refresh function..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
CREATE OR REPLACE FUNCTION charlie.refresh_all_materialized_views()
RETURNS void AS \$\$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY charlie.label_distribution;
  REFRESH MATERIALIZED VIEW CONCURRENTLY charlie.source_summary;
  REFRESH MATERIALIZED VIEW CONCURRENTLY charlie.pipeline_run_summary;
  REFRESH MATERIALIZED VIEW CONCURRENTLY charlie.data_quality_summary;
END;
\$\$ LANGUAGE plpgsql;

COMMENT ON FUNCTION charlie.refresh_all_materialized_views() IS 'Refresh all materialized views concurrently';
EOF
echo "✓ Refresh function created"

# Verify installation
echo ""
echo "Verifying installation..."
TABLE_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'charlie';")
echo "✓ Found $TABLE_COUNT tables in charlie schema"

INDEX_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'charlie';")
echo "✓ Found $INDEX_COUNT indexes"

VIEW_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'charlie';")
echo "✓ Found $VIEW_COUNT materialized views"

echo ""
echo "================================================"
echo "Database initialization completed successfully!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Set environment variables for API keys"
echo "2. Initialize Python environment: uv venv && source .venv/bin/activate"
echo "3. Install dependencies: uv pip install -e ."
echo "4. Run pipeline: python charlie_tr1_flow.py run --tickers AAPL --as_of_date 2024-01-15"
echo ""

unset PGPASSWORD
