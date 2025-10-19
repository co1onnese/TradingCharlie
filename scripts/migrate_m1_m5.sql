-- Migration script for M1-M5 enhancements
-- Adds new columns and tables to existing Charlie schema

\echo 'Starting migration for M1-M5 enhancements...'

-- M1: Add new columns to raw_news
ALTER TABLE charlie.raw_news
ADD COLUMN IF NOT EXISTS content_hash TEXT,
ADD COLUMN IF NOT EXISTS request_meta JSONB;

\echo '✓ Updated raw_news table'

-- M1: Add new columns to raw_news_alt
ALTER TABLE charlie.raw_news_alt
ADD COLUMN IF NOT EXISTS content_hash TEXT,
ADD COLUMN IF NOT EXISTS request_meta JSONB;

\echo '✓ Updated raw_news_alt table'

-- M1: Add window_days column to price_window
ALTER TABLE charlie.price_window
ADD COLUMN IF NOT EXISTS window_days SMALLINT;

\echo '✓ Updated price_window table'

-- M2: Add new columns to assembled_sample
ALTER TABLE charlie.assembled_sample
ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES charlie.pipeline_run(run_id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS as_of_cutoff TIMESTAMPTZ;

\echo '✓ Updated assembled_sample table'

-- M1: Create normalized_news table
CREATE TABLE IF NOT EXISTS charlie.normalized_news (
  id                BIGSERIAL PRIMARY KEY,
  asset_id          BIGINT REFERENCES charlie.asset(asset_id) ON DELETE SET NULL,
  published_at_utc  TIMESTAMP WITH TIME ZONE NOT NULL,
  source            TEXT NOT NULL,
  headline          TEXT,
  snippet           TEXT,
  url               TEXT,
  tokens_count      INTEGER,
  bucket            TEXT,                       -- '0-3', '4-10', '11-30'
  lang              TEXT,
  is_relevant       BOOLEAN DEFAULT NULL,
  raw_news_id       BIGINT REFERENCES charlie.raw_news(news_id) ON DELETE SET NULL,
  raw_news_alt_id   BIGINT REFERENCES charlie.raw_news_alt(alt_news_id) ON DELETE SET NULL,
  content_hash      TEXT NOT NULL UNIQUE,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_normalized_news_asset_date ON charlie.normalized_news (asset_id, published_at_utc);
CREATE INDEX IF NOT EXISTS idx_normalized_news_bucket ON charlie.normalized_news (asset_id, bucket);
CREATE INDEX IF NOT EXISTS idx_normalized_news_hash ON charlie.normalized_news (content_hash);
CREATE INDEX IF NOT EXISTS idx_normalized_news_fulltext ON charlie.normalized_news USING GIN (to_tsvector('english', COALESCE(headline, '') || ' ' || COALESCE(snippet, '')));

\echo '✓ Created normalized_news table'

-- M1: Create data_quality_summary materialized view
DROP MATERIALIZED VIEW IF EXISTS charlie.data_quality_summary CASCADE;
CREATE MATERIALIZED VIEW charlie.data_quality_summary AS
SELECT
  CURRENT_DATE as report_date,

  -- News deduplication stats
  (SELECT COUNT(*) FROM charlie.raw_news) as raw_news_total,
  (SELECT COUNT(*) FROM charlie.raw_news_alt) as raw_news_alt_total,
  (SELECT COUNT(*) FROM charlie.normalized_news) as normalized_news_total,
  (SELECT COUNT(DISTINCT content_hash) FROM charlie.normalized_news) as unique_content_hashes,

  -- Bucketing stats
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE bucket = '0-3') as bucket_0_3_count,
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE bucket = '4-10') as bucket_4_10_count,
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE bucket = '11-30') as bucket_11_30_count,

  -- Relevance filtering
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE is_relevant = true) as relevant_count,
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE is_relevant = false) as irrelevant_count,
  (SELECT COUNT(*) FROM charlie.normalized_news WHERE is_relevant IS NULL) as unfiltered_count,

  -- Audit log stats
  (SELECT COUNT(*) FROM charlie.audit_log WHERE event_type = 'dedup_skip') as dedup_events,
  (SELECT COUNT(*) FROM charlie.audit_log WHERE event_type = 'quality_fail') as quality_fail_events,
  (SELECT COUNT(*) FROM charlie.audit_log WHERE event_type = 'rate_limit') as rate_limit_events
;

CREATE UNIQUE INDEX idx_data_quality_summary_date ON charlie.data_quality_summary (report_date);

\echo '✓ Created data_quality_summary materialized view'

\echo 'Migration completed successfully!'
