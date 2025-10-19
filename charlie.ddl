BEGIN;

-- ==========================================================
-- SCHEMA
-- ==========================================================
CREATE SCHEMA IF NOT EXISTS charlie;
SET search_path = charlie, public;

-- ==========================================================
-- CORE TABLES
-- ==========================================================
CREATE TABLE IF NOT EXISTS asset (
  asset_id       BIGSERIAL PRIMARY KEY,
  ticker         TEXT NOT NULL UNIQUE,
  name           TEXT,
  sector         TEXT,
  market_cap     NUMERIC,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ==========================================================
-- NEWS SOURCES
-- ==========================================================

-- 1. Primary News (Finnhub / Google News)
CREATE TABLE IF NOT EXISTS raw_news (
  news_id       BIGSERIAL PRIMARY KEY,
  asset_id      BIGINT REFERENCES asset(asset_id) ON DELETE SET NULL,
  source        TEXT NOT NULL,              -- 'finnhub', 'google_news'
  headline      TEXT,
  snippet       TEXT,
  url           TEXT,
  published_at  TIMESTAMP WITH TIME ZONE,
  fetched_at    TIMESTAMP WITH TIME ZONE DEFAULT now(),
  raw_json      JSONB,
  dedupe_hash   TEXT UNIQUE,                -- UNIQUE constraint for ON CONFLICT upserts
  is_relevant   BOOLEAN DEFAULT NULL,
  bucket        TEXT,                       -- '0-3', '4-10', '11-30'
  tokens_count  INTEGER,
  file_path     TEXT,                       -- local filesystem path to raw JSON
  content_hash  TEXT,                       -- sha256(headline+url+published_at) for cross-source dedup
  request_meta  JSONB,                      -- {endpoint, params, status, latency}
  created_at    TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_news_asset_date ON raw_news (asset_id, published_at);
CREATE INDEX IF NOT EXISTS idx_raw_news_content_hash ON raw_news (content_hash) WHERE content_hash IS NOT NULL;

-- 2. Alternative News (NewsAPI, Webz.io, GDELT)
CREATE TABLE IF NOT EXISTS raw_news_alt (
  alt_news_id   BIGSERIAL PRIMARY KEY,
  asset_id      BIGINT REFERENCES asset(asset_id) ON DELETE SET NULL,
  source        TEXT NOT NULL,              -- 'newsapi', 'webz', 'gdelt'
  headline      TEXT,
  snippet       TEXT,
  language      TEXT,
  region        TEXT,
  url           TEXT,
  published_at  TIMESTAMP WITH TIME ZONE,
  fetched_at    TIMESTAMP WITH TIME ZONE DEFAULT now(),
  sentiment     JSONB,                      -- optional: {polarity, subjectivity}
  raw_json      JSONB,
  dedupe_hash   TEXT UNIQUE,                -- UNIQUE constraint for ON CONFLICT upserts
  is_relevant   BOOLEAN DEFAULT NULL,
  bucket        TEXT,                       -- temporal bucket (same as core news)
  tokens_count  INTEGER,
  file_path     TEXT,
  content_hash  TEXT,                       -- sha256(headline+url+published_at) for cross-source dedup
  request_meta  JSONB,                      -- {endpoint, params, status, latency}
  created_at    TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_news_alt_asset_date ON raw_news_alt (asset_id, published_at);
CREATE INDEX IF NOT EXISTS idx_raw_news_alt_content_hash ON raw_news_alt (content_hash) WHERE content_hash IS NOT NULL;

-- 3. Normalized News (deduplicated and enriched)
CREATE TABLE IF NOT EXISTS normalized_news (
  id                BIGSERIAL PRIMARY KEY,
  asset_id          BIGINT REFERENCES asset(asset_id) ON DELETE SET NULL,
  published_at_utc  TIMESTAMP WITH TIME ZONE NOT NULL,
  source            TEXT NOT NULL,
  headline          TEXT,
  snippet           TEXT,
  url               TEXT,
  tokens_count      INTEGER,
  bucket            TEXT,                       -- '0-3', '4-10', '11-30'
  lang              TEXT,                       -- detected language code
  is_relevant       BOOLEAN DEFAULT NULL,
  raw_news_id       BIGINT REFERENCES raw_news(news_id) ON DELETE SET NULL,
  raw_news_alt_id   BIGINT REFERENCES raw_news_alt(alt_news_id) ON DELETE SET NULL,
  content_hash      TEXT NOT NULL UNIQUE,       -- canonical hash for deduplication
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_normalized_news_asset_date ON normalized_news (asset_id, published_at_utc);
CREATE INDEX IF NOT EXISTS idx_normalized_news_bucket ON normalized_news (asset_id, bucket);
CREATE INDEX IF NOT EXISTS idx_normalized_news_fulltext ON normalized_news USING GIN (to_tsvector('english', COALESCE(headline, '') || ' ' || COALESCE(snippet, '')));

-- ==========================================================
-- PRICE / TECHNICAL DATA
-- ==========================================================
CREATE TABLE IF NOT EXISTS price_window (
  price_window_id BIGSERIAL PRIMARY KEY,
  asset_id        BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
  as_of_date      DATE NOT NULL,
  window_days     SMALLINT,                   -- number of days used in technical calculations
  ohlcv_window    JSONB,
  technicals      JSONB,
  file_path       TEXT,
  created_at      TIMESTAMP WITH TIME ZONE DEFAULT now(),
  UNIQUE (asset_id, as_of_date)
);

-- ==========================================================
-- FUNDAMENTALS (SimFin / Yahoo) + new FMP extension
-- ==========================================================

-- Legacy fundamental data
CREATE TABLE IF NOT EXISTS fundamentals (
  fund_id     BIGSERIAL PRIMARY KEY,
  asset_id    BIGINT REFERENCES asset(asset_id),
  source      TEXT,                         -- 'simfin', 'yahoo'
  report_date DATE,
  period_type TEXT,                         -- 'Q' or 'Y'
  data        JSONB,
  file_path   TEXT,
  created_at  TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- NEW: FMP fundamentals
CREATE TABLE IF NOT EXISTS raw_fmp_fundamentals (
  fmp_id      BIGSERIAL PRIMARY KEY,
  asset_id    BIGINT REFERENCES asset(asset_id),
  report_date DATE,
  period_type TEXT,
  currency    TEXT,
  raw_json    JSONB,
  normalized  JSONB,                        -- numeric normalization results
  source_url  TEXT,
  file_path   TEXT,
  fetched_at  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  created_at  TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fmp_fund_asset_date ON raw_fmp_fundamentals (asset_id, report_date);

-- ==========================================================
-- INSIDER / ANALYST DATA
-- ==========================================================
CREATE TABLE IF NOT EXISTS insider_txn (
  id BIGSERIAL PRIMARY KEY,
  asset_id BIGINT REFERENCES asset(asset_id) ON DELETE CASCADE,
  filing_date DATE,
  transaction_type TEXT,
  shares NUMERIC,
  amount NUMERIC,
  mspr NUMERIC,
  raw_json JSONB,
  file_path TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analyst_reco (
  id BIGSERIAL PRIMARY KEY,
  asset_id BIGINT REFERENCES asset(asset_id) ON DELETE CASCADE,
  reco_date DATE,
  consensus_rating TEXT,
  firm TEXT,
  raw_json JSONB,
  file_path TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ==========================================================
-- MACRO / ECONOMIC DATA
-- ==========================================================

-- Legacy FRED data
CREATE TABLE IF NOT EXISTS macro_series (
  series_id BIGSERIAL PRIMARY KEY,
  series_name TEXT,
  series_code TEXT UNIQUE,
  data JSONB,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT now(),
  file_path TEXT
);

-- NEW: EODHD economic events
CREATE TABLE IF NOT EXISTS raw_eodhd_economic_events (
  econ_id    BIGSERIAL PRIMARY KEY,
  event_date DATE,
  country    TEXT,
  category   TEXT,             -- 'Inflation', 'GDP', etc.
  event_name TEXT,
  importance TEXT,              -- 'low', 'medium', 'high'
  actual     TEXT,
  forecast   TEXT,
  previous   TEXT,
  raw_json   JSONB,
  file_path  TEXT,
  fetched_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_eodhd_event_date ON raw_eodhd_economic_events (event_date);

-- NEW: EODHD options data
CREATE TABLE IF NOT EXISTS raw_eodhd_options (
  option_id   BIGSERIAL PRIMARY KEY,
  asset_id    BIGINT REFERENCES asset(asset_id),
  as_of_date  DATE,
  expiration  DATE,
  option_type TEXT,              -- 'call' or 'put'
  strike      NUMERIC,
  open_interest NUMERIC,
  implied_vol NUMERIC,
  underlying_price NUMERIC,
  raw_json    JSONB,
  file_path   TEXT,
  fetched_at  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  created_at  TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_eodhd_opt_asset_date ON raw_eodhd_options (asset_id, as_of_date);

-- ==========================================================
-- ASSEMBLED SAMPLE + LABELS + DISTILLATION
-- ==========================================================

CREATE TABLE IF NOT EXISTS assembled_sample (
  sample_id      BIGSERIAL PRIMARY KEY,
  asset_id       BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
  as_of_date     DATE NOT NULL,
  variation_id   SMALLINT NOT NULL,
  run_id         BIGINT REFERENCES pipeline_run(run_id) ON DELETE SET NULL,
  as_of_cutoff   TIMESTAMP WITH TIME ZONE,  -- exact cutoff timestamp for as-of integrity
  prompt_path    TEXT,                       -- local path to text file (.txt/.json)
  prompt_blob    TEXT,                       -- optional short version
  prompt_tokens  INTEGER,
  -- NEW: richer sources_meta JSON
  sources_meta   JSONB,                      -- e.g. {
                                              --   "news": {"count": 12, "sources":["finnhub","google_news"]},
                                              --   "news_alt": {"count": 5, "sources":["gdelt","newsapi"]},
                                              --   "technicals": {"window_days": 15},
                                              --   "fundamentals": {"sources":["simfin","fmp"]},
                                              --   "macro": {"sources":["fred","eodhd"], "count":3},
                                              --   "options": {"included": true, "records":25}
                                              -- }
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  UNIQUE (asset_id, as_of_date, variation_id)
);
CREATE INDEX IF NOT EXISTS idx_assembled_asset_date ON assembled_sample (asset_id, as_of_date);

-- Labels
CREATE TABLE IF NOT EXISTS sample_label (
  label_id BIGSERIAL PRIMARY KEY,
  sample_id BIGINT NOT NULL REFERENCES assembled_sample(sample_id) ON DELETE CASCADE,
  composite_signal NUMERIC,
  label_class SMALLINT CHECK (label_class BETWEEN 1 AND 5),
  quantile NUMERIC,
  computed_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Distilled theses
CREATE TABLE IF NOT EXISTS distilled_thesis (
  thesis_id BIGSERIAL PRIMARY KEY,
  sample_id BIGINT NOT NULL REFERENCES assembled_sample(sample_id) ON DELETE CASCADE,
  thesis_path TEXT,                    -- path to file with full thesis text
  thesis_text TEXT,                    -- inline shorter summary
  thesis_structure JSONB,
  source_model TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ==========================================================
-- PIPELINE RUN + AUDIT
-- ==========================================================
CREATE TABLE IF NOT EXISTS pipeline_run (
  run_id BIGSERIAL PRIMARY KEY,
  run_name TEXT,
  run_type TEXT,
  started_at TIMESTAMP WITH TIME ZONE,
  finished_at TIMESTAMP WITH TIME ZONE,
  status TEXT,
  seed BIGINT,
  config JSONB,
  artifacts JSONB,                     -- map of output directories/paths
  meta JSONB
);

CREATE TABLE IF NOT EXISTS audit_log (
  audit_id BIGSERIAL PRIMARY KEY,
  table_name TEXT,
  record_id TEXT,
  action TEXT,
  actor TEXT,
  details JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ==========================================================
-- MATERIALIZED VIEWS (Data Quality)
-- ==========================================================

-- Data quality summary for monitoring and dashboards
CREATE MATERIALIZED VIEW IF NOT EXISTS data_quality_summary AS
SELECT
  'raw_news' AS table_name,
  source,
  COUNT(*) AS row_count,
  COUNT(DISTINCT content_hash) AS unique_content_hashes,
  COUNT(*) - COUNT(DISTINCT content_hash) AS duplicate_count,
  ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT content_hash)) / NULLIF(COUNT(*), 0), 2) AS dedupe_rate_pct,
  ROUND(100.0 * COUNT(CASE WHEN headline IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_headline_pct,
  ROUND(100.0 * COUNT(CASE WHEN published_at IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_date_pct,
  MAX(fetched_at) AS last_fetched_at
FROM charlie.raw_news
GROUP BY source

UNION ALL

SELECT
  'raw_news_alt' AS table_name,
  source,
  COUNT(*) AS row_count,
  COUNT(DISTINCT content_hash) AS unique_content_hashes,
  COUNT(*) - COUNT(DISTINCT content_hash) AS duplicate_count,
  ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT content_hash)) / NULLIF(COUNT(*), 0), 2) AS dedupe_rate_pct,
  ROUND(100.0 * COUNT(CASE WHEN headline IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_headline_pct,
  ROUND(100.0 * COUNT(CASE WHEN published_at IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_date_pct,
  MAX(fetched_at) AS last_fetched_at
FROM charlie.raw_news_alt
GROUP BY source

UNION ALL

SELECT
  'normalized_news' AS table_name,
  source,
  COUNT(*) AS row_count,
  COUNT(DISTINCT content_hash) AS unique_content_hashes,
  0 AS duplicate_count,
  0.0 AS dedupe_rate_pct,
  ROUND(100.0 * COUNT(CASE WHEN headline IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_headline_pct,
  ROUND(100.0 * COUNT(CASE WHEN published_at_utc IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS null_date_pct,
  MAX(created_at) AS last_fetched_at
FROM charlie.normalized_news
GROUP BY source;

CREATE INDEX IF NOT EXISTS idx_data_quality_summary_table ON data_quality_summary (table_name, source);

COMMIT;
