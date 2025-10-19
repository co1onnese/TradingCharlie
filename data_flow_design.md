Product Requirements Document (PRD)
Tauric-TR1-DB Platform — Local Filesystem Storage (Metaflow-Based)
1. Overview

Goal:
Design and implement a reproducible, end-to-end data pipeline for building the Tauric-TR1-DB dataset — a multi-modal financial forecasting corpus — using Metaflow for orchestration, Postgres for structured metadata, and local filesystem directories for large data artifacts.

The system must be easily upgradeable to cloud object storage (e.g., Amazon S3 or MinIO) without major code changes.

2. Motivation & Context

Researchers and data engineers require a reliable, versioned dataset generation pipeline that can run fully offline or on-premises.

Storing artifacts locally allows rapid iteration, simple backups, and easier debugging during early development.

Future migration to object storage should be achievable by switching path configuration and minimal code modifications (via an abstraction layer).

3. Stakeholders
Role	Responsibilities
ML Research Team	Consumes assembled dataset and distilled theses for model fine-tuning
Data Engineering Team	Maintains ingestion, cleaning, and labeling pipeline
Platform/Ops	Manages compute, Metaflow environment, and backup jobs
Compliance	Reviews source licensing and retention policies
4. Success Metrics

✅ End-to-end Metaflow run completes for all 14 tickers within SLA (≤ 4 hours for full 18-month re-ingest).

✅ 100 % of dataset entries have verifiable provenance (Postgres + filesystem path).

✅ Re-running with the same seed reproduces identical hashes of assembled prompts.

✅ Label distributions remain within ± 2 % of target quantiles.

✅ All artifacts and metadata are recoverable from local backups.

5. Scope
In Scope

Metaflow orchestration pipeline with steps:

ingest_raw → normalize_dedupe → compute_technicals → assemble_samples → generate_labels → distill_theses → export_parquet → finish

Postgres schema for metadata.

Local directory tree for storage of all artifacts.

Data quality checks, reproducibility, and provenance tracking.

Utilities to migrate local directories to S3 later.

Out of Scope

Continuous model training or serving.

Real-time market data ingestion.

Production S3 cloud deployment (phase 2 feature).

6. Storage Design (Local Filesystem)
6.1 Directory Hierarchy

Root directory configurable via environment variable TAURIC_DATA_ROOT (default /opt/tauric_data/):

/opt/tauric_data/
├── raw/
│   ├── finnhub/
│   ├── google_news/
│   ├── yahoo_price/
│   └── fred/
├── normalized/
│   ├── news/
│   ├── price_window/
│   ├── insider/
│   ├── analyst/
│   └── macro/
├── assembled/
│   └── <ticker>/<date>/variation_<n>.json
├── labels/
│   └── <ticker>/<date>.csv
├── distilled_theses/
│   └── <ticker>/<date>/<sample_id>.json
├── exports/
│   └── parquet/
│       ├── train/
│       └── validation/
└── logs/


Each sub-folder includes a manifest file (manifest.json) with metadata and checksums.

6.2 Migration Path to S3

All filesystem operations abstracted behind a storage interface (storage.py) providing:

storage.save(obj, path)
storage.load(path)
storage.list(prefix)


Switching to S3 requires only changing backend configuration (e.g., STORAGE_BACKEND=s3, S3_BUCKET=tauric-data) without refactoring pipeline logic.

Directory structure mirrors future S3 key prefixes.

7. Functional Requirements

Ingestion

Pull data from APIs (Finnhub, Google News, Yahoo Finance, FRED, SimFin).

Save raw JSON payloads to /raw/<source>/<ticker>/<date>.json.

Record metadata in Postgres (raw_* tables).

Normalization & Deduplication

Normalize timestamps, HTML cleaning, deduplication via dedupe_hash.

Store processed JSON in /normalized/... and update Postgres records.

Technical Computation

Generate 15-day OHLCV windows and indicators (RSI, MACD, etc.).

Save price_window JSON files.

Sample Assembly

Produce ~20 variations per (ticker, date) and save assembled prompts to /assembled/<ticker>/<date>/.

Label Generation

Compute volatility-adjusted composite signals per Algorithm S1.

Save label CSVs and insert rows into sample_label.

Distillation

Run LLM “planner + generator” modules to create reasoning text.

Save each to /distilled_theses/<ticker>/<date>/<sample_id>.json.

Parquet Export

Merge assembled_sample, sample_label, and distilled_thesis tables.

Write partitioned Parquet files to /exports/parquet/.

Provenance / Run Metadata

Each Metaflow run writes a pipeline_run record and stores a local manifest /logs/run_<id>.json.

8. Non-Functional Requirements
Category	Requirement
Reproducibility	Store git hash, Metaflow run ID, and pipeline parameters in Postgres and local manifest.json.
Performance	Full re-ingest (100k samples) finishes < 4 hours on 16-core workstation.
Security	API keys via environment variables or secrets files outside tracked directories.
Observability	Logs per run under /logs/; optional integration with Prometheus via Metaflow metrics.
Scalability	Parallelize per ticker; future scale-out via Metaflow @batch on cloud.
Upgradeability	Replace LocalStorage backend with S3Storage via config only.
Backups	Nightly rsync or tarball snapshot of /opt/tauric_data + Postgres dump.
Idempotency	All pipeline steps must be idempotent; reruns should not duplicate data or cause inconsistencies.
9. Data Governance

Public, licensed financial data only — no PII.

All files checksummed (SHA256) for integrity.

Retain raw and normalized data for 24 months unless storage quota exceeded.

Access via POSIX permissions or group ownership (e.g., tauric-data group).

10. Milestones
Week	Deliverable
0–1	Environment setup (Postgres, Metaflow, directory structure) + Configuration management system
2–3	Ingest + normalize flows + Database indexes and partitioning
4	Technicals + assemble + label + Idempotent pipeline steps
5	Distillation + Parquet export + Checkpointing and data lineage
6	Full pipeline run (local storage) + Performance optimization
7	Migration-ready abstraction layer verified + Comprehensive testing
8	Documentation & training + Operational runbooks
11. Risks & Mitigation
Risk	Mitigation
Disk space exhaustion	Monitor /opt/tauric_data usage; compress archives older than 90 days.
File corruption	Checksums + backup validation.
Local path inconsistencies	Central config for TAURIC_DATA_ROOT; unit tests verify existence.
Migration complexity	Use consistent path schema identical to S3 key pattern.
Configuration drift	Version control for config files; validation on startup.
Database performance	Monitor query performance; add indexes as needed; partition large tables.
Pipeline failures	Idempotent steps; checkpointing; retry logic with exponential backoff.
Data inconsistency	Data lineage tracking; validation at each step; atomic operations.
12. Acceptance Criteria

Successful Metaflow pipeline execution writes all outputs under /opt/tauric_data.

Postgres pipeline_run.status = 'success'.

Re-run with same seed produces identical checksums.

Migration test: switching to mock S3 backend runs unchanged and produces same manifests.

Configuration validation passes for all environments (dev, staging, prod).

Database queries complete within performance thresholds (indexes effective).

Pipeline reruns are idempotent (no duplicate data, consistent outputs).

All pipeline steps support checkpointing and resumption from failures.

13. Configuration Management

13.1 Hierarchical Configuration System

Replace the current CONFIG dictionary with a hierarchical configuration system:

- **Defaults**: `config/defaults.yaml` - Base configuration values
- **Environment**: Environment variables override defaults (TAURIC_*)
- **Runtime**: CLI parameters override environment variables
- **Validation**: Pydantic Settings for config validation and type checking

13.2 Configuration Structure

```yaml
# config/defaults.yaml
data_root: "/opt/tauric_data"
db_url: "postgresql+psycopg2://tauric:tauricpass@localhost:5432/tauric"
storage_backend: "local"
api_keys:
  finnhub: ""
  google_news: ""
  fmp: ""
  eodhd: ""
  fred: ""
llm:
  provider: "openai"
  model: "gpt-4o-mini"
  batch_size: 8
pipeline:
  variation_count: 20
  token_budget: 8192
  seed: 1234
```

13.3 Environment-Specific Configs

- **Development**: `config/dev.yaml` - Local development settings
- **Staging**: `config/staging.yaml` - Pre-production testing
- **Production**: `config/prod.yaml` - Production environment settings

13.4 Configuration Validation

Use Pydantic Settings to validate:
- Required API keys are present
- Database URL format is valid
- Storage paths are accessible
- LLM configuration is complete
- Pipeline parameters are within valid ranges

14. Database Performance Optimizations

14.1 Critical Indexes

Add composite indexes for common query patterns:

```sql
-- News queries by asset and date range
CREATE INDEX idx_raw_news_asset_published ON raw_news (asset_id, published_at);
CREATE INDEX idx_raw_news_alt_asset_published ON raw_news_alt (asset_id, published_at);

-- Price window queries
CREATE INDEX idx_price_window_asset_date ON price_window (asset_id, as_of_date);

-- Assembled samples by asset and date
CREATE INDEX idx_assembled_asset_date ON assembled_sample (asset_id, as_of_date);

-- Sample labels and thesis lookups
CREATE INDEX idx_sample_label_sample ON sample_label (sample_id);
CREATE INDEX idx_distilled_thesis_sample ON distilled_thesis (sample_id);
```

14.2 Table Partitioning

Partition large tables by date for improved query performance:

```sql
-- Partition raw_news by month
CREATE TABLE raw_news_partitioned (
  LIKE raw_news INCLUDING ALL
) PARTITION BY RANGE (published_at);

-- Create monthly partitions
CREATE TABLE raw_news_2024_01 PARTITION OF raw_news_partitioned
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

14.3 Materialized Views

Create materialized views for common aggregations:

```sql
-- Label distribution summary
CREATE MATERIALIZED VIEW label_distribution AS
SELECT 
  asset_id,
  as_of_date,
  label_class,
  COUNT(*) as count,
  AVG(composite_signal) as avg_signal
FROM sample_label sl
JOIN assembled_sample a ON sl.sample_id = a.sample_id
GROUP BY asset_id, as_of_date, label_class;

-- Source metadata summary
CREATE MATERIALIZED VIEW source_summary AS
SELECT 
  asset_id,
  as_of_date,
  sources_meta->>'news' as news_count,
  sources_meta->>'technicals' as technicals_info
FROM assembled_sample;
```

15. Pipeline Idempotency

15.1 Idempotent Steps

All pipeline steps must be idempotent to support safe reruns:

- **ingest_raw**: Check for existing raw data before fetching
- **normalize_dedupe**: Use UPSERT operations for normalized data
- **compute_technicals**: Update existing price_window records
- **assemble_samples**: Check for existing samples before creating new ones
- **generate_labels**: Update existing labels or create new ones
- **distill_theses**: Check for existing thesis before LLM calls
- **export_parquet**: Overwrite existing parquet files

15.2 Upsert Logic

Implement proper upsert operations:

```python
def upsert_raw_news(engine: Engine, row: Dict[str, Any]):
    stmt = text("""
    INSERT INTO tauric.raw_news (asset_id, source, headline, snippet, url, published_at, raw_json, dedupe_hash)
    VALUES (:asset_id, :source, :headline, :snippet, :url, :published_at, :raw_json, :dedupe_hash)
    ON CONFLICT (asset_id, source, dedupe_hash) 
    DO UPDATE SET 
      headline = EXCLUDED.headline,
      snippet = EXCLUDED.snippet,
      raw_json = EXCLUDED.raw_json,
      fetched_at = now()
    """)
```

15.3 Checkpointing

Add checkpointing for long-running steps:

- Store intermediate state after each major operation
- Allow resumption from last successful checkpoint
- Implement step-level retry with exponential backoff
- Track completion status in pipeline_run metadata

15.4 Data Lineage

Track data lineage to support idempotent operations:

- Record which raw data contributed to each assembled sample
- Maintain mapping between source records and generated samples
- Enable selective reprocessing of affected samples only
- Support partial pipeline reruns for specific date ranges

16. Future Upgrade Plan

To move from local filesystem → S3:

Update STORAGE_BACKEND=s3 in config.

Sync local directory to S3 (aws s3 sync /opt/tauric_data s3://tauric-data/).

Replace path resolver to prepend s3://tauric-data/.

Re-run pipeline — identical outputs verified by checksum parity.