### Objectives
- **End-to-end alignment**: Collect → clean/normalize → assemble → label → distill → export.
- **Database-backed normalization**: Strong dedup, provenance, quality checks, and as‑of integrity.
- **Modalities coverage**: Complete ingestion across news, fundamentals, macro, options, insider, analyst.
- **Labeling parity**: Volatility-aware labeling per asset with asymmetric quantiles.

### Database changes (DDL)
- **Add normalized tables**
  - `normalized_news` (id, asset_id, published_at_utc, source, headline, snippet, url, tokens_count, bucket, lang, is_relevant, raw_news_id, raw_news_alt_id, content_hash, created_at)
    - Indexes: (asset_id, published_at_utc), UNIQUE(content_hash), GIN full-text on headline+snippet.
  - Optional stubs: `normalized_fundamentals`, `normalized_options`, `normalized_macro` with core keys + `normalized` JSONB.
- **Extend existing tables**
  - `raw_news`, `raw_news_alt`: add `content_hash TEXT`, `request_meta JSONB`; index on `content_hash` (UNIQUE if feasible).
  - `price_window`: add `window_days SMALLINT`.
  - `assembled_sample`: add `run_id BIGINT REFERENCES pipeline_run(run_id)`, optional `as_of_cutoff TIMESTAMPTZ`.
- **Provenance and views**
  - New MV `charlie.data_quality_summary` (row counts, dedupe rates, null ratios by table/source).
  - Update `scripts/init_db.sh` to create new tables, indexes, MVs, and extend the refresh function.

### Ingestion enhancements (`charlie_tr1_flow.py`, `charlie_fetchers.py`, `charlie_utils.py`)
- **News ingestion**
  - Implement `upsert_raw_news_alt(engine, row)` mirroring `upsert_raw_news`.
  - Compute/persist `content_hash = sha256(headline+url+published_at)` for all raw inserts.
  - Persist `request_meta` (endpoint, params, status, latency) on raw rows.
- **Additional modalities**
  - Add DB writers: `insert_insider_txn`, `insert_analyst_reco`, `insert_raw_eodhd_econ_events`; call them in `ingest_raw`.
  - SEC filings: stub EDGAR fetcher (10-Q/10-K) with `request_meta`; store to `raw_fundamentals` or new `raw_sec_filings`.
- **Rate limiting/retries**
  - Centralize per-provider backoff (tenacity) using `CONFIG` delays; log circuit-breaker events to `audit_log`.

### Normalize and dedupe (`normalize_dedupe`)
- **Timezone normalization**: Parse all dates and store `published_at_utc` in UTC.
- **Bucketing**: Compute `bucket` relative to each `as_of_date` (0-3, 4-10, 11-30).
- **Token estimates + language**: Use current estimator; optionally add fast language detection to populate `lang`.
- **Cross-source dedup**: Dedup across `raw_news` and `raw_news_alt` via `content_hash`; keep canonical reference.
- **Relevance heuristic**: Headline/snippet must include ticker/company alias, min length, allowed sources; set `is_relevant`.
- **Persistence**: Upsert to `normalized_news` with idempotency; QA-logs for drops, dedupe hits, failures.
- **Quality gates**: Validate required fields and date windows; on failure, write to `audit_log` and skip insert.

### Technicals (`compute_technicals`)
- Replace minimal pandas calcs with `ta` library (RSI, MACD, Bollinger, ATR, EMA, Ichimoku).
- Persist compact `latest` snapshot + short `series`; record `window_days`.
- Guard for NaNs/short windows; maintain deterministic output types.

### Assembly (`assemble_samples`)
- **As‑of integrity**
  - News: select by `bucket` windows; configurable min/max per bucket.
  - Fundamentals: latest report ≤ `as_of_date`.
  - Macro/events: events up to `as_of_date`.
  - Options: same-day or nearest prior if policy allows.
- **Sampling and budgeting**
  - Per-modality quotas; approximate token budget using estimator; stable seeding.
  - Populate `sources_meta` with counts, sources, date windows; attach `run_id`, `as_of_cutoff`.
- **Variations**
  - Deterministic `variation_id` generation; ensure uniqueness on (asset, date, variation_id).

### Labels (`generate_labels`)
- Compute labels per exact `as_of_date` using full price series; ensure strict forward-looking windows.
- Maintain per-asset quantiles; asymmetric cutoffs {3%, 15%, 53%, 85%}.
- One `sample_label` per `sample_id`; skip when not available (no leakage).
- Refresh `label_distribution` MV post-run.

### Distillation (`distill_theses`)
- Enable real LLM integration if configured; retry/backoff; capture `tokens_used` and approximate cost.
- Structure extraction into `thesis_structure` (summary, claims, evidence, risks) with version tag.
- Parameterize sampling rate per ticker; ensure representative coverage; deterministic selection by seed.

### Exports (`export_parquet`)
- Stabilize export schema; include `run_id`, `as_of_cutoff`, and normalized `sources_meta` fields.
- Partition by `ticker/as_of_date`; deterministic filenames including `run_id`.
- Add validation: row/column counts, null checks; emit `_SUCCESS` and checksum per partition.

### Provenance and lineage
- Store `content_hash` and `file_sha256` for artifacts; extend `pipeline_run.artifacts` with checksums.
- Persist `request_meta` per fetch; indexable fields for diagnostics.
- Add helper `write_audit(engine, table_name, record_id, action, details)`; log drops, dedupe, invalid rows, rate-limit events.
- End-of-run: call `charlie.refresh_all_materialized_views()`.

### Configuration and ops
- Update `.env` keys for new fetchers; document in README.
- Add CLI flags to toggle modalities, sampling rates, export format; add `--dry_run`.
- Batch upserts; paginate large selects; avoid large in-memory accumulations.

### Testing
- **Unit tests**: `compute_labels_for_asset`, bucket computation, hashing/dedup, ta indicators vs fixtures.
- **Integration tests**: Mini synthetic pipeline; assert assembled/labels counts; verify export presence and schema.
- **SQL tests**: Uniqueness constraints, indexes, MV refresh.

### Milestones
- **M1**: DDL updates; raw upsert hashes; populate `normalized_news` with bucketing/relevance.
- **M2**: Assembly as‑of integrity and `sources_meta`; `ta` indicators; per-date labels.
- **M3**: Additional modalities ingestion/writers; provenance and audit logging.
- **M4**: Distillation integration and structured parsing; export hardening.
- **M5**: Tests, docs, performance tuning, MV dashboards.

### Deliverables
- Updated `charlie.ddl`, `scripts/init_db.sh`.
- Enhanced steps in `charlie_tr1_flow.py` (`ingest_raw`, `normalize_dedupe`, `compute_technicals`, `assemble_samples`, `generate_labels`, `distill_theses`, `export_parquet`).
- New/extended upsert helpers in `charlie_utils.py`.
- Extended fetchers in `charlie_fetchers.py`.
- README updates and configuration guidance.
