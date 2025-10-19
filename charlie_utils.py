# charlie_utils.py
#
# Utilities module for Charlie-TR1 pipeline
# Contains: Config, Storage, Database utilities, Data transformations

from dotenv import load_dotenv
load_dotenv('.env.local')

from pathlib import Path
import os
import json
import hashlib
import logging
import math
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Technical analysis library
try:
    import ta
except ImportError:
    ta = None

# API-specific imports (will check availability in validate_and_log_config)
try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import finnhub
except ImportError:
    finnhub = None

try:
    from fredapi import Fred
except ImportError:
    Fred = None

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

# -------------------------
# CONFIG - update per environment
# -------------------------
CONFIG = {
    # Local root for all artifacts (raw, normalized, assembled, labels, thesis, exports, logs)
    "DATA_ROOT": os.environ.get("CHARLIE_DATA_ROOT", "/opt/charlie_data"),

    # Postgres connection string (SQLAlchemy URL)
    "DB_URL": os.environ.get("CHARLIE_DB_URL", "postgresql+psycopg2://charlie:charliepass@localhost:5432/charlie"),

    # Storage backend: 'local' or 's3'
    "STORAGE_BACKEND": os.environ.get("CHARLIE_STORAGE_BACKEND", "local"),

    # LLM / distillation config (M4: added Claude support)
    "LLM": {
        "provider": os.environ.get("CHARLIE_LLM_PROVIDER", "openai"),
        "api_key": os.environ.get("CHARLIE_LLM_API_KEY", None),  # OpenAI key
        "anthropic_api_key": os.environ.get("ANTHROPIC_API_KEY", None),  # Claude key
        "model": os.environ.get("CHARLIE_LLM_MODEL", "gpt-4o-mini"),
        "claude_model": os.environ.get("CHARLIE_CLAUDE_MODEL", "claude-3-5-sonnet-20241022"),
        "fallback_to_claude": os.environ.get("CHARLIE_LLM_FALLBACK", "true").lower() == "true",
        "batch_size": int(os.environ.get("CHARLIE_LLM_BATCH", "8"))
    },

    # Ingestion API keys
    "FINNHUB_API_KEY": os.environ.get("FINNHUB_API_KEY", ""),
    "SERPAPI_KEY": os.environ.get("SERPAPI_KEY", ""),  # For Google News
    "YAHOO": {},  # yfinance doesn't need a key
    "SIMFIN_API_KEY": os.environ.get("SIMFIN_API_KEY", ""),
    "FMP_API_KEY": os.environ.get("FMP_API_KEY", ""),
    "EODHD_API_KEY": os.environ.get("EODHD_API_KEY", ""),
    "FRED_API_KEY": os.environ.get("FRED_API_KEY", ""),
    "NEWSAPI_KEY": os.environ.get("NEWSAPI_KEY", ""),

    # Pipeline configuration
    "MAX_RETRIES": int(os.environ.get("CHARLIE_MAX_RETRIES", "3")),
    "RATE_LIMIT_DELAY": float(os.environ.get("CHARLIE_RATE_LIMIT_DELAY", "1.0")),
    "DEBUG": os.environ.get("CHARLIE_DEBUG", "false").lower() == "true",

    # M2: Assembly quotas (per-modality limits)
    "ASSEMBLY_QUOTAS": {
        "news_per_bucket": {
            "0-3": int(os.environ.get("CHARLIE_NEWS_BUCKET_0_3", "10")),
            "4-10": int(os.environ.get("CHARLIE_NEWS_BUCKET_4_10", "8")),
            "11-30": int(os.environ.get("CHARLIE_NEWS_BUCKET_11_30", "5"))
        },
        "max_fundamentals": int(os.environ.get("CHARLIE_MAX_FUNDAMENTALS", "3")),
        "max_options": int(os.environ.get("CHARLIE_MAX_OPTIONS", "25")),
        "max_macro_events": int(os.environ.get("CHARLIE_MAX_MACRO_EVENTS", "5")),
        "max_insider_txns": int(os.environ.get("CHARLIE_MAX_INSIDER_TXNS", "10")),
        "max_analyst_recos": int(os.environ.get("CHARLIE_MAX_ANALYST_RECOS", "5")),
    },
}

# Ensure root exists
Path(CONFIG["DATA_ROOT"]).mkdir(parents=True, exist_ok=True)

# -------------------------
# Logging setup
# -------------------------
log_level = logging.DEBUG if CONFIG.get("DEBUG", False) else logging.INFO
logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("charlie")

# Validate configuration and log API status
def validate_and_log_config():
    """Validate configuration and log which APIs are available"""
    logger.info(f"Data root: {CONFIG['DATA_ROOT']}")
    logger.info(f"Database: {CONFIG['DB_URL'].split('@')[1] if '@' in CONFIG['DB_URL'] else 'configured'}")
    logger.info(f"Storage backend: {CONFIG['STORAGE_BACKEND']}")

    # Check library installations
    lib_status = {
        "yfinance": yf is not None,
        "finnhub": finnhub is not None,
        "fredapi": Fred is not None,
        "openai": OpenAI is not None,
    }

    # Check API keys
    api_status = {
        "Yahoo Finance": lib_status["yfinance"],  # No key needed
        "Finnhub": bool(CONFIG['FINNHUB_API_KEY']) and lib_status["finnhub"],
        "FMP": bool(CONFIG['FMP_API_KEY']),
        "EODHD": bool(CONFIG['EODHD_API_KEY']),
        "FRED": bool(CONFIG['FRED_API_KEY']) and lib_status["fredapi"],
        "NewsAPI": bool(CONFIG['NEWSAPI_KEY']),
        "SimFin": bool(CONFIG['SIMFIN_API_KEY']),
        "SerpAPI (Google News)": bool(CONFIG['SERPAPI_KEY']),
        "LLM": bool(CONFIG['LLM']['api_key']) and lib_status["openai"],
    }

    logger.info("API Configuration Status:")
    for api, available in api_status.items():
        status = "✓ Ready" if available else "✗ Not available"
        logger.info(f"  {api}: {status}")

    if not any(api_status.values()):
        logger.warning("⚠️  No APIs available! Pipeline will run but return empty data.")

    return api_status

# -------------------------
# Storage abstraction
# -------------------------
class StorageBackend:
    def save_json(self, obj: Any, path: str) -> str:
        raise NotImplementedError

    def read_json(self, path: str) -> Any:
        raise NotImplementedError

    def makedirs(self, path: str):
        raise NotImplementedError

    def list(self, prefix: str) -> List[str]:
        raise NotImplementedError

class LocalStorage(StorageBackend):
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _full(self, path: str) -> Path:
        p = Path(path)
        if p.is_absolute():
            return p
        return self.root.joinpath(path)

    def save_json(self, obj: Any, path: str) -> str:
        full = self._full(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
        return str(full)

    def read_json(self, path: str) -> Any:
        full = self._full(path)
        with open(full, "r", encoding="utf-8") as f:
            return json.load(f)

    def makedirs(self, path: str):
        self._full(path).mkdir(parents=True, exist_ok=True)

    def list(self, prefix: str) -> List[str]:
        p = self._full(prefix)
        if not p.exists():
            return []
        return [str(pf) for pf in p.rglob("*") if pf.is_file()]

# S3Storage stub - implement with boto3 if you switch to S3
class S3Storage(StorageBackend):
    def __init__(self, bucket: str, root_prefix: str = ""):
        raise NotImplementedError("S3Storage not implemented in this file; replace with boto3 wrapper.")

# Choose backend
if CONFIG["STORAGE_BACKEND"] == "s3":
    storage = S3Storage(bucket=os.environ.get("CHARLIE_S3_BUCKET", "charlie-data"))
else:
    storage = LocalStorage(CONFIG["DATA_ROOT"])

# -------------------------
# Database utilities (SQLAlchemy)
# -------------------------
def get_db_engine() -> Engine:
    return create_engine(CONFIG["DB_URL"], future=True)

def write_pipeline_run_to_db(engine: Engine, run_meta: Dict[str, Any]) -> int:
    """
    Insert a pipeline_run record and return run_id.
    run_meta: {run_name, run_type, started_at, finished_at, status, seed, config, artifacts, meta}
    """
    now = datetime.utcnow()
    insert_sql = text("""
    INSERT INTO charlie.pipeline_run (run_name, run_type, started_at, finished_at, status, seed, config, artifacts, meta)
    VALUES (:run_name, :run_type, :started_at, :finished_at, :status, :seed, :config, :artifacts, :meta)
    RETURNING run_id
    """)
    with engine.begin() as conn:
        result = conn.execute(insert_sql, {
            "run_name": run_meta.get("run_name"),
            "run_type": run_meta.get("run_type"),
            "started_at": run_meta.get("started_at"),
            "finished_at": run_meta.get("finished_at"),
            "status": run_meta.get("status"),
            "seed": run_meta.get("seed"),
            "config": json.dumps(run_meta.get("config") or {}),
            "artifacts": json.dumps(run_meta.get("artifacts") or {}),
            "meta": json.dumps(run_meta.get("meta") or {})
        })
        run_id = result.scalar()
    logger.info(f"Inserted pipeline_run id={run_id}")
    return int(run_id)

def upsert_asset(engine: Engine, ticker: str, name: Optional[str]=None, sector: Optional[str]=None, market_cap: Optional[float]=None) -> int:
    """
    Upsert asset and return asset_id.
    """
    with engine.begin() as conn:
        # Try select
        sel = conn.execute(text("SELECT asset_id FROM charlie.asset WHERE ticker = :ticker"), {"ticker": ticker}).fetchone()
        if sel:
            return int(sel[0])
        # Insert
        result = conn.execute(text("""
            INSERT INTO charlie.asset (ticker, name, sector, market_cap)
            VALUES (:ticker, :name, :sector, :market_cap)
            RETURNING asset_id
        """), {"ticker": ticker, "name": name, "sector": sector, "market_cap": market_cap})
        return int(result.scalar())

# Upsert helpers for raw tables (idempotent operations)
def upsert_raw_news(engine: Engine, row: Dict[str, Any]):
    """Insert or update raw_news with ON CONFLICT handling"""
    stmt = text("""
    INSERT INTO charlie.raw_news (
      asset_id, source, headline, snippet, url, published_at,
      fetched_at, raw_json, dedupe_hash, is_relevant, bucket, tokens_count, file_path,
      content_hash, request_meta
    ) VALUES (
      :asset_id, :source, :headline, :snippet, :url, :published_at,
      :fetched_at, :raw_json, :dedupe_hash, :is_relevant, :bucket, :tokens_count, :file_path,
      :content_hash, :request_meta
    )
    ON CONFLICT (dedupe_hash)
    DO UPDATE SET
      fetched_at = EXCLUDED.fetched_at,
      raw_json = EXCLUDED.raw_json,
      file_path = EXCLUDED.file_path,
      content_hash = EXCLUDED.content_hash,
      request_meta = EXCLUDED.request_meta
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "source": row.get("source"),
            "headline": row.get("headline"),
            "snippet": row.get("snippet"),
            "url": row.get("url"),
            "published_at": row.get("published_at"),
            "fetched_at": row.get("fetched_at", datetime.utcnow()),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "dedupe_hash": row.get("dedupe_hash"),
            "is_relevant": row.get("is_relevant"),
            "bucket": row.get("bucket"),
            "tokens_count": row.get("tokens_count"),
            "file_path": row.get("file_path"),
            "content_hash": row.get("content_hash"),
            "request_meta": json.dumps(row.get("request_meta", {})) if row.get("request_meta") else None
        })

# Keep old name for compatibility
insert_raw_news = upsert_raw_news

def insert_price_window(engine: Engine, row: Dict[str, Any]):
    stmt = text("""
    INSERT INTO charlie.price_window (asset_id, as_of_date, window_days, ohlcv_window, technicals, file_path)
    VALUES (:asset_id, :as_of_date, :window_days, :ohlcv_window, :technicals, :file_path)
    ON CONFLICT (asset_id, as_of_date) DO UPDATE SET
      window_days = EXCLUDED.window_days,
      ohlcv_window = EXCLUDED.ohlcv_window,
      technicals = EXCLUDED.technicals,
      file_path = EXCLUDED.file_path
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "as_of_date": row.get("as_of_date"),
            "window_days": row.get("window_days"),
            "ohlcv_window": json.dumps(row.get("ohlcv_window", {})),
            "technicals": json.dumps(row.get("technicals", {})),
            "file_path": row.get("file_path")
        })

def insert_raw_fmp_fundamentals(engine: Engine, row: Dict[str, Any]):
    stmt = text("""
    INSERT INTO charlie.raw_fmp_fundamentals (
      asset_id, report_date, period_type, currency, raw_json, normalized, source_url, file_path, fetched_at
    ) VALUES (
      :asset_id, :report_date, :period_type, :currency, :raw_json, :normalized, :source_url, :file_path, :fetched_at
    )
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "report_date": row.get("report_date"),
            "period_type": row.get("period_type"),
            "currency": row.get("currency"),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "normalized": json.dumps(row.get("normalized", {})),
            "source_url": row.get("source_url"),
            "file_path": row.get("file_path"),
            "fetched_at": row.get("fetched_at", datetime.utcnow())
        })

def insert_raw_eodhd_options(engine: Engine, row: Dict[str, Any]):
    stmt = text("""
    INSERT INTO charlie.raw_eodhd_options (
      asset_id, as_of_date, expiration, option_type, strike, open_interest, implied_vol, underlying_price, raw_json, file_path, fetched_at
    ) VALUES (
      :asset_id, :as_of_date, :expiration, :option_type, :strike, :open_interest, :implied_vol, :underlying_price, :raw_json, :file_path, :fetched_at
    )
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "as_of_date": row.get("as_of_date"),
            "expiration": row.get("expiration"),
            "option_type": row.get("option_type"),
            "strike": row.get("strike"),
            "open_interest": row.get("open_interest"),
            "implied_vol": row.get("implied_vol"),
            "underlying_price": row.get("underlying_price"),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "file_path": row.get("file_path"),
            "fetched_at": row.get("fetched_at", datetime.utcnow())
        })

# M3: Additional DB writers for new modalities
def insert_insider_txn(engine: Engine, row: Dict[str, Any]):
    """Insert insider transaction record"""
    stmt = text("""
    INSERT INTO charlie.insider_txn (
      asset_id, filing_date, transaction_type, shares, amount, mspr, raw_json, file_path
    ) VALUES (
      :asset_id, :filing_date, :transaction_type, :shares, :amount, :mspr, :raw_json, :file_path
    )
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "filing_date": row.get("filing_date"),
            "transaction_type": row.get("transaction_type"),
            "shares": row.get("shares"),
            "amount": row.get("amount"),
            "mspr": row.get("mspr"),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "file_path": row.get("file_path")
        })

def insert_analyst_reco(engine: Engine, row: Dict[str, Any]):
    """Insert analyst recommendation record"""
    stmt = text("""
    INSERT INTO charlie.analyst_reco (
      asset_id, reco_date, consensus_rating, firm, raw_json, file_path
    ) VALUES (
      :asset_id, :reco_date, :consensus_rating, :firm, :raw_json, :file_path
    )
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "reco_date": row.get("reco_date"),
            "consensus_rating": row.get("consensus_rating"),
            "firm": row.get("firm"),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "file_path": row.get("file_path")
        })

def insert_raw_eodhd_econ_events(engine: Engine, row: Dict[str, Any]):
    """Insert economic event record"""
    stmt = text("""
    INSERT INTO charlie.raw_eodhd_economic_events (
      event_date, country, category, event_name, importance, actual, forecast, previous, raw_json, file_path, fetched_at
    ) VALUES (
      :event_date, :country, :category, :event_name, :importance, :actual, :forecast, :previous, :raw_json, :file_path, :fetched_at
    )
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "event_date": row.get("event_date"),
            "country": row.get("country"),
            "category": row.get("category"),
            "event_name": row.get("event_name"),
            "importance": row.get("importance"),
            "actual": row.get("actual"),
            "forecast": row.get("forecast"),
            "previous": row.get("previous"),
            "raw_json": json.dumps(row.get("raw_json", {})),
            "file_path": row.get("file_path"),
            "fetched_at": row.get("fetched_at", datetime.utcnow())
        })

def insert_assembled_sample(engine: Engine, row: Dict[str, Any]) -> int:
    stmt = text("""
    INSERT INTO charlie.assembled_sample (
      asset_id, as_of_date, variation_id, run_id, as_of_cutoff,
      prompt_path, prompt_blob, prompt_tokens, sources_meta
    ) VALUES (
      :asset_id, :as_of_date, :variation_id, :run_id, :as_of_cutoff,
      :prompt_path, :prompt_blob, :prompt_tokens, :sources_meta
    )
    ON CONFLICT (asset_id, as_of_date, variation_id)
    DO UPDATE SET
      run_id = EXCLUDED.run_id,
      as_of_cutoff = EXCLUDED.as_of_cutoff,
      prompt_path = EXCLUDED.prompt_path,
      prompt_blob = EXCLUDED.prompt_blob,
      prompt_tokens = EXCLUDED.prompt_tokens,
      sources_meta = EXCLUDED.sources_meta
    RETURNING sample_id
    """)
    with engine.begin() as conn:
        res = conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "as_of_date": row.get("as_of_date"),
            "variation_id": row.get("variation_id"),
            "run_id": row.get("run_id"),
            "as_of_cutoff": row.get("as_of_cutoff"),
            "prompt_path": row.get("prompt_path"),
            "prompt_blob": row.get("prompt_blob"),
            "prompt_tokens": row.get("prompt_tokens"),
            "sources_meta": json.dumps(row.get("sources_meta", {}))
        })
        return int(res.scalar())

def insert_sample_label(engine: Engine, row: Dict[str, Any]):
    # First try to update, if no rows affected then insert
    update_stmt = text("""
    UPDATE charlie.sample_label 
    SET composite_signal = :composite_signal,
        label_class = :label_class,
        quantile = :quantile,
        computed_at = :computed_at
    WHERE sample_id = :sample_id
    """)
    insert_stmt = text("""
    INSERT INTO charlie.sample_label (sample_id, composite_signal, label_class, quantile, computed_at)
    VALUES (:sample_id, :composite_signal, :label_class, :quantile, :computed_at)
    """)
    params = {
        "sample_id": row.get("sample_id"),
        "composite_signal": row.get("composite_signal"),
        "label_class": row.get("label_class"),
        "quantile": row.get("quantile"),
        "computed_at": row.get("computed_at", datetime.utcnow())
    }
    with engine.begin() as conn:
        result = conn.execute(update_stmt, params)
        if result.rowcount == 0:
            # No rows updated, so insert
            try:
                conn.execute(insert_stmt, params)
            except:
                # If insert fails (e.g., already exists), ignore
                pass

def insert_distilled_thesis(engine: Engine, row: Dict[str, Any]):
    # First try to update, if no rows affected then insert
    update_stmt = text("""
    UPDATE charlie.distilled_thesis 
    SET thesis_path = :thesis_path,
        thesis_text = :thesis_text,
        thesis_structure = :thesis_structure,
        source_model = :source_model
    WHERE sample_id = :sample_id
    """)
    insert_stmt = text("""
    INSERT INTO charlie.distilled_thesis (sample_id, thesis_path, thesis_text, thesis_structure, source_model)
    VALUES (:sample_id, :thesis_path, :thesis_text, :thesis_structure, :source_model)
    """)
    params = {
        "sample_id": row.get("sample_id"),
        "thesis_path": row.get("thesis_path"),
        "thesis_text": row.get("thesis_text"),
        "thesis_structure": json.dumps(row.get("thesis_structure", {})),
        "source_model": row.get("source_model")
    }
    with engine.begin() as conn:
        result = conn.execute(update_stmt, params)
        if result.rowcount == 0:
            # No rows updated, so insert
            try:
                conn.execute(insert_stmt, params)
            except:
                # If insert fails (e.g., already exists), ignore
                pass

# -------------------------
# Utility helpers
# -------------------------
def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def save_obj_and_record(storage: StorageBackend, obj: Any, base_dir: str, filename: str) -> str:
    """
    Saves obj as JSON to local storage and returns the absolute path.
    base_dir is relative to storage root (e.g., 'raw/finnhub/AAPL/2025-03-15/'),
    filename is the filename (e.g., 'article_123.json')
    """
    path = Path(base_dir) / filename
    full_path = storage.save_json(obj, str(path))
    return full_path

def date_range(start_date: date, end_date: date) -> List[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=i) for i in range(days + 1)]

def truncate_text_for_budget(text: str, token_budget: int = 4000) -> (str, int):
    """
    naïve token estimation: 1 token ~ 4 chars; truncate accordingly
    """
    max_chars = token_budget * 4
    if len(text) <= max_chars:
        return text, math.ceil(len(text) / 4)
    truncated = text[:max_chars]
    return truncated, math.ceil(len(truncated) / 4)

# -------------------------
# Normalization utilities (M1)
# -------------------------
def compute_content_hash(headline: str, url: str, published_at: Any) -> str:
    """
    Compute SHA256 hash for cross-source deduplication.
    Uses headline + url + published_at timestamp as input.
    """
    # Normalize inputs
    headline = (headline or "").strip().lower()
    url = (url or "").strip().lower()

    # Convert published_at to string
    if isinstance(published_at, datetime):
        pub_str = published_at.isoformat()
    elif isinstance(published_at, str):
        pub_str = published_at
    else:
        pub_str = str(published_at) if published_at else ""

    # Combine and hash
    combined = f"{headline}|{url}|{pub_str}"
    return sha256_hash(combined)

def normalize_to_utc(timestamp: Any, source_tz: str = "UTC") -> Optional[datetime]:
    """
    Normalize timestamp to UTC. Handles various input formats.
    Returns None if timestamp is invalid.
    """
    if timestamp is None:
        return None

    if isinstance(timestamp, datetime):
        # If already datetime, ensure it has timezone info
        if timestamp.tzinfo is None:
            # Assume UTC if no timezone
            return timestamp.replace(tzinfo=None)  # Store as timezone-naive UTC
        else:
            # Convert to UTC
            return timestamp.astimezone(None).replace(tzinfo=None)

    if isinstance(timestamp, str):
        # Try to parse string timestamp
        try:
            from dateutil import parser
            dt = parser.parse(timestamp)
            if dt.tzinfo is None:
                return dt  # Assume UTC
            else:
                return dt.astimezone(None).replace(tzinfo=None)
        except:
            logger.warning(f"Failed to parse timestamp: {timestamp}")
            return None

    return None

def compute_bucket(published_at: datetime, as_of_date: date) -> Optional[str]:
    """
    Compute temporal bucket based on days between published_at and as_of_date.
    Buckets: '0-3', '4-10', '11-30'
    Returns None if inputs are invalid.
    """
    if published_at is None or as_of_date is None:
        return None

    # Convert published_at to date for comparison
    if isinstance(published_at, datetime):
        pub_date = published_at.date()
    elif isinstance(published_at, date):
        pub_date = published_at
    else:
        return None

    # Calculate days difference
    days_diff = (as_of_date - pub_date).days

    if days_diff < 0:
        return None  # Future article, invalid
    elif days_diff <= 3:
        return "0-3"
    elif days_diff <= 10:
        return "4-10"
    elif days_diff <= 30:
        return "11-30"
    else:
        return None  # Too old

def check_relevance(headline: str, snippet: str, ticker: str, company_name: Optional[str] = None) -> bool:
    """
    Check if news article is relevant to the asset.
    Criteria:
    - Headline or snippet must contain ticker or company name
    - Minimum length requirements
    - Not from blacklisted sources (future enhancement)
    """
    if not headline and not snippet:
        return False

    # Combine text for search
    text = ((headline or "") + " " + (snippet or "")).lower()

    # Check minimum length (at least 20 characters)
    if len(text.strip()) < 20:
        return False

    # Check for ticker presence
    ticker_lower = ticker.lower()
    if ticker_lower in text:
        return True

    # Check for company name if provided
    if company_name:
        company_lower = company_name.lower()
        if company_lower in text:
            return True

    return False

def write_audit(engine: Engine, table_name: str, record_id: str, action: str, details: Dict[str, Any]):
    """
    Write audit log entry for tracking data quality issues, deduplication, etc.
    """
    stmt = text("""
    INSERT INTO charlie.audit_log (table_name, record_id, action, actor, details)
    VALUES (:table_name, :record_id, :action, :actor, :details)
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "table_name": table_name,
            "record_id": str(record_id),
            "action": action,
            "actor": "pipeline",
            "details": json.dumps(details)
        })

def upsert_normalized_news(engine: Engine, row: Dict[str, Any]):
    """
    Insert or update normalized_news table with deduplication via content_hash.
    """
    stmt = text("""
    INSERT INTO charlie.normalized_news (
      asset_id, published_at_utc, source, headline, snippet, url,
      tokens_count, bucket, lang, is_relevant,
      raw_news_id, raw_news_alt_id, content_hash
    ) VALUES (
      :asset_id, :published_at_utc, :source, :headline, :snippet, :url,
      :tokens_count, :bucket, :lang, :is_relevant,
      :raw_news_id, :raw_news_alt_id, :content_hash
    )
    ON CONFLICT (content_hash)
    DO UPDATE SET
      published_at_utc = EXCLUDED.published_at_utc,
      tokens_count = EXCLUDED.tokens_count,
      bucket = EXCLUDED.bucket,
      is_relevant = EXCLUDED.is_relevant
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "published_at_utc": row.get("published_at_utc"),
            "source": row.get("source"),
            "headline": row.get("headline"),
            "snippet": row.get("snippet"),
            "url": row.get("url"),
            "tokens_count": row.get("tokens_count"),
            "bucket": row.get("bucket"),
            "lang": row.get("lang"),
            "is_relevant": row.get("is_relevant"),
            "raw_news_id": row.get("raw_news_id"),
            "raw_news_alt_id": row.get("raw_news_alt_id"),
            "content_hash": row.get("content_hash")
        })

def upsert_raw_news_alt(engine: Engine, row: Dict[str, Any]):
    """
    Insert or update raw_news_alt with content_hash and request_meta support.
    """
    stmt = text("""
    INSERT INTO charlie.raw_news_alt (
      asset_id, source, headline, snippet, language, region, url, published_at,
      fetched_at, sentiment, raw_json, dedupe_hash, is_relevant, bucket,
      tokens_count, file_path, content_hash, request_meta
    ) VALUES (
      :asset_id, :source, :headline, :snippet, :language, :region, :url, :published_at,
      :fetched_at, :sentiment, :raw_json, :dedupe_hash, :is_relevant, :bucket,
      :tokens_count, :file_path, :content_hash, :request_meta
    )
    ON CONFLICT (dedupe_hash)
    DO UPDATE SET
      fetched_at = EXCLUDED.fetched_at,
      raw_json = EXCLUDED.raw_json,
      file_path = EXCLUDED.file_path,
      content_hash = EXCLUDED.content_hash,
      request_meta = EXCLUDED.request_meta
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "source": row.get("source"),
            "headline": row.get("headline"),
            "snippet": row.get("snippet"),
            "language": row.get("language"),
            "region": row.get("region"),
            "url": row.get("url"),
            "published_at": row.get("published_at"),
            "fetched_at": row.get("fetched_at", datetime.utcnow()),
            "sentiment": json.dumps(row.get("sentiment", {})) if row.get("sentiment") else None,
            "raw_json": json.dumps(row.get("raw_json", {})),
            "dedupe_hash": row.get("dedupe_hash"),
            "is_relevant": row.get("is_relevant"),
            "bucket": row.get("bucket"),
            "tokens_count": row.get("tokens_count"),
            "file_path": row.get("file_path"),
            "content_hash": row.get("content_hash"),
            "request_meta": json.dumps(row.get("request_meta", {})) if row.get("request_meta") else None
        })

# -------------------------
# Core data transformations
# -------------------------
def compute_technical_indicators(ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute technical indicators using the ta library (M2 enhancement).
    Indicators: RSI, MACD, Bollinger Bands, ATR, EMA, Ichimoku
    Returns both latest values and short series for context.

    Args:
        ohlcv_df: DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume']

    Returns:
        Dict with 'latest' (snapshot values), 'series' (time series), and 'window_days'
    """
    if ohlcv_df is None or ohlcv_df.empty:
        return {}

    # Validate minimum data requirements
    if len(ohlcv_df) < 5:
        logger.warning(f"Insufficient data for technical indicators: {len(ohlcv_df)} rows")
        return {"error": "insufficient_data", "rows": len(ohlcv_df)}

    df = ohlcv_df.copy().reset_index(drop=True)

    # Ensure numeric types
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Guard against all-NaN columns
    if df['close'].isna().all():
        return {"error": "all_nan_close"}

    window_days = len(df)

    try:
        # Use ta library if available, otherwise fallback to pandas
        if ta is not None:
            # RSI (14-period)
            rsi = ta.momentum.RSIIndicator(close=df['close'], window=14)
            df['rsi_14'] = rsi.rsi()

            # MACD (12, 26, 9)
            macd = ta.trend.MACD(close=df['close'], window_slow=26, window_fast=12, window_sign=9)
            df['macd'] = macd.macd()
            df['macd_signal'] = macd.macd_signal()
            df['macd_diff'] = macd.macd_diff()

            # Bollinger Bands (20, 2)
            bollinger = ta.volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
            df['bb_upper'] = bollinger.bollinger_hband()
            df['bb_mid'] = bollinger.bollinger_mavg()
            df['bb_lower'] = bollinger.bollinger_lband()
            df['bb_width'] = bollinger.bollinger_wband()

            # ATR (14-period)
            atr = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14)
            df['atr_14'] = atr.average_true_range()

            # EMA (various periods)
            df['ema_12'] = ta.trend.EMAIndicator(close=df['close'], window=12).ema_indicator()
            df['ema_26'] = ta.trend.EMAIndicator(close=df['close'], window=26).ema_indicator()
            df['ema_50'] = ta.trend.EMAIndicator(close=df['close'], window=50).ema_indicator() if len(df) >= 50 else None

            # Ichimoku Cloud (requires sufficient data)
            if len(df) >= 52:
                ichimoku = ta.trend.IchimokuIndicator(high=df['high'], low=df['low'], window1=9, window2=26, window3=52)
                df['ichimoku_a'] = ichimoku.ichimoku_a()
                df['ichimoku_b'] = ichimoku.ichimoku_b()
                df['ichimoku_base'] = ichimoku.ichimoku_base_line()
                df['ichimoku_conversion'] = ichimoku.ichimoku_conversion_line()

            # SMA (simple moving averages)
            df['sma_20'] = ta.trend.SMAIndicator(close=df['close'], window=20).sma_indicator()
            df['sma_50'] = ta.trend.SMAIndicator(close=df['close'], window=50).sma_indicator() if len(df) >= 50 else None

        else:
            # Fallback to pandas calculations
            logger.warning("ta library not available, using basic pandas calculations")
            df['close_float'] = df['close'].astype(float)
            df['sma_20'] = df['close_float'].rolling(window=20, min_periods=1).mean()
            df['ema_12'] = df['close_float'].ewm(span=12, adjust=False).mean()
            df['ema_26'] = df['close_float'].ewm(span=26, adjust=False).mean()
            df['macd'] = df['ema_12'] - df['ema_26']

            # Simple RSI
            delta = df['close_float'].diff()
            up = delta.clip(lower=0).rolling(14).mean()
            down = -delta.clip(upper=0).rolling(14).mean()
            df['rsi_14'] = 100 - 100 / (1 + up / down.replace(0, np.nan))

            # Simple Bollinger Bands
            df['bb_mid'] = df['close_float'].rolling(20).mean()
            bb_std = df['close_float'].rolling(20).std()
            df['bb_upper'] = df['bb_mid'] + 2 * bb_std
            df['bb_lower'] = df['bb_mid'] - 2 * bb_std

        # Helper function to safely extract float values
        def safe_float(val):
            if val is None or pd.isna(val):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Build latest snapshot (last row values)
        latest = {
            "close": safe_float(df['close'].iloc[-1]),
            "rsi_14": safe_float(df['rsi_14'].iloc[-1]) if 'rsi_14' in df.columns else None,
            "macd": safe_float(df['macd'].iloc[-1]) if 'macd' in df.columns else None,
            "macd_signal": safe_float(df['macd_signal'].iloc[-1]) if 'macd_signal' in df.columns else None,
            "macd_diff": safe_float(df['macd_diff'].iloc[-1]) if 'macd_diff' in df.columns else None,
            "bb_upper": safe_float(df['bb_upper'].iloc[-1]) if 'bb_upper' in df.columns else None,
            "bb_mid": safe_float(df['bb_mid'].iloc[-1]) if 'bb_mid' in df.columns else None,
            "bb_lower": safe_float(df['bb_lower'].iloc[-1]) if 'bb_lower' in df.columns else None,
            "bb_width": safe_float(df['bb_width'].iloc[-1]) if 'bb_width' in df.columns else None,
            "atr_14": safe_float(df['atr_14'].iloc[-1]) if 'atr_14' in df.columns else None,
            "ema_12": safe_float(df['ema_12'].iloc[-1]) if 'ema_12' in df.columns else None,
            "ema_26": safe_float(df['ema_26'].iloc[-1]) if 'ema_26' in df.columns else None,
            "ema_50": safe_float(df['ema_50'].iloc[-1]) if 'ema_50' in df.columns and df['ema_50'].iloc[-1] is not None else None,
            "sma_20": safe_float(df['sma_20'].iloc[-1]) if 'sma_20' in df.columns else None,
            "sma_50": safe_float(df['sma_50'].iloc[-1]) if 'sma_50' in df.columns and df['sma_50'].iloc[-1] is not None else None,
        }

        # Add Ichimoku if available
        if 'ichimoku_a' in df.columns:
            latest["ichimoku_a"] = safe_float(df['ichimoku_a'].iloc[-1])
            latest["ichimoku_b"] = safe_float(df['ichimoku_b'].iloc[-1])
            latest["ichimoku_base"] = safe_float(df['ichimoku_base'].iloc[-1])
            latest["ichimoku_conversion"] = safe_float(df['ichimoku_conversion'].iloc[-1])

        # Build short series (last 15 data points for context)
        series_length = min(15, len(df))
        series = {
            "close": [safe_float(v) for v in df['close'].iloc[-series_length:].tolist()],
            "rsi_14": [safe_float(v) for v in df['rsi_14'].iloc[-series_length:].tolist()] if 'rsi_14' in df.columns else [],
            "macd": [safe_float(v) for v in df['macd'].iloc[-series_length:].tolist()] if 'macd' in df.columns else [],
            "dates": df['date'].astype(str).iloc[-series_length:].tolist() if 'date' in df.columns else []
        }

        return {
            "latest": latest,
            "series": series,
            "window_days": window_days
        }

    except Exception as e:
        logger.exception("Failed to compute technical indicators")
        return {"error": str(e), "window_days": window_days}

def compute_labels_for_asset(asset_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Implements Algorithm S1 from the paper:
      - Compute EMA (span=3)
      - Forward returns R_tau for tau in {3,7,15}
      - Rolling volatility V_tau (rolling std of R_tau) window 20
      - Normalized S_tau = R_tau / V_tau
      - Weighted signal = 0.3*S3 + 0.5*S7 + 0.2*S15
      - Compute quantiles {0.03,0.15,0.53,0.85} over valid signals and map to classes 1..5
    Expects asset_prices indexed by date ascending with column 'close'
    Returns DataFrame with columns: composite_signal, label_class, quantile
    """
    df = asset_prices.sort_index().copy()
    df['ema3'] = df['close'].ewm(span=3, adjust=False).mean()
    taus = [3,7,15]
    weights = {3:0.3, 7:0.5, 15:0.2}
    signals = pd.Series(index=df.index, dtype=float).fillna(0.0)

    s_components = {}
    for tau in taus:
        R = (df['ema3'].shift(-tau) - df['ema3']) / df['ema3']  # forward return over tau days
        V = R.rolling(window=20, min_periods=10).std()  # volatility estimate
        S = R / V
        s_components[tau] = S
        signals = signals.add(S * weights[tau], fill_value=0)

    valid = signals.dropna()
    if valid.empty:
        df['composite_signal'] = np.nan
        df['label_class'] = None
        df['quantile'] = None
        return df[['composite_signal','label_class','quantile']]

    q03, q15, q53, q85 = valid.quantile([0.03, 0.15, 0.53, 0.85]).tolist()

    def to_label(x):
        if pd.isna(x):
            return None
        if x <= q03:
            return 1
        if x <= q15:
            return 2
        if x <= q53:
            return 3
        if x <= q85:
            return 4
        return 5

    df['composite_signal'] = signals
    df['label_class'] = df['composite_signal'].apply(to_label)
    # compute per-row quantile (relative to distribution)
    df['quantile'] = df['composite_signal'].apply(lambda v: float((valid <= v).sum() / len(valid)) if not pd.isna(v) else None)
    return df[['composite_signal','label_class','quantile']]
