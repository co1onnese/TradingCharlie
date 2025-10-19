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

    # LLM / distillation config
    "LLM": {
        "provider": os.environ.get("CHARLIE_LLM_PROVIDER", "openai"),
        "api_key": os.environ.get("CHARLIE_LLM_API_KEY", None),
        "model": os.environ.get("CHARLIE_LLM_MODEL", "gpt-4o-mini"),
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
    storage = S3Storage(bucket=os.environ.get("TAURIC_S3_BUCKET", "tauric-data"))
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
      fetched_at, raw_json, dedupe_hash, is_relevant, bucket, tokens_count, file_path
    ) VALUES (
      :asset_id, :source, :headline, :snippet, :url, :published_at,
      :fetched_at, :raw_json, :dedupe_hash, :is_relevant, :bucket, :tokens_count, :file_path
    )
    ON CONFLICT (dedupe_hash)
    DO UPDATE SET
      fetched_at = EXCLUDED.fetched_at,
      raw_json = EXCLUDED.raw_json,
      file_path = EXCLUDED.file_path
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
            "file_path": row.get("file_path")
        })

# Keep old name for compatibility
insert_raw_news = upsert_raw_news

def insert_price_window(engine: Engine, row: Dict[str, Any]):
    stmt = text("""
    INSERT INTO charlie.price_window (asset_id, as_of_date, ohlcv_window, technicals, file_path)
    VALUES (:asset_id, :as_of_date, :ohlcv_window, :technicals, :file_path)
    ON CONFLICT (asset_id, as_of_date) DO UPDATE SET
      ohlcv_window = EXCLUDED.ohlcv_window,
      technicals = EXCLUDED.technicals,
      file_path = EXCLUDED.file_path
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "asset_id": row.get("asset_id"),
            "as_of_date": row.get("as_of_date"),
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

# ... add more insert helpers as needed (raw_news_alt, econ events, insider, analyst, etc.)

def insert_assembled_sample(engine: Engine, row: Dict[str, Any]) -> int:
    stmt = text("""
    INSERT INTO charlie.assembled_sample (
      asset_id, as_of_date, variation_id, prompt_path, prompt_blob, prompt_tokens, sources_meta
    ) VALUES (
      :asset_id, :as_of_date, :variation_id, :prompt_path, :prompt_blob, :prompt_tokens, :sources_meta
    )
    ON CONFLICT (asset_id, as_of_date, variation_id)
    DO UPDATE SET
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
# Core data transformations
# -------------------------
def compute_technical_indicators(ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Given a dataframe with columns: ['date','open','high','low','close','volume'] indexed by date asc,
    compute a set of indicators: MA, EMA, MACD, RSI, ATR, Bollinger Bands, Ichimoku
    Return as JSON-able dict.
    """
    # Use pandas to compute technicals. This is intentionally minimal and should be replaced with
    # a robust library (pandas_ta / ta / stockstats) for production parity with the paper.
    if ohlcv_df is None or ohlcv_df.empty:
        return {}

    df = ohlcv_df.copy().reset_index(drop=True)
    df['close'] = df['close'].astype(float)
    # Simple moving averages
    df['ma_5'] = df['close'].rolling(window=5, min_periods=1).mean()
    df['ma_10'] = df['close'].rolling(window=10, min_periods=1).mean()
    df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']
    # RSI (14)
    delta = df['close'].diff()
    up = delta.clip(lower=0).rolling(14).mean()
    down = -delta.clip(upper=0).rolling(14).mean()
    df['rsi_14'] = 100 - 100 / (1 + up / down.replace(0, np.nan))
    # ATR (14)
    df['high_low'] = df['high'] - df['low']
    df['high_close_prev'] = (df['high'] - df['close'].shift(1)).abs()
    df['low_close_prev'] = (df['low'] - df['close'].shift(1)).abs()
    tr = df[['high_low', 'high_close_prev', 'low_close_prev']].max(axis=1)
    df['atr_14'] = tr.rolling(14).mean()
    # Bollinger Bands (20,2)
    df['bb_mid'] = df['close'].rolling(20).mean()
    df['bb_std'] = df['close'].rolling(20).std()
    df['bb_upper'] = df['bb_mid'] + 2 * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - 2 * df['bb_std']

    # Compose a compact JSON structure of last row values and series where helpful
    indicators = {
        "latest": {
            "ma_5": float(df['ma_5'].iloc[-1]),
            "ma_10": float(df['ma_10'].iloc[-1]),
            "macd": float(df['macd'].iloc[-1]) if not pd.isna(df['macd'].iloc[-1]) else None,
            "rsi_14": float(df['rsi_14'].iloc[-1]) if not pd.isna(df['rsi_14'].iloc[-1]) else None,
            "atr_14": float(df['atr_14'].iloc[-1]) if not pd.isna(df['atr_14'].iloc[-1]) else None,
            "bb_upper": float(df['bb_upper'].iloc[-1]) if not pd.isna(df['bb_upper'].iloc[-1]) else None,
            "bb_lower": float(df['bb_lower'].iloc[-1]) if not pd.isna(df['bb_lower'].iloc[-1]) else None
        },
        # Optionally include small series cut (e.g., last 15 closes)
        "series": {
            "close": df['close'].tolist()[-15:],
            "dates": df['date'].astype(str).tolist()[-15:]
        }
    }
    return indicators

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
