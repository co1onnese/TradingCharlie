# charlie_tr1_flow.py
#
# Metaflow pipeline for Charlie-TR1-DB v2 (local filesystem storage)
#
# Usage examples:
#   python charlie_tr1_flow.py run --tickers AAPL,NVDA --start_date 2024-01-01 --end_date 2025-05-31
#   python charlie_tr1_flow.py run --tickers AAPL --as_of_date 2025-03-15 --variation_count 5
#
# Requirements (example):
#   pip install metaflow sqlalchemy psycopg2-binary pandas pyarrow fastparquet python-dateutil requests tqdm

from metaflow import FlowSpec, step, Parameter
from pathlib import Path
import json
import math
import random
from datetime import datetime, timedelta
import pandas as pd
import uuid
from sqlalchemy import text

# Import from our utility modules
from charlie_utils import (
    CONFIG, logger, storage, validate_and_log_config,
    get_db_engine, write_pipeline_run_to_db, upsert_asset,
    insert_raw_news, insert_price_window, insert_raw_fmp_fundamentals,
    insert_raw_eodhd_options, insert_assembled_sample, insert_sample_label,
    insert_distilled_thesis, sha256_hash, save_obj_and_record, date_range,
    truncate_text_for_budget, compute_technical_indicators, compute_labels_for_asset,
    # M1 normalization functions
    compute_content_hash, normalize_to_utc, compute_bucket, check_relevance,
    write_audit, upsert_normalized_news, upsert_raw_news_alt,
    # M3 new modality writers
    insert_insider_txn, insert_analyst_reco, insert_raw_eodhd_econ_events
)

from charlie_fetchers import (
    fetch_yahoo_ohlcv, fetch_finnhub_news, fetch_fmp_fundamentals,
    fetch_newsapi_alt, fetch_google_news, fetch_eodhd_options,
    run_llm_distillation_batch,
    # M3 new modality fetchers
    fetch_insider_transactions, fetch_analyst_recommendations, fetch_edgar_filings,
    fetch_eodhd_econ_events
)

# -------------------------
# Metaflow flow
# -------------------------
class CharlieTR1Pipeline(FlowSpec):
    tickers = Parameter("tickers", default="AAPL,NVDA,MSFT,AMZN,META", help="Comma-separated tickers")
    start_date = Parameter("start_date", default=None, help="YYYY-MM-DD start (optional)")
    end_date = Parameter("end_date", default=None, help="YYYY-MM-DD end (optional)")
    as_of_date = Parameter("as_of_date", default=None, help="If provided, run single date in YYYY-MM-DD")
    seed = Parameter("seed", default=1234, help="Random seed for variation sampling")
    variation_count = Parameter("variation_count", default=20, help="Number of prompt variations per (asset,date)")
    token_budget = Parameter("token_budget", default=8192, help="Target token budget for assembled prompt (approx)")

    @step
    def start(self):
        """
        Validate inputs, compute date ranges and set up artifact directories, write a pipeline_run start record.
        """
        logger.info("Starting CharlieTR1Pipeline")

        # Validate configuration and log API status
        self.api_status = validate_and_log_config()

        random.seed(int(self.seed))
        self.ticker_list = [t.strip().upper() for t in self.tickers.split(",") if t.strip()]
        # compute date list
        if self.as_of_date:
            as_of = datetime.strptime(self.as_of_date, "%Y-%m-%d").date()
            self.date_list = [as_of]
        elif self.start_date and self.end_date:
            s = datetime.strptime(self.start_date, "%Y-%m-%d").date()
            e = datetime.strptime(self.end_date, "%Y-%m-%d").date()
            self.date_list = date_range(s, e)
        else:
            raise Exception("Either as_of_date OR start_date and end_date must be provided.")

        # setup directories
        root = CONFIG["DATA_ROOT"]
        self.raw_dir = str(Path(root) / "raw")
        self.normalized_dir = str(Path(root) / "normalized")
        self.assembled_dir = str(Path(root) / "assembled")
        self.labels_dir = str(Path(root) / "labels")
        self.thesis_dir = str(Path(root) / "distilled_theses")
        self.exports_dir = str(Path(root) / "exports" / "parquet")
        for d in [self.raw_dir, self.normalized_dir, self.assembled_dir, self.labels_dir, self.thesis_dir, self.exports_dir]:
            storage.makedirs(d)

        # Write pipeline_run (initial)
        # Note: Don't store db_engine as instance variable - SQLAlchemy engines can't be pickled by Metaflow
        db_engine = get_db_engine()
        self.run_meta = {
            "run_name": f"charlie_run_{uuid.uuid4().hex[:8]}",
            "run_type": "full_backfill" if len(self.date_list) > 1 else "single_date",
            "started_at": datetime.utcnow(),
            "finished_at": None,
            "status": "running",
            "seed": int(self.seed),
            "config": CONFIG,
            "artifacts": {},
            "meta": {"tickers": self.ticker_list, "dates": [str(d) for d in self.date_list]}
        }
        try:
            self.run_id = write_pipeline_run_to_db(db_engine, self.run_meta)
        except Exception as e:
            logger.exception("Could not write pipeline_run to DB; continuing without DB record.")
            self.run_id = None

        # Kick off per-ticker foreach (Metaflow parallelism)
        # Note: foreach argument must be a string attribute name, not the list itself
        self.next(self.ingest_raw, foreach="ticker_list")

    @step
    def ingest_raw(self):
        """
        For a single ticker (self.input), fetch all raw data for the configured dates, store files and insert raw rows into DB.
        This step runs in parallel across tickers (Metaflow foreach)
        """
        ticker = self.input
        logger.info(f"[ingest_raw] ticker={ticker}")
        self.ticker = ticker
        db_engine = get_db_engine()
        self.asset_id = upsert_asset(db_engine, ticker)

        # create ticker-specific directories
        raw_ticker_dir = Path(self.raw_dir) / ticker
        storage.makedirs(str(raw_ticker_dir))

        # main ingestion loop across dates
        self.fetched_manifest = []
        for as_of_date in self.date_list:
            # FINNHUB news
            try:
                news_items = fetch_finnhub_news(ticker, as_of_date, CONFIG["FINNHUB_API_KEY"])
                for i, item in enumerate(news_items):
                    filename = f"{ticker}/{as_of_date}/finnhub_news_{i}.json"
                    file_path = save_obj_and_record(storage, item, str(Path("raw") / "finnhub" / ticker / str(as_of_date)), f"article_{i}.json")
                    dedupe_hash = sha256_hash((item.get("headline","") + item.get("url","") + str(item.get("published_at",""))))
                    row = {
                        "asset_id": self.asset_id,
                        "source": "finnhub",
                        "headline": item.get("headline"),
                        "snippet": item.get("snippet", ""),
                        "url": item.get("url"),
                        "published_at": item.get("published_at"),
                        "fetched_at": datetime.utcnow(),
                        "raw_json": item,
                        "dedupe_hash": dedupe_hash,
                        "is_relevant": None,
                        "bucket": None,
                        "tokens_count": None,
                        "file_path": file_path
                    }
                    try:
                        insert_raw_news(db_engine, row)
                    except Exception:
                        logger.exception("Failed to insert raw_news row")
                # end for news_items
            except Exception:
                logger.exception("Finnhub fetch failed")

            # Google news (same structure)
            try:
                gnews_items = fetch_google_news(ticker, as_of_date, CONFIG["SERPAPI_KEY"])
                for i, item in enumerate(gnews_items):
                    file_path = save_obj_and_record(storage, item, str(Path("raw") / "google_news" / ticker / str(as_of_date)), f"article_{i}.json")
                    dedupe_hash = sha256_hash((item.get("headline","") + item.get("url","") + str(item.get("published_at",""))))
                    row = {
                        "asset_id": self.asset_id,
                        "source": "google_news",
                        "headline": item.get("headline"),
                        "snippet": item.get("snippet", ""),
                        "url": item.get("url"),
                        "published_at": item.get("published_at"),
                        "fetched_at": datetime.utcnow(),
                        "raw_json": item,
                        "dedupe_hash": dedupe_hash,
                        "is_relevant": None,
                        "bucket": None,
                        "tokens_count": None,
                        "file_path": file_path
                    }
                    insert_raw_news(db_engine, row)
            except Exception:
                logger.exception("Google News fetch failed")

            # Alternative news (NewsAPI / Webz / GDELT)
            try:
                alt_items = fetch_newsapi_alt(ticker, as_of_date, CONFIG["NEWSAPI_KEY"])
                for i, item in enumerate(alt_items):
                    file_path = save_obj_and_record(storage, item, str(Path("raw") / "news_alt" / ticker / str(as_of_date)), f"article_{i}.json")
                    dedupe = sha256_hash((item.get("headline","") + item.get("url","") + str(item.get("published_at",""))))
                    stmt = {
                        "asset_id": self.asset_id,
                        "source": item.get("source","newsapi"),
                        "headline": item.get("headline"),
                        "snippet": item.get("snippet"),
                        "language": item.get("language"),
                        "region": item.get("region"),
                        "url": item.get("url"),
                        "published_at": item.get("published_at"),
                        "fetched_at": datetime.utcnow(),
                        "sentiment": item.get("sentiment"),
                        "raw_json": item,
                        "dedupe_hash": dedupe,
                        "is_relevant": None,
                        "bucket": None,
                        "tokens_count": None,
                        "file_path": file_path
                    }
                    try:
                        # direct insert SQL omitted for brevity - use analog to insert_raw_news
                        pass
                    except Exception:
                        logger.exception("Failed to insert raw_news_alt")
            except Exception:
                logger.exception("Alt news fetch failed")

            # Price / technical data (Yahoo)
            try:
                ohlcv_result = fetch_yahoo_ohlcv(ticker, as_of_date)
                # Extract data list from result
                ohlcv_window = ohlcv_result.get("data", []) if ohlcv_result else []

                if ohlcv_window:
                    file_path = save_obj_and_record(storage, ohlcv_window, str(Path("raw") / "yahoo_price" / ticker / str(as_of_date)), f"ohlcv_{as_of_date}.json")
                    # Store in DB
                    insert_price_window(db_engine, {
                        "asset_id": self.asset_id,
                        "as_of_date": as_of_date,
                        "ohlcv_window": ohlcv_window,
                        "technicals": {},  # fill later in compute_technicals step
                        "file_path": file_path
                    })
                else:
                    logger.warning(f"No OHLCV data returned for {ticker} on {as_of_date}")
            except Exception:
                logger.exception("Yahoo OHLCV fetch failed")

            # FMP fundamentals
            try:
                fmp_rows = fetch_fmp_fundamentals(ticker, as_of_date - timedelta(days=365*2), CONFIG["FMP_API_KEY"])
                for fr in fmp_rows:
                    file_path = save_obj_and_record(storage, fr, str(Path("raw") / "fmp" / ticker / str(fr.get("report_date","unknown"))), "report.json")
                    insert_raw_fmp_fundamentals(db_engine, {
                        "asset_id": self.asset_id,
                        "report_date": fr.get("report_date"),
                        "period_type": fr.get("period_type"),
                        "currency": fr.get("currency"),
                        "raw_json": fr,
                        "normalized": fr.get("normalized", {}),
                        "source_url": fr.get("source_url"),
                        "file_path": file_path,
                        "fetched_at": datetime.utcnow()
                    })
            except Exception:
                logger.exception("FMP fundamentals fetch failed")

            # EODHD options & economic events
            try:
                options = fetch_eodhd_options(ticker, as_of_date, CONFIG["EODHD_API_KEY"])
                for opt in options:
                    file_path = save_obj_and_record(storage, opt, str(Path("raw") / "eodhd_options" / ticker / str(as_of_date)), f"opt_{opt.get('expiration')}_{opt.get('strike')}.json")
                    insert_raw_eodhd_options(db_engine, {
                        "asset_id": self.asset_id,
                        "as_of_date": as_of_date,
                        "expiration": opt.get("expiration"),
                        "option_type": opt.get("option_type"),
                        "strike": opt.get("strike"),
                        "open_interest": opt.get("open_interest"),
                        "implied_vol": opt.get("implied_vol"),
                        "underlying_price": opt.get("underlying_price"),
                        "raw_json": opt,
                        "file_path": file_path,
                        "fetched_at": datetime.utcnow()
                    })
            except Exception:
                logger.exception("EODHD options fetch failed")

            # M3: Insider transactions
            try:
                insider_txns = fetch_insider_transactions(ticker, as_of_date, CONFIG["FMP_API_KEY"])
                for txn in insider_txns:
                    file_path = save_obj_and_record(storage, txn, str(Path("raw") / "insider" / ticker / str(as_of_date)), f"insider_{txn.get('filing_date')}.json")
                    insert_insider_txn(db_engine, {
                        "asset_id": self.asset_id,
                        "filing_date": txn.get("filing_date"),
                        "transaction_type": txn.get("transaction_type"),
                        "shares": txn.get("shares"),
                        "amount": txn.get("amount"),
                        "mspr": txn.get("mspr"),
                        "raw_json": txn,
                        "file_path": file_path
                    })
            except Exception:
                logger.exception("Insider transactions fetch failed")

            # M3: Analyst recommendations
            try:
                analyst_recos = fetch_analyst_recommendations(ticker, as_of_date, CONFIG["FMP_API_KEY"])
                for reco in analyst_recos:
                    file_path = save_obj_and_record(storage, reco, str(Path("raw") / "analyst" / ticker / str(as_of_date)), f"analyst_{reco.get('reco_date')}.json")
                    insert_analyst_reco(db_engine, {
                        "asset_id": self.asset_id,
                        "reco_date": reco.get("reco_date"),
                        "consensus_rating": reco.get("consensus_rating"),
                        "firm": reco.get("firm"),
                        "raw_json": reco,
                        "file_path": file_path
                    })
            except Exception:
                logger.exception("Analyst recommendations fetch failed")

            # M3: Economic events (macro) - fetched once per date, not per ticker
            # Only fetch on first ticker to avoid duplicates
            if ticker == self.ticker_list[0]:
                try:
                    # Fetch economic events for a 30-day window around as_of_date
                    event_start = as_of_date - timedelta(days=30)
                    econ_events = fetch_eodhd_econ_events(event_start, as_of_date, CONFIG["EODHD_API_KEY"])
                    for event in econ_events:
                        file_path = save_obj_and_record(storage, event, str(Path("raw") / "macro" / str(as_of_date)), f"event_{event.get('date')}_{event.get('event_name', 'event')}.json")
                        insert_raw_eodhd_econ_events(db_engine, {
                            "event_date": event.get("date"),
                            "country": event.get("country"),
                            "category": event.get("category"),
                            "event_name": event.get("event_name"),
                            "importance": event.get("importance"),
                            "actual": event.get("actual"),
                            "forecast": event.get("forecast"),
                            "previous": event.get("previous"),
                            "raw_json": event,
                            "file_path": file_path,
                            "fetched_at": datetime.utcnow()
                        })
                except Exception:
                    logger.exception("Economic events fetch failed")

            # SEC EDGAR filings (stub - will log but not insert data until implemented)
            try:
                edgar_filings = fetch_edgar_filings(ticker, as_of_date)
                # When implemented, this would insert into a raw_sec_filings table
            except Exception:
                logger.exception("SEC EDGAR filings fetch failed")

            # FRED & SimFin could be fetched once per run or per-date depending on scope

            # record manifest entry
            self.fetched_manifest.append({"date": str(as_of_date), "ticker": ticker})

        # end for dates
        # Persist ticker-level manifest
        manifest_path = str(Path("raw") / "manifests" / ticker / f"manifest_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json")
        full_manifest_path = save_obj_and_record(storage, self.fetched_manifest, manifest_path, "manifest.json")
        self.manifest_path = full_manifest_path

        # next step: normalize/dedupe for this ticker
        self.next(self.normalize_dedupe)

    @step
    def normalize_dedupe(self):
        """
        Normalize raw data and populate normalized_news table.
        - Cross-source deduplication using content_hash
        - Timezone normalization to UTC
        - Bucket computation (0-3, 4-10, 11-30 days)
        - Relevance filtering
        - Quality checks with audit logging
        """
        ticker = self.ticker
        logger.info(f"[normalize_dedupe] ticker={ticker}")
        engine = get_db_engine()

        # Get asset metadata for relevance checking
        with engine.connect() as conn:
            asset_row = conn.execute(text("SELECT ticker, name FROM charlie.asset WHERE asset_id = :aid"),
                                    {"aid": self.asset_id}).fetchone()
            asset_ticker = asset_row[0] if asset_row else ticker
            asset_name = asset_row[1] if asset_row else None

        # Track deduplication stats
        stats = {
            "raw_news_processed": 0,
            "raw_news_alt_processed": 0,
            "duplicates_found": 0,
            "relevance_filtered": 0,
            "quality_failures": 0,
            "normalized_inserted": 0
        }

        # Process raw_news table
        with engine.connect() as conn:
            raw_news_rows = conn.execute(text("""
                SELECT news_id, source, headline, snippet, url, published_at, raw_json
                FROM charlie.raw_news
                WHERE asset_id = :aid
                ORDER BY published_at DESC
            """), {"aid": self.asset_id}).fetchall()

            for row in raw_news_rows:
                stats["raw_news_processed"] += 1
                news_id, source, headline, snippet, url, published_at, raw_json = row

                try:
                    # Compute content_hash for deduplication
                    content_hash = compute_content_hash(headline, url, published_at)

                    # Normalize timestamp to UTC
                    published_at_utc = normalize_to_utc(published_at)
                    if not published_at_utc:
                        stats["quality_failures"] += 1
                        write_audit(engine, "raw_news", str(news_id), "quality_fail",
                                  {"reason": "invalid_timestamp", "headline": headline})
                        continue

                    # Check relevance
                    is_relevant = check_relevance(headline, snippet, asset_ticker, asset_name)
                    if not is_relevant:
                        stats["relevance_filtered"] += 1
                        # Still insert but mark as not relevant for filtering later

                    # Compute token count
                    text_content = (headline or "") + " " + (snippet or "")
                    tokens_count = math.ceil(len(text_content) / 4)

                    # Process for each as_of_date in the pipeline
                    for as_of_date in self.date_list:
                        bucket = compute_bucket(published_at_utc, as_of_date)
                        if bucket is None:
                            continue  # Article outside valid window for this date

                        # Prepare normalized row
                        normalized_row = {
                            "asset_id": self.asset_id,
                            "published_at_utc": published_at_utc,
                            "source": source,
                            "headline": headline,
                            "snippet": snippet,
                            "url": url,
                            "tokens_count": tokens_count,
                            "bucket": bucket,
                            "lang": "en",  # TODO: Add language detection
                            "is_relevant": is_relevant,
                            "raw_news_id": news_id,
                            "raw_news_alt_id": None,
                            "content_hash": content_hash
                        }

                        # Upsert to normalized_news
                        try:
                            upsert_normalized_news(engine, normalized_row)
                            stats["normalized_inserted"] += 1
                        except Exception as e:
                            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                                stats["duplicates_found"] += 1
                                write_audit(engine, "normalized_news", content_hash, "dedupe",
                                          {"source": source, "headline": headline[:100]})
                            else:
                                logger.exception(f"Failed to upsert normalized_news for news_id={news_id}")

                except Exception as e:
                    stats["quality_failures"] += 1
                    logger.exception(f"Failed to normalize raw_news row {news_id}")

        # Process raw_news_alt table
        with engine.connect() as conn:
            raw_alt_rows = conn.execute(text("""
                SELECT alt_news_id, source, headline, snippet, url, published_at, language, raw_json
                FROM charlie.raw_news_alt
                WHERE asset_id = :aid
                ORDER BY published_at DESC
            """), {"aid": self.asset_id}).fetchall()

            for row in raw_alt_rows:
                stats["raw_news_alt_processed"] += 1
                alt_id, source, headline, snippet, url, published_at, lang, raw_json = row

                try:
                    # Compute content_hash for cross-source deduplication
                    content_hash = compute_content_hash(headline, url, published_at)

                    # Normalize timestamp
                    published_at_utc = normalize_to_utc(published_at)
                    if not published_at_utc:
                        stats["quality_failures"] += 1
                        continue

                    # Check relevance
                    is_relevant = check_relevance(headline, snippet, asset_ticker, asset_name)
                    if not is_relevant:
                        stats["relevance_filtered"] += 1

                    # Compute token count
                    text_content = (headline or "") + " " + (snippet or "")
                    tokens_count = math.ceil(len(text_content) / 4)

                    # Process for each as_of_date
                    for as_of_date in self.date_list:
                        bucket = compute_bucket(published_at_utc, as_of_date)
                        if bucket is None:
                            continue

                        normalized_row = {
                            "asset_id": self.asset_id,
                            "published_at_utc": published_at_utc,
                            "source": source,
                            "headline": headline,
                            "snippet": snippet,
                            "url": url,
                            "tokens_count": tokens_count,
                            "bucket": bucket,
                            "lang": lang or "en",
                            "is_relevant": is_relevant,
                            "raw_news_id": None,
                            "raw_news_alt_id": alt_id,
                            "content_hash": content_hash
                        }

                        try:
                            upsert_normalized_news(engine, normalized_row)
                            stats["normalized_inserted"] += 1
                        except Exception as e:
                            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                                stats["duplicates_found"] += 1
                                write_audit(engine, "normalized_news", content_hash, "dedupe",
                                          {"source": source, "headline": headline[:100]})
                            else:
                                logger.exception(f"Failed to upsert normalized_news for alt_news_id={alt_id}")

                except Exception as e:
                    stats["quality_failures"] += 1
                    logger.exception(f"Failed to normalize raw_news_alt row {alt_id}")

        # Log normalization stats
        logger.info(f"[normalize_dedupe] {ticker} stats: {stats}")
        self.normalization_stats = stats

        self.next(self.compute_technicals)

    # @batch(cpu=4, memory=16000)  # Requires S3 datastore - disabled for local mode
    # @retry(times=2, minutes_between_retries=1)
    @step
    def compute_technicals(self):
        """
        For this ticker, read price_window rows, compute technicals, and update price_window table.
        """
        ticker = self.ticker
        logger.info(f"[compute_technicals] ticker={ticker}")
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT price_window_id, as_of_date, ohlcv_window, file_path FROM charlie.price_window WHERE asset_id = :aid"),
                                {"aid": self.asset_id}).fetchall()
            for r in rows:
                pw_id, as_of_date, ohlcv_json, fp = r
                if not ohlcv_json:
                    continue
                # Convert to DataFrame
                try:
                    # ohlcv_json expected: list of dicts {date,open,high,low,close,volume}
                    df = pd.DataFrame(ohlcv_json)
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        df = df.sort_values('date')
                    indicators = compute_technical_indicators(df)
                    # Extract window_days from indicators
                    window_days = indicators.get("window_days", len(df))
                    # update DB - use insert_price_window to upsert technicals
                    insert_price_window(get_db_engine(), {
                        "asset_id": self.asset_id,
                        "as_of_date": as_of_date,
                        "window_days": window_days,
                        "ohlcv_window": ohlcv_json,
                        "technicals": indicators,
                        "file_path": fp
                    })
                    # Save normalized technicals locally
                    technicals_path = save_obj_and_record(storage, indicators, str(Path("normalized") / "price_window" / ticker / str(as_of_date)), f"technicals_{pw_id}.json")
                except Exception:
                    logger.exception("Failed to compute technicals for price_window_id=%s", pw_id)
        self.next(self.assemble_samples)

    # @batch(cpu=4, memory=16000)  # Requires S3 datastore - disabled for local mode
    @step
    def assemble_samples(self):
        """
        M2 Enhancement: Assemble samples with strict as-of integrity and multi-modality support.
        - News: filtered by bucket windows (0-3, 4-10, 11-30 days)
        - Fundamentals: latest report ≤ as_of_date
        - Options: same-day or nearest prior
        - Macro: events up to as_of_date
        - Per-modality quotas with deterministic sampling
        - Rich sources_meta tracking
        """
        ticker = self.ticker
        logger.info(f"[assemble_samples] ticker={ticker}")
        engine = get_db_engine()
        quotas = CONFIG.get("ASSEMBLY_QUOTAS", {})
        assembled_records = []

        for as_of_date in self.date_list:
            # Set as_of_cutoff to end of day for strict temporal filtering
            as_of_cutoff = datetime.combine(as_of_date, datetime.max.time()).replace(microsecond=0)

            # ===== GATHER ALL MODALITIES WITH AS-OF FILTERING =====

            # 1. Technicals (price data and indicators)
            try:
                with engine.connect() as conn:
                    res = conn.execute(text("""
                        SELECT ohlcv_window, technicals, window_days
                        FROM charlie.price_window
                        WHERE asset_id = :aid AND as_of_date = :d
                    """), {"aid": self.asset_id, "d": as_of_date}).fetchone()
                ohlcv_window = res[0] if res else []
                technicals = res[1] if res else {}
                window_days = res[2] if res and res[2] else len(ohlcv_window) if ohlcv_window else 0
            except Exception:
                logger.exception("Failed to fetch price_window")
                ohlcv_window, technicals, window_days = [], {}, 0

            # 2. News from normalized_news (bucketed, relevant only)
            news_by_bucket = {"0-3": [], "4-10": [], "11-30": []}
            try:
                with engine.connect() as conn:
                    news_rows = conn.execute(text("""
                        SELECT bucket, headline, snippet, source, published_at_utc, url, tokens_count
                        FROM charlie.normalized_news
                        WHERE asset_id = :aid
                          AND published_at_utc <= :cutoff
                          AND bucket IS NOT NULL
                          AND is_relevant = true
                        ORDER BY published_at_utc DESC
                    """), {"aid": self.asset_id, "cutoff": as_of_cutoff}).fetchall()

                for row in news_rows:
                    bucket, headline, snippet, source, pub_at, url, tokens = row
                    if bucket in news_by_bucket:
                        news_by_bucket[bucket].append({
                            "headline": headline,
                            "snippet": snippet,
                            "source": source,
                            "published_at": str(pub_at),
                            "url": url,
                            "tokens": tokens or 0
                        })
            except Exception:
                logger.exception("Failed to fetch normalized_news")

            # 3. Fundamentals (latest reports ≤ as_of_date)
            fundamentals_data = []
            try:
                with engine.connect() as conn:
                    fund_rows = conn.execute(text("""
                        SELECT report_date, period_type, normalized, source_url
                        FROM charlie.raw_fmp_fundamentals
                        WHERE asset_id = :aid AND report_date <= :d
                        ORDER BY report_date DESC
                        LIMIT :limit
                    """), {"aid": self.asset_id, "d": as_of_date, "limit": quotas.get("max_fundamentals", 3)}).fetchall()

                for row in fund_rows:
                    fundamentals_data.append({
                        "report_date": str(row[0]),
                        "period_type": row[1],
                        "data": row[2] if row[2] else {},
                        "source": "fmp"
                    })
            except Exception:
                logger.exception("Failed to fetch fundamentals")

            # 4. Options (same-day or nearest prior)
            options_data = []
            try:
                with engine.connect() as conn:
                    opt_rows = conn.execute(text("""
                        SELECT expiration, option_type, strike, open_interest, implied_vol, underlying_price
                        FROM charlie.raw_eodhd_options
                        WHERE asset_id = :aid AND as_of_date <= :d
                        ORDER BY as_of_date DESC
                        LIMIT :limit
                    """), {"aid": self.asset_id, "d": as_of_date, "limit": quotas.get("max_options", 25)}).fetchall()

                for row in opt_rows:
                    options_data.append({
                        "expiration": str(row[0]),
                        "type": row[1],
                        "strike": float(row[2]) if row[2] else None,
                        "open_interest": float(row[3]) if row[3] else None,
                        "iv": float(row[4]) if row[4] else None,
                        "underlying": float(row[5]) if row[5] else None
                    })
            except Exception:
                logger.exception("Failed to fetch options")

            # 5. Macro events (up to as_of_date)
            macro_events = []
            try:
                with engine.connect() as conn:
                    macro_rows = conn.execute(text("""
                        SELECT event_date, country, category, event_name, importance, actual, forecast, previous
                        FROM charlie.raw_eodhd_economic_events
                        WHERE event_date <= :d
                        ORDER BY event_date DESC, importance DESC
                        LIMIT :limit
                    """), {"d": as_of_date, "limit": quotas.get("max_macro_events", 5)}).fetchall()

                for row in macro_rows:
                    macro_events.append({
                        "date": str(row[0]),
                        "country": row[1],
                        "category": row[2],
                        "event": row[3],
                        "importance": row[4],
                        "actual": row[5],
                        "forecast": row[6],
                        "previous": row[7]
                    })
            except Exception:
                logger.exception("Failed to fetch macro events")

            # 6. Insider transactions
            insider_txns = []
            try:
                with engine.connect() as conn:
                    insider_rows = conn.execute(text("""
                        SELECT filing_date, transaction_type, shares, amount, mspr
                        FROM charlie.insider_txn
                        WHERE asset_id = :aid AND filing_date <= :d
                        ORDER BY filing_date DESC
                        LIMIT :limit
                    """), {"aid": self.asset_id, "d": as_of_date, "limit": quotas.get("max_insider_txns", 10)}).fetchall()

                for row in insider_rows:
                    insider_txns.append({
                        "date": str(row[0]),
                        "type": row[1],
                        "shares": float(row[2]) if row[2] else None,
                        "amount": float(row[3]) if row[3] else None,
                        "mspr": float(row[4]) if row[4] else None
                    })
            except Exception:
                logger.exception("Failed to fetch insider transactions")

            # 7. Analyst recommendations
            analyst_recos = []
            try:
                with engine.connect() as conn:
                    analyst_rows = conn.execute(text("""
                        SELECT reco_date, consensus_rating, firm
                        FROM charlie.analyst_reco
                        WHERE asset_id = :aid AND reco_date <= :d
                        ORDER BY reco_date DESC
                        LIMIT :limit
                    """), {"aid": self.asset_id, "d": as_of_date, "limit": quotas.get("max_analyst_recos", 5)}).fetchall()

                for row in analyst_rows:
                    analyst_recos.append({
                        "date": str(row[0]),
                        "rating": row[1],
                        "firm": row[2]
                    })
            except Exception:
                logger.exception("Failed to fetch analyst recommendations")

            # ===== CREATE VARIATIONS WITH DETERMINISTIC SAMPLING =====
            for var_id in range(1, int(self.variation_count) + 1):
                random.seed(int(self.seed) + var_id + hash(f"{ticker}{as_of_date}"))

                # Sample news per bucket respecting quotas
                sampled_news = {}
                news_sources = set()
                total_news_count = 0
                for bucket, items in news_by_bucket.items():
                    quota = quotas.get("news_per_bucket", {}).get(bucket, 10)
                    sampled = random.sample(items, min(quota, len(items))) if items else []
                    sampled_news[bucket] = sampled
                    total_news_count += len(sampled)
                    news_sources.update([item["source"] for item in sampled])

                # Build prompt sections
                prompt_parts = []
                prompt_parts.append(f"=== INVESTMENT ANALYSIS: {ticker} ===")
                prompt_parts.append(f"As-of Date: {as_of_date}")
                prompt_parts.append(f"As-of Cutoff: {as_of_cutoff}\n")

                # Technicals
                if technicals:
                    prompt_parts.append("## Technical Indicators")
                    latest = technicals.get("latest", {})
                    prompt_parts.append(f"Close: ${latest.get('close', 'N/A')}")
                    prompt_parts.append(f"RSI(14): {latest.get('rsi_14', 'N/A')}")
                    prompt_parts.append(f"MACD: {latest.get('macd', 'N/A')}")
                    prompt_parts.append(f"Bollinger Bands: [{latest.get('bb_lower', 'N/A')}, {latest.get('bb_upper', 'N/A')}]")
                    prompt_parts.append(f"ATR(14): {latest.get('atr_14', 'N/A')}\n")

                # News by bucket
                for bucket in ["0-3", "4-10", "11-30"]:
                    items = sampled_news.get(bucket, [])
                    if items:
                        prompt_parts.append(f"## News ({bucket} days ago)")
                        for item in items[:5]:  # Limit display
                            prompt_parts.append(f"- [{item['source']}] {item['headline']}")

                # Fundamentals
                if fundamentals_data:
                    prompt_parts.append("\n## Fundamentals")
                    for fund in fundamentals_data[:2]:
                        prompt_parts.append(f"Report Date: {fund['report_date']} ({fund['period_type']})")

                # Options summary
                if options_data:
                    prompt_parts.append(f"\n## Options (Top {len(options_data)} contracts)")
                    prompt_parts.append(f"Avg IV: {sum(o['iv'] for o in options_data if o['iv'])/len([o for o in options_data if o['iv']]):.2f}%" if any(o['iv'] for o in options_data) else "IV: N/A")

                # Macro
                if macro_events:
                    prompt_parts.append("\n## Macro Events")
                    for event in macro_events[:3]:
                        prompt_parts.append(f"- {event['date']}: {event['event']} ({event['importance']})")

                # Insider
                if insider_txns:
                    prompt_parts.append(f"\n## Insider Activity ({len(insider_txns)} transactions)")

                # Analyst
                if analyst_recos:
                    prompt_parts.append(f"\n## Analyst Recommendations ({len(analyst_recos)} ratings)")

                prompt_text = "\n".join(prompt_parts)
                truncated_text, token_count = truncate_text_for_budget(prompt_text, token_budget=int(self.token_budget))

                # Build rich sources_meta
                sources_meta = {
                    "news": {
                        "total_count": total_news_count,
                        "by_bucket": {b: len(sampled_news.get(b, [])) for b in ["0-3", "4-10", "11-30"]},
                        "sources": list(news_sources)
                    },
                    "technicals": {"window_days": window_days, "included": bool(technicals)},
                    "fundamentals": {"count": len(fundamentals_data), "sources": ["fmp"] if fundamentals_data else []},
                    "options": {"count": len(options_data), "included": len(options_data) > 0},
                    "macro": {"count": len(macro_events), "sources": ["eodhd"] if macro_events else []},
                    "insider": {"count": len(insider_txns)},
                    "analyst": {"count": len(analyst_recos)}
                }

                # Save prompt
                prompt_dir = str(Path("assembled") / ticker / str(as_of_date))
                filename = f"prompt_var{var_id}.json"
                prompt_payload = {
                    "ticker": ticker,
                    "as_of_date": str(as_of_date),
                    "as_of_cutoff": str(as_of_cutoff),
                    "variation_id": var_id,
                    "prompt": truncated_text,
                    "sources_meta": sources_meta
                }
                prompt_path = save_obj_and_record(storage, prompt_payload, prompt_dir, filename)

                # Insert assembled_sample
                sample_id = insert_assembled_sample(engine, {
                    "asset_id": self.asset_id,
                    "as_of_date": as_of_date,
                    "variation_id": var_id,
                    "run_id": self.run_id,
                    "as_of_cutoff": as_of_cutoff,
                    "prompt_path": prompt_path,
                    "prompt_blob": truncated_text[:4000],
                    "prompt_tokens": token_count,
                    "sources_meta": sources_meta
                })
                assembled_records.append(sample_id)

        self.assembled_sample_ids = assembled_records
        logger.info(f"[assemble_samples] {ticker}: created {len(assembled_records)} samples")
        self.next(self.generate_labels)

    # @batch(cpu=4, memory=16000)  # Requires S3 datastore - disabled for local mode
    @step
    def generate_labels(self):
        """
        M2 Enhancement: Generate labels with per-asset quantiles.
        - Uses full price history for accurate quantile computation
        - Asymmetric cutoffs: {3%, 15%, 53%, 85%}
        - Strict forward-looking windows (no leakage)
        - Skip samples where labels unavailable
        """
        ticker = self.ticker
        logger.info(f"[generate_labels] ticker={ticker}")
        engine = get_db_engine()

        labels_inserted = 0
        labels_skipped = 0

        try:
            # Extract close prices by date using price_window records
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT as_of_date, ohlcv_window
                    FROM charlie.price_window
                    WHERE asset_id = :aid
                    ORDER BY as_of_date ASC
                """), {"aid": self.asset_id}).fetchall()

            price_series = []
            for r in rows:
                as_of = r[0]
                ohlcv = r[1] or []
                # Take last close of the window as representative close for as_of date
                if isinstance(ohlcv, list) and len(ohlcv) > 0:
                    last = ohlcv[-1]
                    price_series.append({"date": as_of, "close": last.get("close")})

            if not price_series:
                logger.warning(f"[generate_labels] {ticker}: No price history available, skipping labels")
                self.labels_stats = {"inserted": 0, "skipped": len(self.assembled_sample_ids), "reason": "no_prices"}
                self.next(self.distill_theses)
                return

            # Build price DataFrame and compute labels per Algorithm S1
            price_df = pd.DataFrame(price_series)
            price_df['date'] = pd.to_datetime(price_df['date'])
            price_df = price_df.set_index('date')
            price_df = price_df.sort_index()

            # Compute labels for this asset (per-asset quantiles with asymmetric cutoffs)
            labeled = compute_labels_for_asset(price_df)

            # Assign labels to assembled samples
            for sample_id in self.assembled_sample_ids:
                try:
                    # Fetch assembled_sample row
                    with engine.connect() as conn:
                        res = conn.execute(text("""
                            SELECT sample_id, as_of_date
                            FROM charlie.assembled_sample
                            WHERE sample_id = :sid
                        """), {"sid": sample_id}).fetchone()

                    if not res:
                        labels_skipped += 1
                        continue

                    sid, as_of_date = res
                    dt = pd.to_datetime(as_of_date)

                    # Check if label exists for this date
                    if dt not in labeled.index:
                        labels_skipped += 1
                        logger.debug(f"No label available for sample_id={sid}, date={dt}")
                        continue

                    row = labeled.loc[dt]

                    # Skip if label is invalid (NaN or None)
                    if pd.isna(row['composite_signal']) or row['label_class'] is None:
                        labels_skipped += 1
                        logger.debug(f"Invalid label for sample_id={sid}, date={dt}")
                        continue

                    # Insert label
                    insert_sample_label(engine, {
                        "sample_id": sid,
                        "composite_signal": float(row['composite_signal']),
                        "label_class": int(row['label_class']),
                        "quantile": float(row['quantile']) if not pd.isna(row['quantile']) else None,
                        "computed_at": datetime.utcnow()
                    })
                    labels_inserted += 1

                except Exception:
                    logger.exception(f"Failed to insert sample_label for sample_id={sample_id}")
                    labels_skipped += 1

        except Exception:
            logger.exception(f"Label generation failed for ticker={ticker}")
            labels_skipped = len(self.assembled_sample_ids)

        # Store stats
        self.labels_stats = {
            "inserted": labels_inserted,
            "skipped": labels_skipped,
            "total": len(self.assembled_sample_ids)
        }
        logger.info(f"[generate_labels] {ticker}: {labels_inserted} labels inserted, {labels_skipped} skipped")

        self.next(self.distill_theses)

    # @batch(cpu=4, memory=24000)  # Requires S3 datastore - disabled for local mode
    @step
    def distill_theses(self):
        """
        For cost-control, we may choose to distill only a subset of assembled samples (e.g., sample 10% or first N).
        This step calls LLM distillation in batches and writes distilled_thesis rows + files.
        """
        ticker = self.ticker
        logger.info(f"[distill_theses] ticker={ticker}")
        engine = get_db_engine()
        # fetch assembled samples for this ticker
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT sample_id, prompt_path FROM charlie.assembled_sample WHERE asset_id = :aid"), {"aid": self.asset_id}).fetchall()
        prompts = []
        # naive sampling: take every nth sample to keep costs down; adjust strategy as needed
        sample_every = max(1, int(len(rows) / 50))  # target ~50 distilled samples per ticker (tunable)
        for idx, r in enumerate(rows):
            sample_id, prompt_path = r
            if idx % sample_every != 0:
                continue
            # load prompt text
            try:
                payload = storage.read_json(prompt_path)
                prompt_text = payload.get("prompt", "")
                prompts.append({"sample_id": sample_id, "prompt_text": prompt_text})
            except Exception:
                logger.exception("Failed to read prompt for distillation: %s", prompt_path)

        # batch LLM calls
        outputs = []
        batch_size = int(CONFIG["LLM"].get("batch_size", 8))
        for i in range(0, len(prompts), batch_size):
            batch_prompts = prompts[i:i+batch_size]
            try:
                batch_out = run_llm_distillation_batch(batch_prompts, CONFIG["LLM"])
                outputs.extend(batch_out)
            except Exception:
                logger.exception("LLM distillation batch failed")
                # fallback: create stub outputs
                for p in batch_prompts:
                    outputs.append({"sample_id": p["sample_id"], "thesis_text": "LLM error - stub", "thesis_structure": {}})

        # persist outputs: write files and DB rows
        for o in outputs:
            sample_id = o["sample_id"]
            thesis_text = o.get("thesis_text", "")
            thesis_structure = o.get("thesis_structure", {})
            thesis_filename = f"thesis_sample_{sample_id}.json"
            thesis_dir = str(Path("distilled_theses") / ticker)
            thesis_path = save_obj_and_record(storage, {"sample_id": sample_id, "thesis": thesis_text, "structure": thesis_structure}, thesis_dir, thesis_filename)
            insert_distilled_thesis(engine, {
                "sample_id": sample_id,
                "thesis_path": thesis_path,
                "thesis_text": thesis_text[:4000],
                "thesis_structure": thesis_structure,
                "source_model": CONFIG["LLM"].get("model")
            })

        self.next(self.export_parquet)

    @step
    def export_parquet(self):
        """
        M4 Enhancement: Export to Parquet with validation and checksums.
        - Stable schema with run_id and as_of_cutoff
        - Partition by ticker/as_of_date
        - Deterministic filenames with run_id
        - Validation: row/column counts, null checks
        - Emit _SUCCESS marker and SHA256 checksum
        """
        ticker = self.ticker
        logger.info(f"[export_parquet] ticker={ticker}")
        engine = get_db_engine()

        # Query assembled samples with ALL fields including run_id and as_of_cutoff
        sql = text("""
        SELECT a.sample_id, a.asset_id, a.as_of_date, a.variation_id, a.run_id, a.as_of_cutoff,
               a.prompt_path, a.prompt_blob, a.prompt_tokens, a.sources_meta,
               l.composite_signal, l.label_class, l.quantile,
               d.thesis_path, d.thesis_text, d.thesis_structure
        FROM charlie.assembled_sample a
        LEFT JOIN charlie.sample_label l ON l.sample_id = a.sample_id
        LEFT JOIN charlie.distilled_thesis d ON d.sample_id = a.sample_id
        WHERE a.asset_id = :aid
        ORDER BY a.as_of_date, a.variation_id
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql, {"aid": self.asset_id}).fetchall()

        if not rows:
            logger.warning(f"No records to export for ticker {ticker}")
            self.export_stats = {"rows": 0, "status": "empty"}
            self.next(self.join_all)
            return

        # Build records with stable schema
        records = []
        for r in rows:
            (sample_id, asset_id, as_of_date, variation_id, run_id, as_of_cutoff,
             prompt_path, prompt_blob, prompt_tokens, sources_meta,
             composite_signal, label_class, quantile,
             thesis_path, thesis_text, thesis_structure) = r

            records.append({
                "run_id": run_id,
                "sample_id": sample_id,
                "asset_id": asset_id,
                "ticker": ticker,
                "as_of_date": str(as_of_date),
                "as_of_cutoff": str(as_of_cutoff) if as_of_cutoff else None,
                "variation_id": variation_id,
                "prompt_tokens": prompt_tokens,
                "sources_meta_json": json.dumps(sources_meta) if sources_meta else None,
                "sources_meta": sources_meta,  # Keep structured for Parquet
                "composite_signal": composite_signal,
                "label_class": label_class,
                "quantile": quantile,
                "has_thesis": thesis_path is not None,
                "thesis_structure": thesis_structure,
                "prompt_path": prompt_path,
                "thesis_path": thesis_path
            })

        df = pd.DataFrame(records)

        # Validation: Check schema and data quality
        required_columns = ["run_id", "sample_id", "ticker", "as_of_date", "variation_id"]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            self.export_stats = {"rows": 0, "status": "schema_error", "error": f"missing: {missing_cols}"}
            self.next(self.join_all)
            return

        # Check for excessive nulls in key fields
        null_counts = df[["sample_id", "as_of_date", "prompt_tokens"]].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null counts in key fields: {null_counts.to_dict()}")

        # Partition by ticker/as_of_date
        out_dir = Path(self.exports_dir) / ticker
        out_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic filename with run_id
        run_id_str = str(self.run_id) if hasattr(self, 'run_id') and self.run_id else "norun"
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        out_filename = f"charlie_{ticker}_run{run_id_str}_{timestamp}.parquet"
        out_path = out_dir / out_filename

        # Write Parquet with compression
        df.to_parquet(str(out_path), index=False, compression='snappy')

        # Compute SHA256 checksum
        checksum = sha256_hash(out_path.read_bytes().hex() if out_path.exists() else "error")
        checksum_file = out_dir / f"{out_filename}.sha256"
        checksum_file.write_text(f"{checksum}  {out_filename}\n")

        # Emit _SUCCESS marker
        success_file = out_dir / "_SUCCESS"
        success_file.write_text(f"Export completed at {timestamp}\nRows: {len(df)}\nColumns: {len(df.columns)}\nFile: {out_filename}\nChecksum: {checksum}\n")

        # Log export stats
        stats = {
            "rows": len(df),
            "columns": len(df.columns),
            "file": str(out_path),
            "checksum": checksum,
            "null_counts": null_counts.to_dict(),
            "status": "success"
        }
        logger.info(f"Exported {len(df)} rows to {out_path} (checksum: {checksum[:16]}...)")
        self.export_stats = stats

        # Update run artifacts
        if hasattr(self, 'run_meta'):
            self.run_meta.setdefault("artifacts", {}).setdefault("parquet_exports", []).append({
                "file": str(out_path),
                "checksum": checksum,
                "rows": len(df)
            })

        self.next(self.join_all)

    @step
    def join_all(self, inputs):
        """
        Join point for parallel foreach tasks; inputs contains references to all parallel branches.
        We'll aggregate artifacts and mark pipeline_run as finished.
        """
        logger.info("Joining all ticker runs")
        
        # In Metaflow, instance variables from 'start' aren't automatically available in 'join'
        # We need to merge data from the inputs
        from metaflow import current
        
        # Try to get run_meta and run_id from one of the inputs
        # In Metaflow foreach/join, parent attributes may not propagate automatically
        run_meta = None
        run_id = None
        
        for inp in inputs:
            if hasattr(inp, 'run_meta') and run_meta is None:
                run_meta = inp.run_meta
            if hasattr(inp, 'run_id') and run_id is None:
                run_id = inp.run_id
            if run_meta is not None and run_id is not None:
                break
        
        if not run_meta:
            # Initialize if not found (shouldn't happen, but be defensive)
            run_meta = {
                "run_name": f"charlie_run_{current.run_id}",
                "started_at": datetime.utcnow(),
                "status": "success",
                "artifacts": {},
                "meta": {}
            }
            logger.warning("run_meta not found in inputs, created new one")
        
        # Aggregate run artifacts from child runs
        for inp in inputs:
            try:
                child_meta = getattr(inp, "run_meta", None)
                if child_meta and isinstance(child_meta, dict):
                    # Merge artifacts
                    for k, v in (child_meta.get("artifacts") or {}).items():
                        run_meta.setdefault("artifacts", {}).setdefault(k, []).extend(v if isinstance(v, list) else [v])
            except Exception:
                logger.exception("Failed to aggregate child run_meta")

        # Finalize pipeline_run entry
        run_meta["finished_at"] = datetime.utcnow()
        run_meta["status"] = "success"
        
        if run_id:
            try:
                db_engine = get_db_engine()
                write_pipeline_run_to_db(db_engine, run_meta)
            except Exception:
                logger.exception("Failed to update pipeline_run DB row")
        
        logger.info("Pipeline finished successfully")
        self.next(self.end)

    @step
    def end(self):
        """
        Final aggregation step. Update pipeline_run status and refresh materialized views.
        """
        logger.info("CharlieTR1Pipeline completed.")

        # Update pipeline_run status to completed
        if hasattr(self, 'run_id') and self.run_id:
            try:
                engine = get_db_engine()
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE charlie.pipeline_run
                        SET status = 'completed',
                            finished_at = :finished_at
                        WHERE run_id = :run_id
                    """), {"finished_at": datetime.utcnow(), "run_id": self.run_id})
                logger.info(f"Updated pipeline_run {self.run_id} to completed")

                # Refresh materialized views (M2: includes label_distribution and data_quality_summary)
                logger.info("Refreshing materialized views...")
                conn = engine.connect()
                conn.execute(text("SELECT charlie.refresh_all_materialized_views()"))
                conn.close()
                logger.info("Materialized views refreshed successfully")
            except Exception as e:
                logger.exception("Failed to update pipeline_run or refresh MVs")

if __name__ == "__main__":
    CharlieTR1Pipeline()
