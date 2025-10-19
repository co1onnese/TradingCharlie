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
    truncate_text_for_budget, compute_technical_indicators, compute_labels_for_asset
)

from charlie_fetchers import (
    fetch_yahoo_ohlcv, fetch_finnhub_news, fetch_fmp_fundamentals,
    fetch_newsapi_alt, fetch_google_news, fetch_eodhd_options,
    run_llm_distillation_batch
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
        Normalize the raw files for this ticker and populate normalized/ tables.
        - dedupe news using dedupe_hash
        - set buckets relative to as_of_date (0-3, 4-10, 11-30)
        - compute tokens_count approximations
        """
        ticker = self.ticker
        logger.info(f"[normalize_dedupe] ticker={ticker}")
        # For simplicity, we load raw_news from DB where asset_id == ticker asset_id and process them.
        # In production, you'd stream large files instead of loading huge tables into memory.
        engine = get_db_engine()
        with engine.connect() as conn:
            # fetch recent raw_news rows for this asset
            rows = conn.execute(text("SELECT news_id, raw_json, published_at, file_path FROM charlie.raw_news WHERE asset_id = :aid"),
                                {"aid": self.asset_id}).fetchall()
            # dedupe set
            seen_hashes = set()
            for r in rows:
                try:
                    raw_json = r[1]
                    dedupe_hash = sha256_hash((raw_json.get("headline","") + raw_json.get("url","") + str(raw_json.get("published_at",""))))
                    if dedupe_hash in seen_hashes:
                        continue
                    seen_hashes.add(dedupe_hash)
                    # normalize published_at to UTC iso
                    # compute tokens_count (approx)
                    text_content = raw_json.get("headline","") + " " + raw_json.get("snippet","")
                    tokens_est = math.ceil(len(text_content) / 4)
                    # Determine bucket: compare published_at to as_of_date(s)
                    # For simplicity, pick latest date in configured date_list
                    # (Better: create normalized rows per date)
                    # Save normalized JSON to normalized/news/<ticker>/<date>/
                    normalized = {
                        "headline": raw_json.get("headline"),
                        "snippet": raw_json.get("snippet"),
                        "url": raw_json.get("url"),
                        "published_at": raw_json.get("published_at"),
                        "language": raw_json.get("language", "en"),
                        "tokens_count": tokens_est
                    }
                    norm_path = save_obj_and_record(storage, normalized, str(Path("normalized") / "news" / ticker), f"news_{r[0]}.json")
                    # In production, update normalized tables and set dedupe flags, 'is_relevant'
                except Exception:
                    logger.exception("Failed normalize news row")
        # done normalizing
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
                    # update DB - use insert_price_window to upsert technicals
                    insert_price_window(get_db_engine(), {
                        "asset_id": self.asset_id,
                        "as_of_date": as_of_date,
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
        For each (ticker, date) create `variation_count` assembled prompts by sampling modalities
        and writing assembled files to local storage and DB.
        """
        ticker = self.ticker
        logger.info(f"[assemble_samples] ticker={ticker}")
        engine = get_db_engine()
        assembled_records = []
        for as_of_date in self.date_list:
            # fetch normalized/available modality items for that date
            # For simplicity in this template, we'll create synthetic prompt content using available price_window + technicals
            # In production: fetch news items (bucketed), alt-news, fundamentals, options, macro, insider, analyst, then sample
            try:
                # gather price window and technicals
                with engine.connect() as conn:
                    res = conn.execute(text("SELECT ohlcv_window, technicals FROM charlie.price_window WHERE asset_id = :aid AND as_of_date = :d"),
                                         {"aid": self.asset_id, "d": as_of_date}).fetchone()
                ohlcv_window = res[0] if res else []
                technicals = res[1] if res else {}
            except Exception:
                logger.exception("Failed to fetch price_window for assemble")
                ohlcv_window = []
                technicals = {}

            for var_id in range(1, int(self.variation_count) + 1):
                # create an assembled prompt text
                prompt_parts = []
                sources_meta = {
                    "news": {"count": 0, "sources": []},
                    "news_alt": {"count": 0, "sources": []},
                    "technicals": {"window_days": len(ohlcv_window)},
                    "fundamentals": {"sources": []},
                    "macro": {"sources": []},
                    "insider": {"transactions": 0},
                    "analyst": {"count": 0},
                    "options": {"included": False, "records":0},
                    "distillation": {"included": False}
                }
                # Add a small textual summary of technicals
                prompt_parts.append(f"Ticker: {ticker}  Date: {as_of_date}")
                prompt_parts.append("Technical summary:")
                prompt_parts.append(json.dumps(technicals.get("latest", {})))
                # optionally add sample fundamentals / news - left as TODO to load real items
                prompt_text = "\n\n".join(prompt_parts)
                truncated_text, token_count = truncate_text_for_budget(prompt_text, token_budget=int(self.token_budget))

                # write prompt to storage
                prompt_dir = str(Path("assembled") / ticker / str(as_of_date))
                filename = f"prompt_var{var_id}.json"
                prompt_payload = {"ticker": ticker, "as_of_date": str(as_of_date), "variation_id": var_id, "prompt": truncated_text, "sources_meta": sources_meta}
                prompt_path = save_obj_and_record(storage, prompt_payload, prompt_dir, filename)

                # Insert assembled_sample row and capture sample_id
                sample_id = insert_assembled_sample(engine, {
                    "asset_id": self.asset_id,
                    "as_of_date": as_of_date,
                    "variation_id": var_id,
                    "prompt_path": prompt_path,
                    "prompt_blob": truncated_text[:4000],  # inline a short preview
                    "prompt_tokens": token_count,
                    "sources_meta": sources_meta
                })
                assembled_records.append(sample_id)
        # store assembled sample ids for downstream
        self.assembled_sample_ids = assembled_records
        self.next(self.generate_labels)

    # @batch(cpu=4, memory=16000)  # Requires S3 datastore - disabled for local mode
    @step
    def generate_labels(self):
        """
        For this asset, compute labels per Algorithm S1 across the available price series,
        then join the labels to assembled_sample rows by date and write sample_label rows.
        """
        ticker = self.ticker
        logger.info(f"[generate_labels] ticker={ticker}")
        engine = get_db_engine()

        # Fetch price history for ticker from price_window (we'll use the latest available date window as representative)
        # In production, you'd compute forward returns per 'as_of_date' exact matching assembly dates
        try:
            # Extract close prices by date using price_window records; this is simplified
            with engine.connect() as conn:
                rows = conn.execute(text("SELECT as_of_date, ohlcv_window FROM charlie.price_window WHERE asset_id = :aid ORDER BY as_of_date ASC"), {"aid": self.asset_id}).fetchall()
            price_series = []
            for r in rows:
                as_of = r[0]
                ohlcv = r[1] or []
                # take last close of the window as representative close for as_of date
                if isinstance(ohlcv, list) and len(ohlcv) > 0:
                    last = ohlcv[-1]
                    price_series.append({"date": as_of, "close": last.get("close")})
            price_df = pd.DataFrame(price_series)
            if price_df.empty:
                logger.warning("No price history available for label generation")
            else:
                price_df['date'] = pd.to_datetime(price_df['date'])
                price_df = price_df.set_index('date')
                labeled = compute_labels_for_asset(price_df)
                # For each assembled sample for this ticker/date, assign label if available
                for sample_id in self.assembled_sample_ids:
                    try:
                        # fetch assembled_sample row
                        with engine.connect() as conn:
                            res = conn.execute(text("SELECT sample_id, as_of_date FROM charlie.assembled_sample WHERE sample_id = :sid"), {"sid": sample_id}).fetchone()
                        if not res:
                            continue
                        sid, as_of_date = res
                        # find label row in labeled df for as_of_date
                        dt = pd.to_datetime(as_of_date)
                        if dt in labeled.index:
                            row = labeled.loc[dt]
                            insert_sample_label(engine, {
                                "sample_id": sid,
                                "composite_signal": float(row['composite_signal']) if not pd.isna(row['composite_signal']) else None,
                                "label_class": int(row['label_class']) if not pd.isna(row['label_class']) and row['label_class'] is not None else None,
                                "quantile": float(row['quantile']) if not pd.isna(row['quantile']) else None,
                                "computed_at": datetime.utcnow()
                            })
                    except Exception:
                        logger.exception("Failed to insert sample_label for sample_id=%s", sample_id)
        except Exception:
            logger.exception("Label generation failed for ticker=%s", ticker)

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
        Gather assembled_sample + sample_label + distilled_thesis for this ticker and write Parquet file(s) partitioned by ticker/date.
        """
        ticker = self.ticker
        logger.info(f"[export_parquet] ticker={ticker}")
        engine = get_db_engine()

        # Query assembled samples joined with labels and thesis where present
        sql = text("""
        SELECT a.sample_id, a.asset_id, a.as_of_date, a.variation_id, a.prompt_path, a.prompt_blob, a.prompt_tokens,
               a.sources_meta, l.composite_signal, l.label_class, l.quantile,
               d.thesis_path, d.thesis_text, d.thesis_structure
        FROM charlie.assembled_sample a
        LEFT JOIN charlie.sample_label l ON l.sample_id = a.sample_id
        LEFT JOIN charlie.distilled_thesis d ON d.sample_id = a.sample_id
        WHERE a.asset_id = :aid
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql, {"aid": self.asset_id}).fetchall()
        records = []
        for r in rows:
            (sample_id, asset_id, as_of_date, variation_id, prompt_path, prompt_blob, prompt_tokens, sources_meta,
             composite_signal, label_class, quantile, thesis_path, thesis_text, thesis_structure) = r
            records.append({
                "sample_id": sample_id,
                "asset_id": asset_id,
                "as_of_date": str(as_of_date),
                "variation_id": variation_id,
                "prompt_path": prompt_path,
                "prompt_blob": prompt_blob,
                "prompt_tokens": prompt_tokens,
                "sources_meta": sources_meta,
                "composite_signal": composite_signal,
                "label_class": label_class,
                "quantile": quantile,
                "thesis_path": thesis_path,
                "thesis_text": thesis_text,
                "thesis_structure": thesis_structure
            })
        if records:
            df = pd.DataFrame(records)
            # Partition by ticker for easy consumption
            out_dir = Path(self.exports_dir) / ticker
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"charlie_export_{ticker}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.parquet"
            df.to_parquet(str(out_path), index=False)
            logger.info(f"Wrote export parquet for {ticker} to {out_path}")
            # update run artifacts map
            self.run_meta["artifacts"].setdefault("parquet_exports", []).append(str(out_path))
        else:
            logger.warning("No records to export for ticker %s", ticker)

        # done for this ticker
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
        logger.info("CharlieTR1Pipeline completed.")

if __name__ == "__main__":
    CharlieTR1Pipeline()
