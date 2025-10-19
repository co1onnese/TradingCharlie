# charlie_fetchers.py
#
# Data fetcher functions for Charlie-TR1 pipeline
# Contains: API fetchers for Yahoo, Finnhub, FRED, FMP, NewsAPI, Google News, EODHD, SimFin, and LLM distillation

import logging
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Any
import pandas as pd
import requests
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import from charlie_utils
from charlie_utils import CONFIG

# API-specific imports
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

logger = logging.getLogger("charlie")

# -------------------------
# Provider fetcher implementations with retry logic
# -------------------------

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type((requests.exceptions.RequestException, TimeoutError)))
def fetch_yahoo_ohlcv(ticker: str, as_of_date: date) -> Dict[str, Any]:
    """
    Fetch 15 trading days of OHLCV data ending at as_of_date using yfinance.
    Returns dict with 'data' (list of OHLCV records) or empty dict on failure.
    """
    if yf is None:
        logger.warning("yfinance not installed, skipping Yahoo OHLCV fetch")
        return {}

    try:
        # Calculate start date (go back ~21 calendar days to get 15 trading days)
        start_date = as_of_date - timedelta(days=21)

        logger.debug(f"Fetching Yahoo OHLCV for {ticker} from {start_date} to {as_of_date}")

        # Download data
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=as_of_date + timedelta(days=1))

        if df.empty:
            logger.warning(f"No Yahoo OHLCV data returned for {ticker}")
            return {}

        # Convert to list of dicts, take last 15 trading days
        df = df.tail(15).reset_index()
        records = []
        for _, row in df.iterrows():
            records.append({
                "date": row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date']),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": int(row['Volume'])
            })

        logger.info(f"Successfully fetched {len(records)} trading days from Yahoo for {ticker}")
        return {"data": records}

    except Exception as e:
        logger.error(f"Yahoo OHLCV fetch failed for {ticker}: {e}")
        return {}

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_finnhub_news(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch company news from Finnhub for the past 30 days from as_of_date.
    Returns list of news articles.
    """
    if not api_key or finnhub is None:
        logger.debug(f"Finnhub not available, skipping news fetch for {ticker}")
        return []

    try:
        # Finnhub date range: past 30 days
        start_date = as_of_date - timedelta(days=30)

        logger.debug(f"Fetching Finnhub news for {ticker} from {start_date} to {as_of_date}")

        client = finnhub.Client(api_key=api_key)
        news = client.company_news(ticker, _from=start_date.strftime('%Y-%m-%d'), to=as_of_date.strftime('%Y-%m-%d'))

        # Rate limiting
        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for article in news:
            results.append({
                "headline": article.get('headline', ''),
                "snippet": article.get('summary', ''),
                "url": article.get('url', ''),
                "published_at": datetime.fromtimestamp(article.get('datetime', 0)).isoformat() if article.get('datetime') else None,
                "raw_json": article
            })

        logger.info(f"Fetched {len(results)} news articles from Finnhub for {ticker}")
        return results

    except Exception as e:
        logger.error(f"Finnhub news fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_fred_series(series_code: str, api_key: str) -> Dict[str, Any]:
    """
    Fetch economic series data from FRED.
    Returns dict with series data or empty dict on failure.
    """
    if not api_key or Fred is None:
        logger.debug(f"FRED not available, skipping fetch for {series_code}")
        return {}

    try:
        logger.debug(f"Fetching FRED series: {series_code}")

        fred = Fred(api_key=api_key)
        series = fred.get_series(series_code)

        # Convert to list of dicts
        records = []
        for date_val, value in series.items():
            if not pd.isna(value):
                records.append({
                    "date": date_val.strftime('%Y-%m-%d'),
                    "value": float(value)
                })

        logger.info(f"Fetched {len(records)} data points from FRED for {series_code}")
        return {"series_code": series_code, "data": records}

    except Exception as e:
        logger.error(f"FRED fetch failed for {series_code}: {e}")
        return {}

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_fmp_fundamentals(ticker: str, start_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch financial statements from Financial Modeling Prep.
    Returns list of financial reports.
    
    NOTE: FMP v3 endpoints may require paid subscription. Free tier has limitations.
    """
    if not api_key:
        logger.debug(f"FMP API key not configured, skipping fundamentals for {ticker}")
        return []

    try:
        logger.debug(f"Fetching FMP fundamentals for {ticker}")

        # Try v4 endpoint first (more likely to work with free tier)
        url = f"https://financialmodelingprep.com/api/v4/income-statement/{ticker}"
        params = {"apikey": api_key, "limit": 8}  # Get last 2 years (quarterly)

        response = requests.get(url, params=params, timeout=30)
        
        # If v4 fails, try v3 as fallback
        if response.status_code == 403 or response.status_code == 404:
            logger.debug(f"FMP v4 failed, trying v3 for {ticker}")
            url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
            response = requests.get(url, params=params, timeout=30)
        
        response.raise_for_status()
        data = response.json()
        
        # Check if response is an error message (FMP returns JSON even for errors)
        if isinstance(data, dict) and 'Error Message' in data:
            logger.warning(f"FMP API returned error for {ticker}: {data['Error Message']}")
            return []

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for report in data:
            results.append({
                "report_date": report.get('date'),
                "period_type": report.get('period', 'Q'),
                "currency": report.get('reportedCurrency', 'USD'),
                "raw_json": report,
                "normalized": {
                    "revenue": report.get('revenue'),
                    "net_income": report.get('netIncome'),
                    "ebitda": report.get('ebitda'),
                    "eps": report.get('eps')
                },
                "source_url": url
            })

        logger.info(f"Fetched {len(results)} financial reports from FMP for {ticker}")
        return results

    except Exception as e:
        logger.error(f"FMP fundamentals fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_newsapi_alt(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch news from NewsAPI.org for alternative news coverage.
    Returns list of news articles.
    
    NOTE: NewsAPI.org FREE tier only allows queries for the last 30 days.
    Historical data requires paid subscription.
    """
    if not api_key:
        logger.debug(f"NewsAPI key not configured, skipping alt news for {ticker}")
        return []

    try:
        # NewsAPI FREE tier limitation: only last 30 days from today
        today = datetime.now().date()
        days_ago = (today - as_of_date).days
        
        if days_ago > 28:  # Use 28 to be safe (under 30 day limit)
            logger.info(f"Skipping NewsAPI for {ticker}: date {as_of_date} is {days_ago} days old (free tier limit: 30 days)")
            return []
        
        # NewsAPI date range: past 7 days from as_of_date
        from_date = as_of_date - timedelta(days=7)

        logger.debug(f"Fetching NewsAPI articles for {ticker} from {from_date} to {as_of_date}")

        url = "https://newsapi.org/v2/everything"
        params = {
            "q": ticker,
            "from": from_date.strftime('%Y-%m-%d'),
            "to": as_of_date.strftime('%Y-%m-%d'),
            "language": "en",
            "sortBy": "relevancy",
            "apiKey": api_key,
            "pageSize": 20
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for article in data.get('articles', []):
            results.append({
                "headline": article.get('title', ''),
                "snippet": article.get('description', ''),
                "url": article.get('url', ''),
                "published_at": article.get('publishedAt'),
                "source": article.get('source', {}).get('name', 'newsapi'),
                "language": "en",
                "raw_json": article
            })

        logger.info(f"Fetched {len(results)} articles from NewsAPI for {ticker}")
        return results

    except Exception as e:
        logger.error(f"NewsAPI fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_google_news(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch Google News via SerpAPI.
    Returns list of news articles.
    """
    if not api_key:
        logger.debug(f"SerpAPI key not configured, skipping Google News for {ticker}")
        return []

    try:
        logger.debug(f"Fetching Google News for {ticker} via SerpAPI")

        url = "https://serpapi.com/search"
        params = {
            "engine": "google_news",
            "q": ticker,
            "api_key": api_key
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for article in data.get('news_results', []):
            # Parse date if available - Google News returns various formats
            pub_date_str = article.get('date', '')
            pub_date = None
            if pub_date_str:
                try:
                    # Try parsing common formats
                    from dateutil import parser
                    pub_date = parser.parse(pub_date_str)
                except:
                    # If parsing fails, leave as None
                    logger.debug(f"Could not parse date: {pub_date_str}")
                    pub_date = None
            
            results.append({
                "headline": article.get('title', ''),
                "snippet": article.get('snippet', ''),
                "url": article.get('link', ''),
                "published_at": pub_date,
                "source": article.get('source', {}).get('name', 'google_news') if isinstance(article.get('source'), dict) else 'google_news',
                "raw_json": article
            })

        logger.info(f"Fetched {len(results)} articles from Google News for {ticker}")
        return results

    except Exception as e:
        logger.error(f"Google News fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_eodhd_options(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch options chain data from EODHD.
    Returns list of options contracts.
    
    NOTE: Historical options data (date parameter) requires PREMIUM subscription.
    Free tier only provides current/live options data.
    """
    if not api_key:
        logger.debug(f"EODHD API key not configured, skipping options for {ticker}")
        return []

    try:
        # Check if we're requesting historical data
        today = datetime.now().date()
        is_historical = as_of_date < today
        
        if is_historical:
            logger.info(f"Skipping EODHD options for {ticker}: historical options data (date={as_of_date}) requires premium subscription")
            return []
        
        logger.debug(f"Fetching EODHD current options for {ticker}")

        url = f"https://eodhd.com/api/options/{ticker}.US"
        # Only include date parameter if it's today (current data)
        params = {"api_token": api_key}
        if not is_historical:
            params["date"] = as_of_date.strftime('%Y-%m-%d')

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for option in data.get('data', []):
            results.append({
                "expiration": option.get('expirationDate'),
                "option_type": option.get('type', '').lower(),  # 'call' or 'put'
                "strike": float(option.get('strike', 0)),
                "open_interest": int(option.get('openInterest', 0)),
                "implied_vol": float(option.get('impliedVolatility', 0)),
                "underlying_price": float(option.get('underlyingPrice', 0)),
                "raw_json": option
            })

        logger.info(f"Fetched {len(results)} options contracts from EODHD for {ticker}")
        return results

    except Exception as e:
        logger.error(f"EODHD options fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_eodhd_econ_events(start_date: date, end_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch economic events calendar from EODHD.
    Returns list of economic events.
    """
    if not api_key:
        logger.debug("EODHD API key not configured, skipping economic events")
        return []

    try:
        logger.debug(f"Fetching EODHD economic events from {start_date} to {end_date}")

        url = "https://eodhd.com/api/economic-events"
        params = {
            "api_token": api_key,
            "from": start_date.strftime('%Y-%m-%d'),
            "to": end_date.strftime('%Y-%m-%d')
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for event in data:
            results.append({
                "date": event.get('date'),
                "event_name": event.get('event'),
                "country": event.get('country'),
                "actual": event.get('actual'),
                "forecast": event.get('forecast'),
                "previous": event.get('previous'),
                "raw_json": event
            })

        logger.info(f"Fetched {len(results)} economic events from EODHD")
        return results

    except Exception as e:
        logger.error(f"EODHD economic events fetch failed: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_simfin_fundamentals(ticker: str, start_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch fundamentals from SimFin API.
    Returns list of financial statements.
    """
    if not api_key:
        logger.debug(f"SimFin API key not configured, skipping fundamentals for {ticker}")
        return []

    try:
        logger.debug(f"Fetching SimFin fundamentals for {ticker}")

        # SimFin income statement endpoint
        url = f"https://simfin.com/api/v2/companies/statements"
        headers = {"Authorization": f"api-key {api_key}"}
        params = {
            "ticker": ticker,
            "statement": "pl",  # profit & loss / income statement
            "period": "q"  # quarterly
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for report in data:
            results.append({
                "report_date": report.get('Fiscal Year') + '-' + report.get('Fiscal Period', 'Q1'),
                "period_type": "Q",
                "currency": report.get('Currency', 'USD'),
                "raw_json": report,
                "normalized": {
                    "revenue": report.get('Revenue'),
                    "net_income": report.get('Net Income'),
                    "gross_profit": report.get('Gross Profit')
                }
            })

        logger.info(f"Fetched {len(results)} financial statements from SimFin for {ticker}")
        return results

    except Exception as e:
        logger.error(f"SimFin fundamentals fetch failed for {ticker}: {e}")
        return []

# LLM distillation with OpenAI
def run_llm_distillation_batch(prompts: List[Dict[str, Any]], llm_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate investment theses using OpenAI API.
    prompts: list of {"sample_id": int, "prompt_text": str}
    returns: list of {"sample_id": int, "thesis_text": str, "thesis_structure": dict}
    """
    if OpenAI is None or not llm_config.get("api_key"):
        logger.warning("OpenAI not available or API key not configured - returning stub theses")
        outputs = []
        for p in prompts:
            outputs.append({
                "sample_id": p["sample_id"],
                "thesis_text": f"LLM distillation skipped (OpenAI not configured)",
                "thesis_structure": {"claims": [], "evidence": [], "summary": "stub"},
            })
        return outputs

    try:
        client = OpenAI(api_key=llm_config["api_key"])
        model = llm_config.get("model", "gpt-4o-mini")

        logger.info(f"Running LLM distillation for {len(prompts)} samples using {model}")

        outputs = []
        for idx, p in enumerate(prompts):
            try:
                sample_id = p["sample_id"]
                prompt_text = p["prompt_text"]

                # Create structured prompt for investment thesis generation
                system_prompt = """You are a financial analyst generating concise investment theses.
Analyze the provided data and generate a structured investment thesis with:
1. Executive Summary (2-3 sentences)
2. Key Claims (3-5 bullet points)
3. Supporting Evidence (cite specific data points)
4. Risk Factors (2-3 key risks)
5. Outlook (bullish/bearish/neutral with brief justification)

Keep the response focused and data-driven."""

                user_prompt = f"""Based on the following financial data, generate an investment thesis:

{prompt_text}

Provide a structured analysis following the format specified."""

                logger.debug(f"Generating thesis for sample {sample_id} ({idx+1}/{len(prompts)})")

                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.7,
                    max_tokens=1000
                )

                thesis_text = response.choices[0].message.content

                # Parse structure from the response (simple extraction)
                lines = thesis_text.split('\n')
                claims = [line.strip('- ') for line in lines if line.strip().startswith('-')]

                thesis_structure = {
                    "claims": claims[:5] if claims else [],
                    "evidence": [],  # Could enhance with regex extraction
                    "summary": thesis_text.split('\n')[0] if thesis_text else "No summary",
                    "model": model,
                    "tokens_used": response.usage.total_tokens if hasattr(response, 'usage') else 0
                }

                outputs.append({
                    "sample_id": sample_id,
                    "thesis_text": thesis_text,
                    "thesis_structure": thesis_structure
                })

                logger.info(f"Generated thesis for sample {sample_id} ({len(thesis_text)} chars)")

                # Rate limiting
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Failed to generate thesis for sample {p.get('sample_id')}: {e}")
                outputs.append({
                    "sample_id": p.get("sample_id"),
                    "thesis_text": f"Error generating thesis: {str(e)}",
                    "thesis_structure": {"error": str(e)}
                })

        logger.info(f"Completed LLM distillation batch: {len(outputs)} theses generated")
        return outputs

    except Exception as e:
        logger.error(f"LLM distillation batch failed: {e}")
        # Return stub outputs on complete failure
        return [{
            "sample_id": p["sample_id"],
            "thesis_text": f"LLM error: {str(e)}",
            "thesis_structure": {"error": str(e)}
        } for p in prompts]
