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

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None

logger = logging.getLogger("charlie")

# -------------------------
# Helper functions
# -------------------------

def serialize_to_json_safe(obj: Any) -> Any:
    """
    Recursively convert an object to a JSON-serializable format.
    Handles datetime objects, date objects, and other non-serializable types.
    """
    import json

    def convert(item):
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        elif isinstance(item, dict):
            return {k: convert(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [convert(i) for i in item]
        elif isinstance(item, tuple):
            return tuple(convert(i) for i in item)
        elif hasattr(item, '__dict__'):
            # Handle objects with __dict__ by converting to dict
            return convert(vars(item))
        else:
            # For other types, try to keep them as-is
            # If they're not serializable, json.dumps will handle it later
            return item

    return convert(obj)

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
                "raw_json": serialize_to_json_safe(article)
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

    NOTE: Updated to use /stable/ API endpoints (legacy v3/v4 deprecated August 31, 2025)
    Free tier limited to US stocks. International stocks require premium subscription.
    """
    if not api_key:
        logger.debug(f"FMP API key not configured, skipping fundamentals for {ticker}")
        return []

    try:
        logger.debug(f"Fetching FMP fundamentals for {ticker}")

        # Use new stable API endpoint (symbol as query parameter)
        url = "https://financialmodelingprep.com/stable/income-statement"
        params = {"symbol": ticker, "apikey": api_key, "limit": 8}  # Get last 2 years (quarterly)

        response = requests.get(url, params=params, timeout=30)

        # Handle premium/subscription errors (402)
        if response.status_code == 402:
            logger.warning(f"FMP fundamentals for {ticker} requires premium subscription (402)")
            return []

        response.raise_for_status()
        data = response.json()

        # Check if response is an error message (FMP returns JSON even for errors)
        if isinstance(data, dict) and 'Error Message' in data:
            logger.warning(f"FMP API returned error for {ticker}: {data['Error Message']}")
            return []

        # Handle empty response
        if not data or (isinstance(data, list) and len(data) == 0):
            logger.info(f"No financial data available for {ticker}")
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

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 402:
            logger.warning(f"FMP fundamentals for {ticker} requires premium subscription")
        else:
            logger.error(f"FMP fundamentals HTTP error for {ticker}: {e}")
        return []
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
                "raw_json": serialize_to_json_safe(article)
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
            # Note: SerpAPI nests the date inside the article structure
            pub_date_str = article.get('date', '')
            if not pub_date_str:
                # Try getting date from the nested structure if not at top level
                pub_date_str = article.get('raw_json', {}).get('date', '')

            pub_date = None
            if pub_date_str:
                try:
                    # Try parsing common formats
                    from dateutil import parser
                    pub_date = parser.parse(pub_date_str)
                except:
                    # Try parsing SerpAPI specific format: "10/19/2025, 10:46 AM, +0000 UTC"
                    try:
                        from datetime import datetime
                        # Remove the timezone part and parse as naive datetime
                        if ', +0000 UTC' in pub_date_str:
                            # Format: "MM/DD/YYYY, HH:MM AM/PM, +0000 UTC"
                            date_part = pub_date_str.replace(', +0000 UTC', '')
                            pub_date = datetime.strptime(date_part, '%m/%d/%Y, %I:%M %p')
                        else:
                            logger.debug(f"Could not parse date: {pub_date_str}")
                    except:
                        logger.debug(f"Could not parse date: {pub_date_str}")
                        pub_date = None

            results.append({
                "headline": article.get('title', ''),
                "snippet": article.get('snippet', ''),
                "url": article.get('link', ''),
                "published_at": pub_date.isoformat() if pub_date else None,
                "source": article.get('source', {}).get('name', 'google_news') if isinstance(article.get('source'), dict) else 'google_news',
                "raw_json": serialize_to_json_safe(article)
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

# -------------------------
# M3: Additional modality fetchers (Insider, Analyst, SEC)
# -------------------------

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_insider_transactions(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch insider trading transactions from FMP API.
    Returns list of insider transactions up to as_of_date.

    NOTE: Updated to use /stable/ API endpoints (legacy v3/v4 deprecated August 31, 2025)
    Insider trading data may require premium subscription or may not be available on free tier.
    """
    if not api_key:
        logger.debug(f"FMP API key not configured, skipping insider transactions for {ticker}")
        return []

    try:
        logger.debug(f"Fetching insider transactions for {ticker} up to {as_of_date}")

        # Use new stable API endpoint (may not support symbol filtering on free tier)
        url = "https://financialmodelingprep.com/stable/insider-trading"
        params = {"apikey": api_key, "limit": 100}  # Get more transactions, filter client-side

        response = requests.get(url, params=params, timeout=30)

        # Handle premium/subscription errors (402)
        if response.status_code == 402:
            logger.warning(f"FMP insider transactions for {ticker} requires premium subscription (402)")
            return []

        response.raise_for_status()
        data = response.json()

        # Check if response is an error message (FMP returns JSON even for errors)
        if isinstance(data, dict) and 'Error Message' in data:
            logger.warning(f"FMP API returned error for insider transactions: {data['Error Message']}")
            return []

        # Handle empty response
        if not data or (isinstance(data, list) and len(data) == 0):
            logger.info(f"No insider transaction data available (may require premium subscription)")
            return []

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for txn in data:
            filing_date_str = txn.get('filingDate')
            if not filing_date_str:
                continue

            # Parse filing date and filter by as_of_date
            try:
                filing_date = datetime.strptime(filing_date_str.split('T')[0], '%Y-%m-%d').date()
                if filing_date > as_of_date:
                    continue  # Skip future transactions
            except:
                continue

            # Filter by ticker if available in response (may not be present in stable API)
            txn_symbol = txn.get('symbol', '').upper()
            if txn_symbol and txn_symbol != ticker.upper():
                continue  # Skip transactions for other tickers

            results.append({
                "filing_date": filing_date_str.split('T')[0],
                "transaction_type": txn.get('transactionType', ''),
                "shares": txn.get('securitiesTransacted', 0),
                "amount": txn.get('securitiesOwned', 0),  # Post-transaction ownership
                "mspr": txn.get('pricePerShare', 0),  # Price per share
                "owner": txn.get('reportingName', ''),
                "raw_json": txn
            })

        logger.info(f"Fetched {len(results)} insider transactions from FMP for {ticker}")
        return results

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 402:
            logger.warning(f"FMP insider transactions for {ticker} requires premium subscription")
        else:
            logger.error(f"FMP insider transactions HTTP error for {ticker}: {e}")
        return []
    except Exception as e:
        logger.error(f"FMP insider transactions fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_analyst_recommendations(ticker: str, as_of_date: date, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch analyst recommendations from FMP API.
    Returns list of analyst ratings up to as_of_date.

    NOTE: Updated to use /stable/ API endpoints (legacy v3/v4 deprecated August 31, 2025)
    Free tier limited to US stocks. International stocks require premium subscription.
    """
    if not api_key:
        logger.debug(f"FMP API key not configured, skipping analyst recommendations for {ticker}")
        return []

    try:
        logger.debug(f"Fetching analyst recommendations for {ticker} up to {as_of_date}")

        # Use new stable API endpoint (symbol as query parameter)
        url = "https://financialmodelingprep.com/stable/grades"
        params = {"symbol": ticker, "apikey": api_key, "limit": 20}

        response = requests.get(url, params=params, timeout=30)

        # Handle premium/subscription errors (402)
        if response.status_code == 402:
            logger.warning(f"FMP analyst recommendations for {ticker} requires premium subscription (402)")
            return []

        response.raise_for_status()
        data = response.json()

        # Check if response is an error message (FMP returns JSON even for errors)
        if isinstance(data, dict) and 'Error Message' in data:
            logger.warning(f"FMP API returned error for analyst recommendations: {data['Error Message']}")
            return []

        # Handle empty response
        if not data or (isinstance(data, list) and len(data) == 0):
            logger.info(f"No analyst recommendation data available for {ticker}")
            return []

        time.sleep(CONFIG.get('RATE_LIMIT_DELAY', 1.0))

        results = []
        for reco in data:
            reco_date_str = reco.get('date')
            if not reco_date_str:
                continue

            # Parse date and filter by as_of_date
            try:
                reco_date = datetime.strptime(reco_date_str.split('T')[0], '%Y-%m-%d').date()
                if reco_date > as_of_date:
                    continue  # Skip future recommendations
            except:
                continue

            results.append({
                "reco_date": reco_date_str.split('T')[0],
                "consensus_rating": reco.get('newGrade', ''),
                "previous_rating": reco.get('previousGrade', ''),
                "firm": reco.get('gradingCompany', ''),
                "action": reco.get('action', ''),  # upgrade, downgrade, init, reiterate
                "raw_json": reco
            })

        logger.info(f"Fetched {len(results)} analyst recommendations from FMP for {ticker}")
        return results

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 402:
            logger.warning(f"FMP analyst recommendations for {ticker} requires premium subscription")
        else:
            logger.error(f"FMP analyst recommendations HTTP error for {ticker}: {e}")
        return []
    except Exception as e:
        logger.error(f"FMP analyst recommendations fetch failed for {ticker}: {e}")
        return []

@tenacity_retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_edgar_filings(ticker: str, as_of_date: date) -> List[Dict[str, Any]]:
    """
    Fetch SEC EDGAR filings (10-Q, 10-K) for the ticker up to as_of_date.
    This is a STUB implementation using SEC EDGAR RSS/API.

    NOTE: Full implementation would require parsing CIK, querying SEC EDGAR API,
    and extracting filing metadata. This stub returns an empty list with logging.
    """
    try:
        logger.info(f"SEC EDGAR filings fetch is STUBBED for {ticker} (as_of_date={as_of_date})")
        logger.info("To implement: Query https://www.sec.gov/cgi-bin/browse-edgar with CIK and parse filings")

        # TODO: Implement full SEC EDGAR integration
        # 1. Map ticker to CIK (Central Index Key)
        # 2. Query SEC EDGAR API: https://data.sec.gov/submissions/CIK{cik}.json
        # 3. Parse recent filings (10-Q, 10-K, 8-K)
        # 4. Extract filing dates, URLs, and metadata
        # 5. Filter by as_of_date

        return []

    except Exception as e:
        logger.error(f"SEC EDGAR filings fetch failed for {ticker}: {e}")
        return []

# -------------------------
# M4: LLM distillation with OpenAI and Claude fallback
# -------------------------
def run_llm_distillation_batch(prompts: List[Dict[str, Any]], llm_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate investment theses using LLM APIs with fallback support.
    Tries OpenAI first, falls back to Claude if OpenAI fails.

    prompts: list of {"sample_id": int, "prompt_text": str}
    llm_config: {
        "api_key": str (OpenAI key),
        "anthropic_api_key": str (Claude key),
        "model": str,
        "fallback_to_claude": bool
    }
    returns: list of {"sample_id": int, "thesis_text": str, "thesis_structure": dict}
    """
    # Check availability
    openai_available = OpenAI is not None and llm_config.get("api_key")
    claude_available = Anthropic is not None and llm_config.get("anthropic_api_key")

    if not openai_available and not claude_available:
        logger.warning("No LLM providers available - returning stub theses")
        return _generate_stub_theses(prompts)

    # Try OpenAI first
    if openai_available:
        try:
            return _distill_with_openai(prompts, llm_config)
        except Exception as e:
            logger.error(f"OpenAI distillation failed: {e}")
            if claude_available and llm_config.get("fallback_to_claude", True):
                logger.info("Falling back to Claude for distillation")
                try:
                    return _distill_with_claude(prompts, llm_config)
                except Exception as e2:
                    logger.error(f"Claude fallback also failed: {e2}")
                    return _generate_stub_theses(prompts, error=f"Both providers failed: OpenAI={e}, Claude={e2}")
            else:
                return _generate_stub_theses(prompts, error=str(e))

    # If OpenAI not available but Claude is, use Claude directly
    if claude_available:
        try:
            return _distill_with_claude(prompts, llm_config)
        except Exception as e:
            logger.error(f"Claude distillation failed: {e}")
            return _generate_stub_theses(prompts, error=str(e))

    return _generate_stub_theses(prompts)

def _generate_stub_theses(prompts: List[Dict[str, Any]], error: str = "LLM not configured") -> List[Dict[str, Any]]:
    """Generate stub thesis outputs when LLM is unavailable"""
    return [{
        "sample_id": p["sample_id"],
        "thesis_text": f"LLM distillation skipped: {error}",
        "thesis_structure": {"error": error, "summary": "stub"},
    } for p in prompts]

def _distill_with_openai(prompts: List[Dict[str, Any]], llm_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Distill theses using OpenAI API"""
    client = OpenAI(api_key=llm_config["api_key"])
    model = llm_config.get("model", "gpt-4o-mini")

    logger.info(f"Running OpenAI distillation for {len(prompts)} samples using {model}")

    outputs = []
    for idx, p in enumerate(prompts):
        try:
            sample_id = p["sample_id"]
            prompt_text = p["prompt_text"]

            system_prompt = """You are a senior financial analyst generating comprehensive investment theses.

CRITICAL: You have access to extensive market data including price action, news articles, financial statements, options data, macroeconomic events, insider transactions, and analyst recommendations. You MUST analyze ALL provided data comprehensively.

Generate a structured investment thesis that demonstrates deep understanding of the data:

1. EXECUTIVE SUMMARY (3-4 sentences synthesizing all data sources)

2. PRICE & TECHNICAL ANALYSIS (analyze OHLCV data, indicators, trends)

3. FUNDAMENTAL ANALYSIS (evaluate financial health using statements, ratios)

4. SENTIMENT ANALYSIS (news articles, insider activity, analyst ratings, options positioning)

5. MACROECONOMIC CONTEXT (relevant economic events and their impact)

6. KEY INVESTMENT CLAIMS (4-6 bullet points supported by specific data)

7. SUPPORTING EVIDENCE (cite concrete examples from each data modality)

8. RISK FACTORS (3-4 specific risks identified from the data)

9. INVESTMENT RECOMMENDATION (choose ONE: "strong_buy", "buy", "neutral", "sell", "strong_sell" with detailed justification)

REQUIREMENT: Reference specific data points, dates, numbers, and sources throughout your analysis. Do not ignore any data modality. Show how multiple data sources corroborate or contradict each other."""

            user_prompt = f"""Based on the following financial data, generate an investment thesis:

{prompt_text}

Provide a structured analysis following the format specified. For the recommendation, choose exactly ONE from: "strong_buy", "buy", "neutral", "sell", "strong_sell"."""

            logger.debug(f"Generating thesis {idx+1}/{len(prompts)} for sample {sample_id}")

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
            tokens_used = response.usage.total_tokens if hasattr(response, 'usage') else 0

            thesis_structure = _parse_thesis_structure(thesis_text, model, tokens_used)

            outputs.append({
                "sample_id": sample_id,
                "thesis_text": thesis_text,
                "thesis_structure": thesis_structure
            })

            logger.info(f"Generated thesis for sample {sample_id} ({len(thesis_text)} chars, {tokens_used} tokens)")
            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"OpenAI call failed for sample {p.get('sample_id')}: {e}")
            outputs.append({
                "sample_id": p.get("sample_id"),
                "thesis_text": f"Error: {str(e)}",
                "thesis_structure": {"error": str(e)}
            })

    return outputs

def _distill_with_claude(prompts: List[Dict[str, Any]], llm_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Distill theses using Anthropic Claude API"""
    client = Anthropic(api_key=llm_config["anthropic_api_key"])
    model = llm_config.get("claude_model", "claude-3-5-sonnet-20241022")

    logger.info(f"Running Claude distillation for {len(prompts)} samples using {model}")

    outputs = []
    for idx, p in enumerate(prompts):
        try:
            sample_id = p["sample_id"]
            prompt_text = p["prompt_text"]

            system_prompt = """You are a senior financial analyst generating comprehensive investment theses.

CRITICAL: You have access to extensive market data including price action, news articles, financial statements, options data, macroeconomic events, insider transactions, and analyst recommendations. You MUST analyze ALL provided data comprehensively.

Generate a structured investment thesis that demonstrates deep understanding of the data:

1. EXECUTIVE SUMMARY (3-4 sentences synthesizing all data sources)

2. PRICE & TECHNICAL ANALYSIS (analyze OHLCV data, indicators, trends)

3. FUNDAMENTAL ANALYSIS (evaluate financial health using statements, ratios)

4. SENTIMENT ANALYSIS (news articles, insider activity, analyst ratings, options positioning)

5. MACROECONOMIC CONTEXT (relevant economic events and their impact)

6. KEY INVESTMENT CLAIMS (4-6 bullet points supported by specific data)

7. SUPPORTING EVIDENCE (cite concrete examples from each data modality)

8. RISK FACTORS (3-4 specific risks identified from the data)

9. INVESTMENT RECOMMENDATION (choose ONE: "strong_buy", "buy", "neutral", "sell", "strong_sell" with detailed justification)

REQUIREMENT: Reference specific data points, dates, numbers, and sources throughout your analysis. Do not ignore any data modality. Show how multiple data sources corroborate or contradict each other."""

            user_prompt = f"""Based on the following financial data, generate an investment thesis:

{prompt_text}

Provide a structured analysis following the format specified. For the recommendation, choose exactly ONE from: "strong_buy", "buy", "neutral", "sell", "strong_sell"."""

            logger.debug(f"Generating thesis {idx+1}/{len(prompts)} for sample {sample_id}")

            response = client.messages.create(
                model=model,
                max_tokens=1000,
                temperature=0.7,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )

            thesis_text = response.content[0].text
            tokens_used = response.usage.input_tokens + response.usage.output_tokens if hasattr(response, 'usage') else 0

            thesis_structure = _parse_thesis_structure(thesis_text, model, tokens_used)

            outputs.append({
                "sample_id": sample_id,
                "thesis_text": thesis_text,
                "thesis_structure": thesis_structure
            })

            logger.info(f"Generated thesis for sample {sample_id} ({len(thesis_text)} chars, {tokens_used} tokens)")
            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"Claude call failed for sample {p.get('sample_id')}: {e}")
            outputs.append({
                "sample_id": p.get("sample_id"),
                "thesis_text": f"Error: {str(e)}",
                "thesis_structure": {"error": str(e)}
            })

    return outputs

def _parse_thesis_structure(thesis_text: str, model: str, tokens_used: int) -> Dict[str, Any]:
    """Parse structured information from thesis text"""
    lines = thesis_text.split('\n')
    claims = [line.strip('- ') for line in lines if line.strip().startswith('-')]

    # Extract sections (simple heuristic)
    summary = ""
    evidence = []
    risks = []
    recommendation = "neutral"  # default

    # Valid recommendations
    valid_recommendations = ["strong_buy", "buy", "neutral", "sell", "strong_sell"]

    for i, line in enumerate(lines):
        if 'summary' in line.lower() and i + 1 < len(lines):
            summary = lines[i + 1].strip()
        elif 'evidence' in line.lower() or 'supporting' in line.lower():
            # Collect next few lines
            evidence = [l.strip('- ') for l in lines[i+1:i+4] if l.strip().startswith('-')]
        elif 'risk' in line.lower():
            risks = [l.strip('- ') for l in lines[i+1:i+3] if l.strip().startswith('-')]
        elif 'recommendation' in line.lower() or 'outlook' in line.lower():
            # Look for the recommendation in the next few lines
            for j in range(i+1, min(i+5, len(lines))):
                line_lower = lines[j].lower().strip()
                # Check if line contains any valid recommendation
                for rec in valid_recommendations:
                    if rec in line_lower or rec.replace('_', ' ') in line_lower:
                        recommendation = rec
                        break
                if recommendation != "neutral":
                    break

    return {
        "summary": summary or (lines[0] if lines else "No summary"),
        "claims": claims[:5] if claims else [],
        "evidence": evidence[:3] if evidence else [],
        "risks": risks[:3] if risks else [],
        "recommendation": recommendation,
        "model": model,
        "tokens_used": tokens_used,
        "version": "v3_structured_5point"
    }
