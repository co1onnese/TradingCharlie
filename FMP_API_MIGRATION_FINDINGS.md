# FMP API Migration Findings

**Date:** 2025-10-20
**Issue:** FMP API calls failing with 403 Forbidden errors

## Root Cause

Financial Modeling Prep (FMP) **deprecated legacy API endpoints** (v3 and v4) on **August 31, 2025**. The new API structure uses `/stable/` paths instead of `/api/v3/` or `/api/v4/`.

## API Endpoint Migration Map

| Feature | Old Endpoint | New Endpoint | Status |
|---------|-------------|--------------|--------|
| **Income Statement** | `/api/v3/income-statement/{ticker}` | `/stable/income-statement?symbol={ticker}` | ✅ Works (AAPL) |
| **Analyst Grades** | `/api/v3/grade/{ticker}` | `/stable/grades?symbol={ticker}` | ✅ Works (AAPL) |
| **Insider Trading** | `/api/v4/insider-trading?symbol={ticker}` | `/stable/insider-trading` (no symbol support?) | ⚠️ Limited/Premium |

## Test Results Summary

### ✅ Working Endpoints (with AAPL on free tier)

**1. Income Statement**
```
URL: https://financialmodelingprep.com/stable/income-statement
Params: symbol=AAPL, apikey=XXX, limit=4
Status: 200 OK
Response: Returns financial statement data
```

**2. Analyst Grades**
```
URL: https://financialmodelingprep.com/stable/grades
Params: symbol=AAPL, apikey=XXX, limit=10
Status: 200 OK
Response: Returns 2644 grade records
```

### ⚠️ Premium/Limited Endpoints

**3. Insider Trading**
```
Old v4: https://financialmodelingprep.com/api/v4/insider-trading
Status: 403 Forbidden
Error: "Legacy Endpoint: Due to Legacy endpoints being no longer supported -
        This endpoint is only available for legacy users who have valid
        subscriptions prior August 31, 2025."

New stable: https://financialmodelingprep.com/stable/insider-trading
Status: Returns empty array [] (possibly premium-only or unavailable on free tier)
```

### ⚠️ International Stock Limitations (ZIM ticker)

Testing with ZIM (Israeli shipping company) shows:
```
Income Statement: 402 Payment Required
Error: "Premium Query Parameter: This value set for 'symbol' is not available
        under your current subscription"

Analyst Grades: 402 Payment Required
Same error as above
```

**Conclusion:** International stocks (non-US) require premium subscription.

## Key Changes Required in Code

### 1. URL Structure
**Before:**
```python
url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
```

**After:**
```python
url = "https://financialmodelingprep.com/stable/income-statement"
params = {"symbol": ticker, "apikey": api_key, "limit": 8}
```

### 2. Parameter Format
- Ticker/symbol moved from URL path to query parameter
- Authentication still uses `apikey` query parameter
- All other parameters remain query parameters

## Implementation Plan

### Files to Update

1. **charlie_fetchers.py**
   - Line 181: `fetch_fmp_fundamentals()` - Update to `/stable/income-statement`
   - Line 511: `fetch_insider_transactions()` - Handle unavailable endpoint gracefully
   - Line 568: `fetch_analyst_recommendations()` - Update to `/stable/grades`

2. **api_validation.py**
   - Line 27: `validate_fmp_api_key()` - Update validation endpoint

3. **Test scripts** (informational only)
   - `test_fmp_api.py` - Update for reference
   - `test_fmp_new_api.py` - Update for reference

## Limitations & Recommendations

### Current Free Tier Limitations
1. ✅ US stocks (AAPL, etc.) - Income statements and analyst grades work
2. ❌ International stocks (ZIM, etc.) - Require premium subscription (402)
3. ❌ Insider trading - May require premium or is unavailable in stable API
4. ❌ Legacy v3/v4 endpoints - Deprecated August 31, 2025 (403)

### Recommendations
1. **Update all code to use `/stable/` endpoints** - Critical for functionality
2. **Handle insider trading gracefully** - Return empty list with warning, don't fail pipeline
3. **Document premium requirements** - Users should know international stocks need paid tier
4. **Consider alternative data sources** - For insider trading data if FMP doesn't support it
5. **Test with US tickers** - Use US stocks for development/testing (AAPL, MSFT, TSLA, etc.)

## API Key Status

- **API Key:** Valid ✅
- **Status:** Free tier
- **Working:** Yes, for US stocks on stable endpoints
- **Previous error "Invalid API KEY"** was due to using deprecated v3/v4 endpoints, not invalid key

## Next Steps

1. ✅ Test and validate new stable endpoints
2. ⏭️ Update `fetch_fmp_fundamentals()` to use stable API
3. ⏭️ Update `fetch_analyst_recommendations()` to use stable API
4. ⏭️ Update `fetch_insider_transactions()` to handle gracefully (may not be available)
5. ⏭️ Update validation function
6. ⏭️ Test full pipeline with US stocks
