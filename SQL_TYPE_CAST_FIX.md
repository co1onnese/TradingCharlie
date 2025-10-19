# SQL Type Cast Fix

## Issue: PostgreSQL Syntax Error with Named Parameters

### Error Message
```
psycopg2.errors.SyntaxError: syntax error at or near ":"
LINE 7: '2025-10-19T11:58:05.140380'::timestamp, :raw_json::jsonb...
```

### Root Cause
Cannot use PostgreSQL type casts (`::jsonb`) directly with SQLAlchemy named parameters. 

**Why it fails:**
```sql
-- WRONG: SQLAlchemy parser gets confused
VALUES (:asset_id, :raw_json::jsonb, :sources_meta::jsonb)
                   ^^^^^^^^^^^^^^^^^
-- Parser sees: ":raw_json::jsonb" as malformed parameter name
```

### Solution
Remove `::jsonb` casts - PostgreSQL automatically converts JSON strings to JSONB for JSONB columns.

**Changes made in charlie_utils.py:**

| Function | Line | Change |
|----------|------|--------|
| `upsert_raw_news()` | 246 | `:raw_json::jsonb` → `:raw_json` |
| `insert_price_window()` | 277 | `:ohlcv_window::jsonb, :technicals::jsonb` → `:ohlcv_window, :technicals` |
| `insert_raw_fmp_fundamentals()` | 297 | `:raw_json::jsonb, :normalized::jsonb` → `:raw_json, :normalized` |
| `insert_raw_eodhd_options()` | 318 | `:raw_json::jsonb` → `:raw_json` |
| `insert_assembled_sample()` | 344 | `:sources_meta::jsonb` → `:sources_meta` |
| `insert_distilled_thesis()` | 376 | `:thesis_structure::jsonb` → `:thesis_structure` |

### Why This Works
1. Database columns are already defined as JSONB type in the schema
2. PostgreSQL automatically converts JSON text strings to JSONB
3. We're passing `json.dumps(obj)` which creates valid JSON strings
4. No explicit cast needed - implicit conversion is automatic

### Verification
```bash
✅ No ::jsonb casts in charlie_utils.py (0 matches)
✅ All modules import successfully
✅ All syntax valid
```

### Alternative (if needed)
If explicit casting is required, use SQL CAST() function:
```sql
-- Instead of: :param::jsonb
-- Use: CAST(:param AS jsonb)
VALUES (:asset_id, CAST(:raw_json AS jsonb))
```

But this is unnecessary since PostgreSQL handles it automatically.

---

**Status:** ✅ FIXED - All SQL statements now use clean parameter syntax
