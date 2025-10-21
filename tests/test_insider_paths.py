#!/usr/bin/env python3
"""
Test different insider trading endpoint paths to find the correct one
"""

import requests
import json

API_KEY = "Db2EB156BSO8iSCDPuP2gCWJX2IO6shZ"

# Test different possible paths
paths = [
    "/stable/insider-trading",
    "/stable/insider-trade",
    "/stable/latest-insider-trading",
    "/stable/latest-insider-trade",
    "/stable/search-insider-trades",
    "/stable/insider-trades",
]

print("Testing insider trading endpoint paths...")
print("="*60)

for path in paths:
    url = f"https://financialmodelingprep.com{path}"
    params = {"symbol": "AAPL", "apikey": API_KEY, "limit": 5}

    try:
        response = requests.get(url, params=params, timeout=10)
        status = response.status_code

        print(f"\n{path}")
        print(f"  Status: {status}")

        if status == 200:
            data = response.json()
            if isinstance(data, list):
                print(f"  ✓ SUCCESS! Returned {len(data)} items")
                if len(data) > 0:
                    print(f"  Sample: {json.dumps(data[0], indent=4)[:200]}...")
            else:
                print(f"  ✓ SUCCESS! Response type: {type(data)}")
        elif status == 404:
            print(f"  ✗ Not found")
        elif status == 403:
            try:
                err = response.json()
                print(f"  ✗ Forbidden: {err.get('Error Message', 'Unknown')[:100]}")
            except:
                print(f"  ✗ Forbidden")
        elif status == 402:
            print(f"  ⚠ Premium required")
        else:
            print(f"  ? Unknown status")

    except Exception as e:
        print(f"  ✗ Error: {str(e)[:100]}")

print("\n" + "="*60)
