#!/usr/bin/env python3
"""
FMP Stable API Test Script
Tests the new /stable/ endpoints to verify they work before updating main code
"""

import requests
import json
from datetime import datetime

# API Key from .env.local
API_KEY = "Db2EB156BSO8iSCDPuP2gCWJX2IO6shZ"

def test_endpoint(name, url, params=None):
    """Test an API endpoint and print results"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")
    print("-" * 60)

    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")

        # Try to parse JSON response
        try:
            data = response.json()
            if response.status_code == 200:
                print(f"✓ SUCCESS!")
                # Show a sample of the data
                if isinstance(data, list):
                    print(f"Response: List with {len(data)} items")
                    if len(data) > 0:
                        print(f"First item: {json.dumps(data[0], indent=2)[:500]}...")
                elif isinstance(data, dict):
                    print(f"Response: {json.dumps(data, indent=2)[:500]}...")
            else:
                print(f"✗ FAILED")
                print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        except:
            print(f"Response (text): {response.text[:500]}...")

        return response.status_code, response
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return None, None

def main():
    print(f"FMP Stable API Test Script")
    print(f"Timestamp: {datetime.now()}")
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-4:]}")
    print("\nTesting new /stable/ endpoints...")

    # Test 1: Income Statement (stable API)
    status, _ = test_endpoint(
        "Income Statement (Stable API) - AAPL",
        "https://financialmodelingprep.com/stable/income-statement",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 4}
    )

    if status == 200:
        print("\n✓ Income Statement endpoint works!")
    else:
        print("\n✗ Income Statement endpoint failed")

    # Test 2a: Insider Trading (stable API)
    status, _ = test_endpoint(
        "Insider Trading (Stable API) - AAPL",
        "https://financialmodelingprep.com/stable/latest-insider-trade",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 10}
    )

    # Test 2b: Try the v4 endpoint (might not be migrated yet)
    status_v4, _ = test_endpoint(
        "Insider Trading (v4 API) - AAPL",
        "https://financialmodelingprep.com/api/v4/insider-trading",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 10}
    )

    if status == 200:
        print("\n✓ Insider Trading stable endpoint works!")
    elif status_v4 == 200:
        print("\n⚠ Insider Trading still on v4 endpoint (not migrated to stable yet)")
    else:
        print("\n✗ Insider Trading endpoint failed on both stable and v4")

    # Test 3: Analyst Grades (stable API)
    status, _ = test_endpoint(
        "Analyst Grades (Stable API) - AAPL",
        "https://financialmodelingprep.com/stable/grades",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 10}
    )

    if status == 200:
        print("\n✓ Analyst Grades endpoint works!")
    else:
        print("\n✗ Analyst Grades endpoint failed")

    # Test 4: Try with ZIM (the ticker from our pipeline)
    print("\n" + "="*60)
    print("Testing with ZIM ticker (from pipeline)...")
    print("="*60)

    test_endpoint(
        "Income Statement - ZIM",
        "https://financialmodelingprep.com/stable/income-statement",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 4}
    )

    test_endpoint(
        "Insider Trading (stable) - ZIM",
        "https://financialmodelingprep.com/stable/latest-insider-trade",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 10}
    )

    test_endpoint(
        "Insider Trading (v4) - ZIM",
        "https://financialmodelingprep.com/api/v4/insider-trading",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 10}
    )

    test_endpoint(
        "Analyst Grades - ZIM",
        "https://financialmodelingprep.com/stable/grades",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 10}
    )

if __name__ == "__main__":
    main()
