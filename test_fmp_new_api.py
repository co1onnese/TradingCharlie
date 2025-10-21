#!/usr/bin/env python3
"""
FMP New API Testing Script
Tests to find the new FMP API structure after August 31, 2025 changes
"""

import requests
import json
from datetime import datetime

# API Key from .env.local
API_KEY = "Db2EB156BSO8iSCDPuP2gCWJX2IO6shZ"

def test_endpoint(name, url, params=None, headers=None):
    """Test an API endpoint and print results"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")
    print("-" * 60)
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        # Try to parse JSON response
        try:
            data = response.json()
            if response.status_code == 200:
                print(f"SUCCESS! Response: {json.dumps(data, indent=2)[:1000]}...")
            else:
                print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        except:
            print(f"Response (text): {response.text[:500]}...")
            
        return response.status_code, response
    except Exception as e:
        print(f"Error: {str(e)}")
        return None, None

def main():
    print(f"FMP Stable API Test - Updated Implementation")
    print(f"Timestamp: {datetime.now()}")
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-4:]}")
    print(f"\nNote: Testing new /stable/ endpoints after August 31, 2025 deprecation")
    print("All v3/v4 endpoints now return 403 Forbidden")

    # Test the new stable endpoints we've implemented
    # Use AAPL for testing since it works on free tier (US stock)

    # Test 1: Income Statement (stable API)
    test_endpoint(
        "Income Statement - STABLE API (AAPL)",
        "https://financialmodelingprep.com/stable/income-statement",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 4}
    )

    # Test 2: Analyst Grades (stable API)
    test_endpoint(
        "Analyst Grades - STABLE API (AAPL)",
        "https://financialmodelingprep.com/stable/grades",
        params={"symbol": "AAPL", "apikey": API_KEY, "limit": 10}
    )

    # Test 3: Insider Trading (stable API) - may be premium
    test_endpoint(
        "Insider Trading - STABLE API",
        "https://financialmodelingprep.com/stable/insider-trading",
        params={"apikey": API_KEY, "limit": 20}
    )

    # Test 4: Test with ZIM (international) - should fail with 402
    test_endpoint(
        "Income Statement - STABLE API (ZIM - International)",
        "https://financialmodelingprep.com/stable/income-statement",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 4}
    )

    # Test 5: Test analyst grades with ZIM
    test_endpoint(
        "Analyst Grades - STABLE API (ZIM - International)",
        "https://financialmodelingprep.com/stable/grades",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 10}
    )

def test_fetch_functions():
    """Test our updated fetch functions directly - minimal test"""
    print("\n" + "="*60)
    print("TESTING UPDATED FETCH FUNCTIONS")
    print("="*60)

    # Simple direct test of the stable endpoints
    import requests
    from datetime import date

    test_date = date.today()
    api_key = API_KEY

    print(f"Testing with date: {test_date}")

    # Test fundamentals endpoint directly
    print("\nTesting stable/income-statement endpoint...")
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/income-statement",
            params={"symbol": "AAPL", "apikey": api_key, "limit": 2},
            timeout=10
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Success! Returned {len(data)} financial reports")
        else:
            print(f"Failed: {response.text[:100]}...")
    except Exception as e:
        print(f"Error: {e}")

    # Test grades endpoint directly
    print("\nTesting stable/grades endpoint...")
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/grades",
            params={"symbol": "AAPL", "apikey": api_key, "limit": 5},
            timeout=10
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Success! Returned {len(data)} analyst grades")
        else:
            print(f"Failed: {response.text[:100]}...")
    except Exception as e:
        print(f"Error: {e}")

    # Test insider trading endpoint directly
    print("\nTesting stable/insider-trading endpoint...")
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/insider-trading",
            params={"apikey": api_key, "limit": 10},
            timeout=10
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Success! Returned {len(data)} insider transactions")
        else:
            print(f"Endpoint not available or premium-only: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "="*60)

if __name__ == "__main__":
    main()
    test_fetch_functions()