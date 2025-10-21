#!/usr/bin/env python3
"""
FMP API Testing Script
Tests various endpoints and authentication methods to diagnose API issues
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
    if headers:
        print(f"Headers: {headers}")
    print("-" * 60)
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        # Try to parse JSON response
        try:
            data = response.json()
            print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        except:
            print(f"Response (text): {response.text[:500]}...")
            
        return response.status_code, response
    except Exception as e:
        print(f"Error: {str(e)}")
        return None, None

def main():
    print(f"FMP API Test Script")
    print(f"Timestamp: {datetime.now()}")
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-4:]}")
    
    # Test 1: API key validation endpoint (from their docs)
    test_endpoint(
        "API Key Validation",
        f"https://financialmodelingprep.com/api/v3/apikey/{API_KEY}"
    )
    
    # Test 2: Simple profile endpoint with API key as parameter
    test_endpoint(
        "Company Profile (AAPL) - v3 with apikey param",
        "https://financialmodelingprep.com/api/v3/profile/AAPL",
        params={"apikey": API_KEY}
    )
    
    # Test 3: Income statement (the failing endpoint) with different versions
    test_endpoint(
        "Income Statement v3 - with apikey param",
        "https://financialmodelingprep.com/api/v3/income-statement/ZIM",
        params={"apikey": API_KEY, "limit": 8}
    )
    
    # Test 4: Try v4 endpoint
    test_endpoint(
        "Income Statement v4 - with apikey param",
        "https://financialmodelingprep.com/api/v4/income-statement/ZIM",
        params={"apikey": API_KEY, "limit": 8}
    )
    
    # Test 5: Try with API key in header (some APIs use this)
    test_endpoint(
        "Income Statement v3 - API key in header",
        "https://financialmodelingprep.com/api/v3/income-statement/ZIM",
        params={"limit": 8},
        headers={"apikey": API_KEY}
    )
    
    # Test 6: Try a free endpoint (stock list)
    test_endpoint(
        "Stock List (Free endpoint)",
        "https://financialmodelingprep.com/api/v3/stock/list",
        params={"apikey": API_KEY}
    )
    
    # Test 7: Try quote endpoint (usually works on free tier)
    test_endpoint(
        "Stock Quote (AAPL)",
        "https://financialmodelingprep.com/api/v3/quote/AAPL",
        params={"apikey": API_KEY}
    )
    
    # Test 8: Insider trading endpoint (v4)
    test_endpoint(
        "Insider Trading v4",
        "https://financialmodelingprep.com/api/v4/insider-trading",
        params={"symbol": "ZIM", "apikey": API_KEY, "limit": 50}
    )
    
    # Test 9: Analyst recommendations (v3)
    test_endpoint(
        "Analyst Recommendations v3", 
        "https://financialmodelingprep.com/api/v3/grade/ZIM",
        params={"apikey": API_KEY, "limit": 20}
    )
    
    # Test 10: Try without any authentication to see the difference
    test_endpoint(
        "Income Statement - No API Key",
        "https://financialmodelingprep.com/api/v3/income-statement/ZIM"
    )

if __name__ == "__main__":
    main()