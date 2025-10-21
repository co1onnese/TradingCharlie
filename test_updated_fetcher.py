#!/usr/bin/env python3
"""
Test updated fetch_fmp_fundamentals function
"""

import sys
from datetime import date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Import the updated function
from charlie_fetchers import fetch_fmp_fundamentals

API_KEY = "Db2EB156BSO8iSCDPuP2gCWJX2IO6shZ"

print("Testing updated fetch_fmp_fundamentals()...")
print("="*60)

# Test 1: US stock (should work)
print("\nTest 1: AAPL (US stock - should work)")
print("-"*60)
result = fetch_fmp_fundamentals("AAPL", date.today(), API_KEY)
if result:
    print(f"✓ SUCCESS: Got {len(result)} reports")
    print(f"  Latest report date: {result[0]['report_date']}")
    print(f"  Revenue: ${result[0]['normalized']['revenue']:,}")
else:
    print("✗ FAILED: No data returned")

# Test 2: International stock (should return empty with warning)
print("\nTest 2: ZIM (International stock - should warn about premium)")
print("-"*60)
result = fetch_fmp_fundamentals("ZIM", date.today(), API_KEY)
if not result:
    print("✓ EXPECTED: Empty result (premium required)")
else:
    print(f"? UNEXPECTED: Got {len(result)} reports")

# Test 3: Another US stock
print("\nTest 3: MSFT (US stock - should work)")
print("-"*60)
result = fetch_fmp_fundamentals("MSFT", date.today(), API_KEY)
if result:
    print(f"✓ SUCCESS: Got {len(result)} reports")
    print(f"  Latest report date: {result[0]['report_date']}")
    print(f"  Revenue: ${result[0]['normalized']['revenue']:,}")
else:
    print("✗ FAILED: No data returned")

print("\n" + "="*60)
print("Testing complete!")
