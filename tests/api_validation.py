"""
API Key Validation Module

This module provides functions to validate API keys and check quotas
before running the pipeline to prevent unnecessary failures.
"""

import requests
import logging
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def validate_fmp_api_key(api_key: str) -> Tuple[bool, Optional[str]]:
    """
    Validate FMP API key and return status.

    Uses stable API endpoints (legacy v3/v4 deprecated August 31, 2025)
    Tests with a simple US stock (AAPL) that should work on free tier.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not api_key:
        return False, "FMP API key is empty"

    try:
        # Use stable API with a simple test - income statement for AAPL (free tier)
        url = "https://financialmodelingprep.com/stable/income-statement"
        params = {"symbol": "AAPL", "apikey": api_key, "limit": 1}
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            # Check if we got valid data (should be a list with at least one item)
            if isinstance(data, list) and len(data) > 0:
                return True, None
            else:
                return False, "API key valid but no data returned (possible quota issues)"
        elif response.status_code == 402:
            return False, "API key requires premium subscription"
        else:
            # Try to get error message from response
            try:
                data = response.json()
                error_msg = data.get("Error Message", f"HTTP {response.status_code}")
                return False, f"FMP API validation failed: {error_msg}"
            except:
                return False, f"FMP API validation failed with status {response.status_code}"
    except Exception as e:
        return False, f"FMP API validation error: {str(e)}"


def validate_eodhd_api_key(api_key: str) -> Tuple[bool, Optional[Dict]]:
    """
    Validate EODHD API key and return quota information.
    
    Returns:
        Tuple of (is_valid, quota_info)
        quota_info contains: {
            'daily_limit': int,
            'used_today': int,
            'remaining': int,
            'subscription_type': str
        }
    """
    if not api_key:
        return False, {"error": "EODHD API key is empty"}
    
    try:
        url = f"https://eodhd.com/api/user?api_token={api_key}&fmt=json"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            daily_limit = data.get('dailyRateLimit', 0)
            api_requests = data.get('apiRequests', 0)
            remaining = daily_limit - api_requests
            
            quota_info = {
                'daily_limit': daily_limit,
                'used_today': api_requests,
                'remaining': remaining,
                'subscription_type': data.get('subscriptionType', 'unknown'),
                'user_email': data.get('email', 'unknown')
            }
            
            return True, quota_info
        else:
            return False, {"error": f"EODHD API validation failed with status {response.status_code}"}
    except Exception as e:
        return False, {"error": f"EODHD API validation error: {str(e)}"}


def validate_all_api_keys(config: Dict) -> Dict[str, Dict]:
    """
    Validate all API keys in the configuration.
    
    Returns:
        Dictionary with validation results for each API
    """
    results = {}
    
    # Validate FMP
    if 'FMP_API_KEY' in config:
        is_valid, error = validate_fmp_api_key(config['FMP_API_KEY'])
        results['FMP'] = {
            'valid': is_valid,
            'error': error,
            'key_prefix': config['FMP_API_KEY'][:10] + '...' if config['FMP_API_KEY'] else 'None'
        }
    
    # Validate EODHD
    if 'EODHD_API_KEY' in config:
        is_valid, info = validate_eodhd_api_key(config['EODHD_API_KEY'])
        if is_valid:
            results['EODHD'] = {
                'valid': True,
                'quota': info,
                'key_prefix': config['EODHD_API_KEY'][:10] + '...' if config['EODHD_API_KEY'] else 'None'
            }
        else:
            results['EODHD'] = {
                'valid': False,
                'error': info.get('error', 'Unknown error'),
                'key_prefix': config['EODHD_API_KEY'][:10] + '...' if config['EODHD_API_KEY'] else 'None'
            }
    
    # Add other API validations as needed
    # Note: Some APIs like Finnhub, NewsAPI don't have simple validation endpoints
    
    return results


def print_validation_report(results: Dict[str, Dict]) -> None:
    """
    Print a formatted validation report.
    """
    print("\n" + "="*60)
    print("API KEY VALIDATION REPORT")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    for api_name, result in results.items():
        print(f"{api_name} API:")
        print(f"  Key: {result.get('key_prefix', 'Not configured')}")
        print(f"  Status: {'✓ Valid' if result['valid'] else '✗ Invalid'}")
        
        if not result['valid']:
            print(f"  Error: {result.get('error', 'Unknown error')}")
        elif api_name == 'EODHD' and 'quota' in result:
            quota = result['quota']
            print(f"  Subscription: {quota['subscription_type']}")
            print(f"  Daily Limit: {quota['daily_limit']}")
            print(f"  Used Today: {quota['used_today']}")
            print(f"  Remaining: {quota['remaining']}")
            if quota['remaining'] <= 0:
                print("  ⚠️  WARNING: Daily quota exhausted!")
        
        print()
    
    print("="*60 + "\n")


def check_api_readiness(config: Dict) -> bool:
    """
    Check if APIs are ready for use.
    
    Returns:
        True if critical APIs are valid and have quota remaining
    """
    results = validate_all_api_keys(config)
    
    # Check FMP
    if 'FMP' in results and not results['FMP']['valid']:
        logger.error(f"FMP API key is invalid: {results['FMP'].get('error')}")
        return False
    
    # Check EODHD
    if 'EODHD' in results:
        if not results['EODHD']['valid']:
            logger.error(f"EODHD API key is invalid: {results['EODHD'].get('error')}")
            return False
        elif results['EODHD']['quota']['remaining'] <= 0:
            logger.error("EODHD API daily quota exhausted")
            return False
    
    return True


if __name__ == "__main__":
    # Load environment variables from .env.local
    from dotenv import load_dotenv
    import os
    from pathlib import Path
    
    # Check if .env.local exists
    env_file = Path('.env.local')
    if not env_file.exists():
        print("❌ Error: .env.local file not found!")
        print("   Please create .env.local with your API keys:")
        print("   FMP_API_KEY=your_fmp_key_here")
        print("   EODHD_API_KEY=your_eodhd_key_here")
        exit(1)
    
    # Load environment variables from .env.local
    load_dotenv('.env.local')
    
    # Get API keys from environment variables
    config = {
        'FMP_API_KEY': os.environ.get('FMP_API_KEY', ''),
        'EODHD_API_KEY': os.environ.get('EODHD_API_KEY', ''),
    }
    
    # Check if API keys are loaded
    missing_keys = [key for key, value in config.items() if not value]
    if missing_keys:
        print(f"❌ Error: Missing API keys in .env.local: {', '.join(missing_keys)}")
        print("   Please add the missing keys to your .env.local file")
        exit(1)
    
    # Validate all API keys
    results = validate_all_api_keys(config)
    print_validation_report(results)
    
    # Check overall readiness
    if check_api_readiness(config):
        print("✓ All critical APIs are ready for use")
    else:
        print("✗ Some APIs are not ready. Please check the errors above.")