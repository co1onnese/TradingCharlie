# test_charlie_pipeline.py
#
# Unit tests for Charlie TR1 pipeline
# M5: Basic tests for critical functions

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from charlie_utils import (
    compute_content_hash, normalize_to_utc, compute_bucket,
    check_relevance, compute_labels_for_asset
)


class TestContentHash:
    """Tests for content_hash deduplication"""

    def test_compute_content_hash_basic(self):
        """Test basic content hash computation"""
        headline = "Apple Reports Strong Q1 Earnings"
        url = "https://example.com/article"
        published_at = datetime(2024, 1, 15, 10, 30)

        hash1 = compute_content_hash(headline, url, published_at)
        hash2 = compute_content_hash(headline, url, published_at)

        # Same inputs should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 produces 64 hex characters

    def test_compute_content_hash_different_inputs(self):
        """Test that different inputs produce different hashes"""
        published_at = datetime(2024, 1, 15, 10, 30)

        hash1 = compute_content_hash("Headline 1", "url1", published_at)
        hash2 = compute_content_hash("Headline 2", "url1", published_at)
        hash3 = compute_content_hash("Headline 1", "url2", published_at)

        # Different headlines or URLs should produce different hashes
        assert hash1 != hash2
        assert hash1 != hash3

    def test_compute_content_hash_handles_none(self):
        """Test that None values are handled gracefully"""
        published_at = datetime(2024, 1, 15)

        hash1 = compute_content_hash(None, "url", published_at)
        hash2 = compute_content_hash("headline", None, published_at)

        # Should not raise errors
        assert isinstance(hash1, str)
        assert isinstance(hash2, str)


class TestBucketComputation:
    """Tests for temporal bucket assignment"""

    def test_compute_bucket_0_3_days(self):
        """Test bucket assignment for recent articles (0-3 days)"""
        as_of_date = date(2024, 1, 15)
        published_at = datetime(2024, 1, 14, 10, 0)  # 1 day ago

        bucket = compute_bucket(published_at, as_of_date)
        assert bucket == "0-3"

    def test_compute_bucket_4_10_days(self):
        """Test bucket assignment for medium-age articles (4-10 days)"""
        as_of_date = date(2024, 1, 15)
        published_at = datetime(2024, 1, 10, 10, 0)  # 5 days ago

        bucket = compute_bucket(published_at, as_of_date)
        assert bucket == "4-10"

    def test_compute_bucket_11_30_days(self):
        """Test bucket assignment for older articles (11-30 days)"""
        as_of_date = date(2024, 1, 30)
        published_at = datetime(2024, 1, 10, 10, 0)  # 20 days ago

        bucket = compute_bucket(published_at, as_of_date)
        assert bucket == "11-30"

    def test_compute_bucket_too_old(self):
        """Test that very old articles return None"""
        as_of_date = date(2024, 2, 15)
        published_at = datetime(2024, 1, 1, 10, 0)  # 45 days ago

        bucket = compute_bucket(published_at, as_of_date)
        assert bucket is None

    def test_compute_bucket_future_article(self):
        """Test that future articles return None"""
        as_of_date = date(2024, 1, 15)
        published_at = datetime(2024, 1, 20, 10, 0)  # 5 days in future

        bucket = compute_bucket(published_at, as_of_date)
        assert bucket is None


class TestRelevanceFiltering:
    """Tests for relevance checking"""

    def test_check_relevance_ticker_match(self):
        """Test relevance when ticker appears in headline"""
        headline = "AAPL announces new iPhone"
        snippet = "Apple Inc. reports strong sales"
        ticker = "AAPL"

        assert check_relevance(headline, snippet, ticker) is True

    def test_check_relevance_company_name(self):
        """Test relevance when company name appears"""
        headline = "Apple announces new product"
        snippet = "The company reports..."
        ticker = "AAPL"
        company_name = "Apple"

        assert check_relevance(headline, snippet, ticker, company_name) is True

    def test_check_relevance_no_match(self):
        """Test relevance when ticker/company not mentioned"""
        headline = "Market update for tech sector"
        snippet = "General market news..."
        ticker = "AAPL"

        assert check_relevance(headline, snippet, ticker) is False

    def test_check_relevance_too_short(self):
        """Test that very short articles are marked irrelevant"""
        headline = "News"
        snippet = "..."
        ticker = "AAPL"

        assert check_relevance(headline, snippet, ticker) is False


class TestLabelGeneration:
    """Tests for label generation with per-asset quantiles"""

    def test_compute_labels_basic(self):
        """Test basic label computation"""
        # Create synthetic price series
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        close_prices = [100 + i + np.sin(i/5)*5 for i in range(100)]

        price_df = pd.DataFrame({'close': close_prices}, index=dates)

        labeled = compute_labels_for_asset(price_df)

        # Should return dataframe with required columns
        assert 'composite_signal' in labeled.columns
        assert 'label_class' in labeled.columns
        assert 'quantile' in labeled.columns
        assert len(labeled) == len(price_df)

    def test_compute_labels_class_distribution(self):
        """Test that label classes use asymmetric quantiles"""
        # Create longer price series for better distribution
        dates = pd.date_range('2024-01-01', periods=200, freq='D')

        # Create synthetic data with trend
        np.random.seed(42)
        close_prices = 100 + np.cumsum(np.random.randn(200) * 2)

        price_df = pd.DataFrame({'close': close_prices}, index=dates)
        labeled = compute_labels_for_asset(price_df)

        # Filter out NaN labels (early periods without enough forward data)
        valid_labels = labeled['label_class'].dropna()

        if len(valid_labels) > 0:
            # Should have 5 classes
            unique_classes = valid_labels.unique()
            assert all(cls in [1, 2, 3, 4, 5] for cls in unique_classes if not pd.isna(cls))

    def test_compute_labels_handles_insufficient_data(self):
        """Test behavior with insufficient data"""
        # Very short series
        dates = pd.date_range('2024-01-01', periods=5, freq='D')
        close_prices = [100, 101, 102, 103, 104]

        price_df = pd.DataFrame({'close': close_prices}, index=dates)
        labeled = compute_labels_for_asset(price_df)

        # Should not crash, may have NaN values
        assert 'composite_signal' in labeled.columns
        assert len(labeled) == 5


class TestTimezoneNormalization:
    """Tests for timezone normalization"""

    def test_normalize_to_utc_datetime(self):
        """Test normalization of datetime with timezone"""
        # Create datetime with timezone
        from dateutil import tz
        dt_est = datetime(2024, 1, 15, 10, 0, tzinfo=tz.gettz('America/New_York'))

        normalized = normalize_to_utc(dt_est)

        # Should return timezone-naive datetime
        assert normalized is not None
        assert normalized.tzinfo is None
        # EST is UTC-5, so 10:00 EST should be 15:00 UTC
        # (but we're returning naive datetime at UTC value)

    def test_normalize_to_utc_string(self):
        """Test normalization from ISO string"""
        dt_string = "2024-01-15T10:30:00Z"

        normalized = normalize_to_utc(dt_string)

        assert normalized is not None
        assert isinstance(normalized, datetime)

    def test_normalize_to_utc_invalid(self):
        """Test handling of invalid input"""
        normalized = normalize_to_utc(None)
        assert normalized is None


# Pytest configuration
def test_imports():
    """Sanity check that imports work"""
    assert compute_content_hash is not None
    assert compute_bucket is not None
    assert check_relevance is not None
    assert compute_labels_for_asset is not None


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
