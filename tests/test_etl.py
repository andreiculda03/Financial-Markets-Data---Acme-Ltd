import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

# Import the ETL pipeline
from ingestion.etl import FinancialETLPipeline

@pytest.fixture
def sample_yfinance_data():
    """Creates a mock Pandas DataFrame mimicking Yahoo Finance output."""
    dates = pd.date_range(start="2026-05-01", periods=3, tz="UTC")
    data = {
        "Open": [150.0, 151.0, 152.0],
        "Close": [150.5, 151.5, 152.5],
        "Volume": [1000, 1100, 1200]
    }
    return pd.DataFrame(data, index=dates)

class TestFinancialETL:

    @patch('ingestion.etl.TimeSeriesRepository')
    @patch('ingestion.etl.AssetRepository')
    def test_transform_canonical_asset_id_stock(self, mock_asset, mock_ts, sample_yfinance_data):
        """Test if a standard stock symbol is formatted correctly."""
        pipeline = FinancialETLPipeline(provider="yfinance")
        pipeline.reset_stats()
        
        records = pipeline.transform_market_data(sample_yfinance_data, "AAPL", "job_123")
        
        assert len(records) == 3
        # AAPL should become aapl_stock_01
        assert records[0]["meta"]["asset_id"] == "aapl_stock_01"
        assert records[0]["meta"]["data_source_id"] == "yfinance"

    @patch('ingestion.etl.TimeSeriesRepository')
    @patch('ingestion.etl.AssetRepository')
    def test_transform_canonical_asset_id_crypto(self, mock_asset, mock_ts, sample_yfinance_data):
        """Test if a crypto symbol with a hyphen is formatted correctly."""
        pipeline = FinancialETLPipeline(provider="yfinance")
        pipeline.reset_stats() # <-- FIX
        
        records = pipeline.transform_market_data(sample_yfinance_data, "BTC-USD", "job_123")
        
        assert records[0]["meta"]["asset_id"] == "btc_usd_crypto_01"

    @patch('ingestion.etl.TimeSeriesRepository')
    @patch('ingestion.etl.AssetRepository')
    def test_transform_canonical_asset_id_index(self, mock_asset, mock_ts, sample_yfinance_data):
        """Test if a market index with a caret (^) is formatted correctly."""
        pipeline = FinancialETLPipeline(provider="yfinance")
        pipeline.reset_stats() # <-- FIX
        
        records = pipeline.transform_market_data(sample_yfinance_data, "^GSPC", "job_123")
        assert records[0]["meta"]["asset_id"] == "gspc_index_01"

    @patch('ingestion.etl.TimeSeriesRepository')
    @patch('ingestion.etl.AssetRepository')
    def test_transform_data_shape(self, mock_asset, mock_ts, sample_yfinance_data):
        """Ensure the numerical data is correctly mapped into the indicators dictionary."""
        pipeline = FinancialETLPipeline(provider="yfinance")
        pipeline.reset_stats() # <-- FIX
        
        records = pipeline.transform_market_data(sample_yfinance_data, "AAPL", "job_123")
        
        first_record = records[0]
        
        assert "indicators" in first_record
        assert first_record["indicators"]["open"] == 150.0
        assert first_record["indicators"]["close"] == 150.5
        assert first_record["indicators"]["volume"] == 1000
        
        assert first_record["ingest_id"] == "job_123"
        assert first_record["quality_flags"] == 0
        assert "business_date" in first_record