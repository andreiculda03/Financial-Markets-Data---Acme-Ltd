import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from dal.repositories import AssetRepository, TimeSeriesRepository

@pytest.fixture
def mock_db():
    """Creates a mock MongoDB collection to prevent live database calls during testing."""
    with patch('dal.repositories.get_db') as mock_get_db:
        mock_db_instance = MagicMock()
        mock_collection = MagicMock()
        mock_db_instance.__getitem__.return_value = mock_collection
        mock_get_db.return_value = mock_db_instance
        
        yield mock_collection

class TestAssetRepository:
    
    def test_save_new_asset(self, mock_db):
        """Test inserting a brand new asset with no previous history."""
        repo = AssetRepository()
        mock_db.find_one.return_value = None
        
        test_asset = {"asset_id": "test_stock_01", "symbol": "TEST"}
        result = repo.save(test_asset)
        
        assert result["version"] == 1
        assert result["is_deleted"] is False
        assert result["valid_to"] is None
        assert "valid_from" in result
        
        mock_db.insert_one.assert_called_once()

    def test_save_existing_asset_versioning(self, mock_db):
        """Test SCD Type 2 logic: Updating an existing asset bumps the version."""
        repo = AssetRepository()

        mock_db.find_one.return_value = {
            "_id": "fake_id_123",
            "asset_id": "test_stock_01",
            "version": 1
        }
        
        test_asset = {"asset_id": "test_stock_01", "symbol": "TEST-UPDATED"}
        result = repo.save(test_asset)
        
        assert result["version"] == 2
        mock_db.update_one.assert_called_once()
        mock_db.insert_one.assert_called_once()

    def test_find_latest_active(self, mock_db):
        """Ensure finding the latest asset filters for active records only."""
        repo = AssetRepository()
        
        mock_db.find_one.return_value = {"asset_id": "test_stock", "version": 1}
        result = repo.findLatest("test_stock")
        
        mock_db.find_one.assert_called_with({
            "asset_id": "test_stock", 
            "valid_to": None, 
            "is_deleted": False
        })
        assert result is not None

    def test_logical_delete(self, mock_db):
        """Test that deleting an asset does not erase it, but marks it deleted."""
        repo = AssetRepository()
        
        # Simulate finding the active record
        mock_db.find_one.return_value = {
            "_id": "fake_id_123",
            "asset_id": "test_stock_01",
            "version": 1
        }
        
        repo.delete("test_stock_01")
        mock_db.update_one.assert_called_once()
        
        args, kwargs = mock_db.insert_one.call_args
        inserted_doc = args[0]
        
        assert inserted_doc["version"] == 2
        assert inserted_doc["is_deleted"] is True
        assert inserted_doc["valid_to"] is None


class TestTimeSeriesRepository:
    
    def test_idempotent_save_batch(self, mock_db):
        """Test the Filter & Insert functionality for Native Time Series data."""
        repo = TimeSeriesRepository()
        
        d1 = datetime.now()
        d2 = datetime.now()
        
        mock_records = [
            {"meta": {"asset_id": "btc_crypto"}, "business_date": d1},
            {"meta": {"asset_id": "btc_crypto"}, "business_date": d2}
        ]
        mock_db.find.return_value = [{"business_date": d1}]
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = ["fake_id_1"]
        mock_db.insert_many.return_value = mock_insert_result
        
        count = repo.save_batch(mock_records)
        assert count == 1
        mock_db.insert_many.assert_called_once()

    def test_empty_save_batch(self, mock_db):
        """Ensure passing an empty list doesn't crash the database."""
        repo = TimeSeriesRepository()
        count = repo.save_batch([])
        
        assert count == 0
        mock_db.insert_many.assert_not_called()