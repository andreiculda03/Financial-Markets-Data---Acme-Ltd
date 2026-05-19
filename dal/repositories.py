from typing import List, Optional, TypeVar, Generic
from datetime import datetime, timezone
from database.db import get_db

T = TypeVar('T')

class AssetRepository:
    """Repository for managing Financial Assets with SCD Type 2 semantics."""
    
    def __init__(self):
        db = get_db()
        if db is None:
            raise RuntimeError("Database connection is not initialized.")
        self.collection = db["assets"]

    def save(self, asset_dict: dict) -> dict:
        """Closes the previous version and inserts a new temporal record."""
        now = datetime.now(timezone.utc)
        asset_id = asset_dict["asset_id"]
        
        current = self.findLatest(asset_id)
        new_version = 1
        
        if current:
            self.collection.update_one(
                {"_id": current["_id"]},
                {"$set": {"valid_to": now}}
            )
            new_version = current.get("version", 1) + 1
            
        asset_dict["version"] = new_version
        asset_dict["valid_from"] = now
        asset_dict["valid_to"] = None
        asset_dict["is_deleted"] = False
        
        self.collection.insert_one(asset_dict)
        return asset_dict

    def findLatest(self, asset_id: str) -> Optional[dict]:
        """Returns the newest active version of an asset."""
        return self.collection.find_one({"asset_id": asset_id, "valid_to": None, "is_deleted": False})

    def findAll(self, offset: int = 0, limit: int = 20) -> List[dict]:
        """Returns all currently active assets, strictly paginated and sorted alphabetically."""
        cursor = self.collection.find(
            {"valid_to": None, "is_deleted": False}
        ).sort("asset_id", 1).skip(offset).limit(limit)
        return list(cursor)

    def delete(self, asset_id: str) -> None:
        """Logical deletion by inserting a marker record."""
        latest = self.findLatest(asset_id)
        if latest:
            now = datetime.now(timezone.utc)
            self.collection.update_one({"_id": latest["_id"]}, {"$set": {"valid_to": now}})
            latest.pop("_id", None)
            latest["version"] += 1
            latest["valid_from"] = now
            latest["valid_to"] = None
            latest["is_deleted"] = True
            self.collection.insert_one(latest)

class DataSourceRepository:
    """Repository for querying metadata about available data providers."""
    
    def __init__(self):
        db = get_db()
        if db is None:
            raise RuntimeError("Database connection is not initialized.")
        self.collection = db["assets"] # We derive sources from active asset attributes

    def findAll(self) -> List[str]:
        """Returns a distinct list of all data sources currently in use."""
        return self.collection.distinct("attributes.source", {"valid_to": None, "is_deleted": False})

    def findById(self, source_id: str) -> Optional[dict]:
        """Returns details about a specific data source."""
        doc = self.collection.find_one({"attributes.source": source_id, "valid_to": None, "is_deleted": False})
        if doc:
            return {"dataSourceId": source_id, "description": f"Data integration for {source_id.capitalize()}"}
        return None

class TimeSeriesRepository:
    """Repository for managing time series data points using MongoDB Native TS."""
    
    def __init__(self):
        db = get_db()
        if db is None:
            raise RuntimeError("Database connection is not initialized.")
        self.collection = db["time_series"]

    def save_batch(self, records: List[dict]) -> int:
        """Idempotent batch insert for Time Series data."""
        if not records:
            return 0
        result = self.collection.insert_many(records)
        return len(result.inserted_ids)

    def find_by_date_range(self, asset_id: str, data_source_id: str, start_date: datetime, end_date: datetime) -> List[dict]:
        """Fetches points ordered by business_date descending, filtered by date range and source."""
        query = {
            "meta.asset_id": asset_id,
            "meta.data_source_id": data_source_id,
            "business_date": {"$gte": start_date, "$lt": end_date}
        }
        cursor = self.collection.find(query).sort("business_date", -1)
        return list(cursor)
    def findAll(self, asset_id: str, limit: int = 100) -> List[dict]:
        """Fetches points ordered by business_date descending (latest first) for MCP/AI consumption."""
        cursor = self.collection.find(
            {"meta.asset_id": asset_id}
        ).sort("business_date", -1).limit(limit)
        return list(cursor)
    def get_coverage_summary(self) -> list:
        """Aggregates the min/max dates and total record count for each asset."""
        pipeline = [
            {
                "$group": {
                    "_id": "$meta.asset_id",
                    "oldest_record": {"$min": "$business_date"},
                    "newest_record": {"$max": "$business_date"},
                    "total_records": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}} # Sort alphabetically
        ]
        cursor = self.collection.aggregate(pipeline)
        return list(cursor)

class AnalyticsRepository:
    """Repository for accessing pre-computed Data Mining aggregations."""
    def __init__(self):
        db = get_db()
        if db is None:
            raise RuntimeError("Database connection is not initialized.")
        self.collection = db["aggregates"]

    def get_monthly_rollups(self, asset_id: str, limit: int = 12) -> list:
        """Fetches the latest monthly analytics for an asset."""
        cursor = self.collection.find(
            {"asset_id": asset_id, "interval_type": "1_month"},
            {"_id": 0} 
        ).sort("period", -1).limit(limit)
        return list(cursor)