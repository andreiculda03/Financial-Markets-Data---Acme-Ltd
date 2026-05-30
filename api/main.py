from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any
from datetime import datetime, timezone

from database.db import connect_to_mongo, close_mongo_connection
from models.schemas import AssetBase, TemporalAssetRecord
from dal.repositories import AssetRepository, TimeSeriesRepository, DataSourceRepository

class AssetCreate(AssetBase):
    asset_id: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_to_mongo()
    yield
    close_mongo_connection()

app = FastAPI(
    title="Acme Financial Data Warehouse API",
    lifespan=lifespan
)

@app.post("/assets", response_model=TemporalAssetRecord)
def create_or_update_asset(asset: AssetCreate) -> TemporalAssetRecord:
    repo = AssetRepository()
    try:
        saved_asset = repo.save(asset.model_dump())
        return TemporalAssetRecord(**saved_asset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save asset: {str(e)}")

@app.get("/assets", response_model=List[str])
def get_all_assets(
    offset: int = Query(0, description="Starting position in the collection"),
    limit: int = Query(20, description="Maximum number of returned asset ids")
) -> List[str]:
    """[1.1] Returns a paginated JSON array of IDs of all available assets."""
    repo = AssetRepository()
    try:
        assets = repo.findAll(offset=offset, limit=limit)
        return [doc["asset_id"] for doc in assets]
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/assets/{asset_id}", response_model=TemporalAssetRecord)
def get_asset_details(asset_id: str) -> TemporalAssetRecord:
    """[1.2] Return the detailed JSON representation for the asset."""
    repo = AssetRepository()
    asset = repo.findLatest(asset_id)
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found or logically deleted")
    return TemporalAssetRecord(**asset)

@app.delete("/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: str):
    """Logical Deletion Implementation."""
    repo = AssetRepository()
    asset = repo.findLatest(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
        
    repo.delete(asset_id)
    return None

@app.get("/data-sources", response_model=List[str])
def get_data_sources(
    offset: int = Query(0, description="Starting position in the collection"),
    limit: int = Query(20, description="Maximum number of returned data sources")
) -> List[str]:
    """[2] Returns the list of data sources' ids."""
    repo = DataSourceRepository()
    sources = repo.findAll()
    return sources[offset : offset + limit]

@app.get("/data-sources/{dataSourceId}", response_model=Dict[str, str])
def get_data_source_details(dataSourceId: str) -> Dict[str, str]:
    """[2] Returns the detail of a specified data source."""
    repo = DataSourceRepository()
    source = repo.findById(dataSourceId)
    if not source:
        raise HTTPException(status_code=404, detail="Data source not found")
    return source

@app.get("/data")
def get_time_series_data(
    assetId: str = Query(..., description="Asset ID"),
    dataSourceId: str = Query(..., description="Data Source ID"),
    startBusinessDate: str = Query(..., description="Oldest date (YYYY-MM-DD)"),
    endBusinessDate: str = Query(..., description="Newest date (YYYY-MM-DD)"),
    includeAttributes: bool = Query(False, description="Include list of attributes")
):
    """Returns the time series data for a specified asset and data source.
    Strictly follows the response payload shape requested in the specification."""
    try:
        start_dt = datetime.strptime(startBusinessDate, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(endBusinessDate, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    repo = TimeSeriesRepository()
    try:
        records = repo.find_by_date_range(assetId, dataSourceId, start_dt, end_dt)
        
        formatted_records = []
        for r in records:
            formatted_records.append({
                "businessDate": r["business_date"].strftime("%Y-%m-%d"),
                "values": [r.get("indicators", {})] 
            })
        response: Dict[str, Any] = {
            "data": {
                "assetId": assetId,
                "datasourceId": dataSourceId,
                "records": formatted_records
            }
        }
        
        if includeAttributes:
            if records and "indicators" in records[0]:
                response["attributes"] = list(records[0]["indicators"].keys())
            else:
                response["attributes"] = ["open", "high", "low", "close", "volume"] 
                
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")