from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime, timezone

def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)

# --- 1. DATA SOURCE MODELS ---
class DataSourceBase(BaseModel):
    """Represents the vendor providing the data."""
    name: str = Field(..., description="E.g., Alpha Vantage, CoinGecko")
    description: str = Field(..., description="Description of the API or dataset")
    attributes: Dict[str, str] = Field(default_factory=dict, description="e.g., API tier, rate limits")

class DataSourceRecord(DataSourceBase):
    data_source_id: str = Field(..., description="Unique provider ID (e.g., 'alphavantage')")
    system_date: datetime = Field(default_factory=get_utc_now, description="Audit timestamp ")

# --- 2. ASSET MODELS (SCD TYPE 2) ---
class AssetBase(BaseModel):
    """Core attributes for all financial instruments[cite: 361]."""
    symbol: str = Field(..., description="Ticker symbol, e.g., TSLA, BTC")
    asset_class: str = Field(..., description="stock, crypto, commodity, etc.")
    description: str = Field(..., description="Description of the financial instrument")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Flexible dictionary for heterogeneous data [cite: 506]")

class TemporalAssetRecord(AssetBase):
    """The database record that includes temporal versioning rules."""
    asset_id: str = Field(..., description="Logical ID grouping all versions of this asset")
    version: int = Field(default=1, description="Version number of this asset record")
    valid_from: datetime = Field(default_factory=get_utc_now, description="When this record became valid")
    valid_to: Optional[datetime] = Field(default=None, description="When this record was superseded or deleted [cite: 471]")
    is_deleted: bool = Field(default=False, description="Marker to indicate if the asset is logically deleted [cite: 473]")

# --- 3. TIME SERIES MODELS (BI-TEMPORAL) ---
class TimeSeriesMeta(BaseModel):
    """Metadata for the time-series record, used by MongoDB for compression."""
    asset_id: str = Field(..., description="Reference to the logical asset_id [cite: 363]")
    data_source_id: str = Field(..., description="Vendor ID for provenance [cite: 363]")

class TimeSeriesDataPoint(BaseModel):
    """Represents a single point in time using the Time Series Pattern[cite: 351, 353]."""
    business_date: datetime = Field(..., description="When the value was true in the market ")
    system_date: datetime = Field(default_factory=get_utc_now, description="When it was ingested (Audit) ")
    meta: TimeSeriesMeta = Field(..., description="Indexed metadata field for clustering [cite: 355]")
    indicators: Dict[str, float] = Field(..., description="Numerical market data indicators [cite: 472]")
    
    # Provenance
    ingest_id: str = Field(..., description="Identifier for the specific ETL batch job")
    quality_flags: int = Field(default=0, description="0 = Clean. Higher numbers indicate data warnings.")