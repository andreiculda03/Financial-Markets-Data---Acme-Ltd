import uuid
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from models.schemas import TimeSeriesDataPoint, TimeSeriesMeta, get_utc_now
from dal.repositories import TimeSeriesRepository, AssetRepository

class FinancialETLPipeline:
    
    def __init__(self):
        self.ts_repo = TimeSeriesRepository()
        self.asset_repo = AssetRepository()
        
    def reset_stats(self):
        self.stats = {"fetched": 0, "transformed": 0, "stored": 0, "skipped": 0, "errors": 0}

    # 1. EXTRACT STAGE
    def extract_market_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Reads raw data from Yahoo Finance."""
        print(f"[EXTRACT] Fetching 5 years of daily data for {symbol}...")
        try:
            ticker = yf.Ticker(symbol)
            # Fetch the last 5 years of historical data
            df = ticker.history(period="5y")
            
            if df.empty:
                self.stats["errors"] += 1
                return None
                
            self.stats["fetched"] += len(df)
            return df
        except Exception as e:
            print(f"[EXTRACT ERROR] {e}")
            self.stats["errors"] += 1
            return None

    # 2. TRANSFORM STAGE
    def transform_market_data(self, df: pd.DataFrame, symbol: str, ingest_id: str) -> List[dict]:
        print(f"[TRANSFORM] Mapping {symbol} data to canonical models...")
        records = []
        system_time = get_utc_now()
        
        clean_symbol = symbol.lower().replace("-", "_").replace("^", "")
        asset_type = "crypto" if "-" in symbol else ("index" if "^" in symbol else "stock")
        canonical_asset_id = f"{clean_symbol}_{asset_type}_01"
        
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
            
        for date_index, row in df.iterrows():
            try:
                business_dt = date_index.to_pydatetime() # type: ignore
                
                if business_dt.tzinfo is None:
                    business_dt = business_dt.replace(tzinfo=timezone.utc)
                else:
                    business_dt = business_dt.astimezone(timezone.utc)
                
                meta_data = TimeSeriesMeta(
                    asset_id=canonical_asset_id, 
                    data_source_id="yfinance"
                )
                
                record = TimeSeriesDataPoint(
                    business_date=business_dt,
                    system_date=system_time,
                    meta=meta_data,
                    indicators={
                        "open": float(row["Open"]),
                        "close": float(row["Close"]),
                        "volume": float(row["Volume"])
                    },
                    ingest_id=ingest_id,
                    quality_flags=0
                )
                data_dict = record.model_dump()
                
                data_dict["timestamp"] = data_dict["business_date"] 
                
                records.append(data_dict)
                self.stats["transformed"] += 1
                
            except Exception as e:
                self.stats["skipped"] += 1
                
        return records

    # 3. LOAD STAGES
    def load_asset(self, symbol: str) -> None:
        """Ensures the parent asset exists with dynamic asset class detection."""
        print(f"[LOAD] Updating Asset Catalog for {symbol}...")
        
        clean_symbol = symbol.lower().replace("-", "_").replace("^", "")
        asset_type = "crypto" if "-" in symbol else ("index" if "^" in symbol else "stock")
        canonical_asset_id = f"{clean_symbol}_{asset_type}_01"
        
        asset_dict = {
            "asset_id": canonical_asset_id,
            "symbol": symbol.upper(),
            "asset_class": asset_type,
            "description": f"Daily market data for {symbol.upper()}",
            "attributes": {
                "source": "yfinance",
                "currency": "USD"
            }
        }
        try:
            self.asset_repo.save(asset_dict)
        except Exception as e:
            print(f"[LOAD ERROR] Failed to save asset {symbol}: {e}")
            self.stats["errors"] += 1

    def load_time_series(self, records: List[dict]) -> None:
        """Stores the transformed data using the Data Access Layer."""
        if not records:
            return
            
        print(f"[LOAD] Writing {len(records)} records to the Data Access Layer...")
        try:
            inserted_count = self.ts_repo.save_batch(records)
            self.stats["stored"] += inserted_count
        except Exception as e:
            print(f"[LOAD ERROR] Failed to persist batch: {e}")
            self.stats["errors"] += 1

    def run_ingestion_job(self, symbol: str):
        """Executes the full pipeline for a specific asset."""
        self.reset_stats()
        ingest_id = f"job_etl_{uuid.uuid4().hex[:8]}"
        print(f"\n--- Starting ETL Job: {ingest_id} ---")
        
        df = self.extract_market_data(symbol)
        
        if df is not None and not df.empty:
            transformed_records = self.transform_market_data(df, symbol, ingest_id)
            self.load_asset(symbol) 
            self.load_time_series(transformed_records)
        else:
            print(f"[WARNING] No data fetched for {symbol}.")
            
        self.report_observability()

    def report_observability(self):
        """Outputs pipeline metrics to the console."""
        print("\n=== ETL Observability Report ===")
        print(f"Records Fetched:     {self.stats['fetched']}")
        print(f"Records Transformed: {self.stats['transformed']}")
        print(f"Records Stored:      {self.stats['stored']}")
        print(f"Records Skipped:     {self.stats['skipped']}")
        print(f"Pipeline Errors:     {self.stats['errors']}")
        print("================================\n")


if __name__ == "__main__":
    from database.db import connect_to_mongo, close_mongo_connection
    connect_to_mongo()
    
    pipeline = FinancialETLPipeline()
    
    portfolio = [
        # Technology
        "AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMZN", "META",
        # Finance & Banks
        "JPM", "BAC", "GS", "V",
        # Cryptocurrencies
        "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
        # Market Indices & ETFs
        "^GSPC",   # S&P 500 Index
        "^DJI",    # Dow Jones Industrial Average
        "QQQ",     # Nasdaq ETF
        # Commodities (via ETFs)
        "GLD",     # Gold
        "USO"      # Oil
    ]
    
    for asset in portfolio:
        pipeline.run_ingestion_job(asset)
    
    close_mongo_connection()