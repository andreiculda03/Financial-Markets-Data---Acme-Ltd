import os
from typing import Optional, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME = os.getenv("DATABASE_NAME", "acme_ltd_db")

class Database:
    client: Optional[MongoClient] = None
    db: Optional[Any] = None

db_instance = Database()

def connect_to_mongo():
    """Establishes a connection and configures advanced collection structures."""
    try:
        db_instance.client = MongoClient(MONGO_URL)
        db_instance.client.admin.command('ping')
        
        if db_instance.client is not None:
            db_instance.db = db_instance.client[DB_NAME]
            
            existing_collections = db_instance.db.list_collection_names()
            if "time_series" not in existing_collections:
                print("Initializing native MongoDB Time-Series collection...")
                db_instance.db.create_collection(
                    "time_series",
                    timeseries={
                        "timeField": "business_date",
                        "metaField": "meta",
                        "granularity": "minutes" 
                    }
                )
                
                print("Building compound indexes for analytics layer...")
                db_instance.db["time_series"].create_index(
                    [("meta.asset_id", 1), ("business_date", -1)]
                )
                # Index for Asset Collection (SCD Type 2)
                db_instance.db["assets"].create_index([("asset_id", 1)])
            
        print(f"Successfully connected to MongoDB! Database: {DB_NAME}")
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")

def close_mongo_connection():
    if db_instance.client:
        db_instance.client.close()
        print("Closed MongoDB connection.")

def get_db():
    return db_instance.db