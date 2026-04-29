from database.db import connect_to_mongo, get_db, close_mongo_connection

def reset_time_series_collection():
    db = get_db()
    if db is None:
        print("Database connection failed.")
        return

    print("Dropping old 'time_series' collection...")
    db.drop_collection("time_series")

    print("Creating new Native Time-Series collection with 'business_date'...")
    # Explicitly creating the time-series collection with the new schema
    db.create_collection(
        "time_series",
        timeseries={
            "timeField": "business_date",
            "metaField": "meta",
            "granularity": "hours"
        }
    )
    
    # Re-creating our optimized index for the Data Access Layer
    print("Building compound index for fast retrieval...")
    db["time_series"].create_index([("meta.asset_id", 1), ("business_date", -1)])
    
    print("Database successfully upgraded to Bi-Temporal schema!")

if __name__ == "__main__":
    connect_to_mongo()
    reset_time_series_collection()
    close_mongo_connection()