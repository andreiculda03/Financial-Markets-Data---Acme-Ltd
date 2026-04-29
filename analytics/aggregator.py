from database.db import get_db, connect_to_mongo, close_mongo_connection

def run_monthly_rollups():
    """
    Runs a MongoDB Aggregation Pipeline to compress daily tick data
    into monthly analytical summaries (Min, Max, Avg, Count).
    This fulfills Use Case 3 of the project requirements.
    """
    db = get_db()
    if db is None:
        print("Error: Database connection failed.")
        return

    print("Starting Big Data Aggregation Pipeline...")
    
    pipeline = [
        {
            "$addFields": {
                "year_month": {
                    "$dateToString": {"format": "%Y-%m", "date": "$timestamp"}
                }
            }
        },
        
        {
            "$group": {
                "_id": {
                    "asset_id": "$meta.asset_id",
                    "month": "$year_month"
                },
                "data_points_counted": {"$sum": 1},
                
                # We use $ifNull to handle heterogeneous fields (price_usd vs close vs yield_percent)
                "max_value": {
                    "$max": {
                        "$ifNull": ["$indicators.close", {"$ifNull": ["$indicators.price_usd", "$indicators.yield_percent"]}]
                    }
                },
                "min_value": {
                    "$min": {
                        "$ifNull": ["$indicators.close", {"$ifNull": ["$indicators.price_usd", "$indicators.yield_percent"]}]
                    }
                },
                "avg_value": {
                    "$avg": {
                        "$ifNull": ["$indicators.close", {"$ifNull": ["$indicators.price_usd", "$indicators.yield_percent"]}]
                    }
                }
            }
        },
        
        {
            "$project": {
                "_id": 0,
                "asset_id": "$_id.asset_id",
                "interval_type": "1_month",
                "period": "$_id.month",
                "stats": {
                    "count": "$data_points_counted",
                    "high": {"$round": ["$max_value", 4]},
                    "low": {"$round": ["$min_value", 4]},
                    "average": {"$round": ["$avg_value", 4]}
                }
            }
        },
        
        {
            "$merge": {
                "into": "aggregates",
                "on": ["asset_id", "period", "interval_type"],
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    try:
        db["time_series"].aggregate(pipeline)
        
        rollup_count = db["aggregates"].count_documents({})
        print(f"Aggregation Complete. Generated {rollup_count} monthly rollups.")
        
    except Exception as e:
        print(f"Aggregation Pipeline failed: {e}")

if __name__ == "__main__":
    connect_to_mongo()
    
    db = get_db()
    if db is not None:
        # Ensure the aggregates collection has the unique index required by $merge
        db["aggregates"].create_index(
            [("asset_id", 1), ("period", 1), ("interval_type", 1)], 
            unique=True
        )
        run_monthly_rollups()
    else:
        print("Error: Could not retrieve database instance to create indexes.")
        
    close_mongo_connection()