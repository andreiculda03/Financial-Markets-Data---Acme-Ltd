"""
Use Case B: Production Machine Learning Pipeline (Global Market Model)
Trains a single, cross-asset Gradient Boosted Trees model on all available 
assets simultaneously using partitioned window functions.
"""
# --- PYTHON 3.13 CLEAN COMPATIBILITY PATCH ---
import sys
import typing
sys.modules['typing.io'] = typing
# ---------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, percent_rank
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def run_production_ml():
    print("Initializing Spark JVM for Global Market Machine Learning Pipeline...")
    
    spark = SparkSession.builder \
        .appName("Data Warehouses - Global GBT") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()

    print("Spark Session initialized. Loading market data from MongoDB...")
    database_name = "acme_ltd_db"
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "time_series") \
        .load()

    # 1. Feature Engineering (Processes all assets simultaneously)
    print("Engineering stationary features for all assets in parallel...")
    
    asset_df = df.select(
        col("meta.asset_id").alias("asset_id"),
        col("business_date").alias("bdate"),
        col("indicators.open").alias("target_open"),
        col("indicators.close").alias("close"),
        col("indicators.volume").alias("volume")
    )

    #Partitioning by asset_id guarantees timeline isolation across assets
    window_spec = Window.partitionBy("asset_id").orderBy("bdate")
    window_7d = Window.partitionBy("asset_id").orderBy("bdate").rowsBetween(-7, -1)

    engineered_df = asset_df \
        .withColumn("prev_open", lag("target_open", 1).over(window_spec)) \
        .withColumn("prev_close", lag("close", 1).over(window_spec)) \
        .withColumn("prev_volume", lag("volume", 1).over(window_spec)) \
        .withColumn("ma_7d_close", avg("close").over(window_7d)) \
        .dropna()

    engineered_df = engineered_df.withColumn("yesterday_trend", col("prev_close") - col("prev_open"))
    
    # STATIONARITY: Target is percentage return
    engineered_df = engineered_df.withColumn("target_return", (col("target_open") - col("prev_close")) / col("prev_close"))

    assembler = VectorAssembler(
        inputCols=["prev_close", "prev_volume", "ma_7d_close", "yesterday_trend"],
        outputCol="features"
    )
    model_df = assembler.transform(engineered_df)

    # 2. Chronological Proportional Train/Test Split (80/20) per Asset
    split_window = Window.partitionBy("asset_id").orderBy("bdate")
    model_df = model_df.withColumn("train_rank", percent_rank().over(split_window))
    
    train_data = model_df.filter(col("train_rank") <= 0.8).cache()
    test_data = model_df.filter(col("train_rank") > 0.8).cache()

    print(f"Training Global Model on {train_data.count()} historical observations across all assets...")
    print("Training Gradient Boosted Trees regression model on stationary returns...")

    # 3. Train the Global Champion Model (Gradient Boosted Trees)
    print("Training Gradient Boosted Trees on Stationary Returns...")
    gbt = GBTRegressor(featuresCol="features", labelCol="target_return", maxIter=20, maxDepth=5, seed=42)
    gbt_model = gbt.fit(train_data)

    # 4. Generate Predictions & Evaluate
    predictions = gbt_model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(labelCol="target_return", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="target_return", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="target_return", predictionCol="prediction", metricName="r2")

    print("\n" + "="*50)
    print("GLOBAL MARKET MODEL PERFORMANCE METRICS (GBT)")
    print("="*50)
    print(f"R² Score:                    {evaluator_r2.evaluate(predictions):.4f}")
    print(f"Root Mean Squared Error:     {evaluator_rmse.evaluate(predictions):.4f}")
    print(f"Mean Absolute Error:         {evaluator_mae.evaluate(predictions):.4f}")
    print("="*50 + "\n")

    # Convert the predicted global percentage return back into absolute dollar prices!
    predictions = predictions.withColumn("predicted_price_usd", col("prev_close") * (1 + col("prediction")))
    
    from pyspark.sql.functions import date_add
    predictions = predictions.withColumn("target_prediction_date", date_add(col("bdate"), 1))
    
    # Select our shifted future target date instead of the current historical bdate
    results_df = predictions.select(
        col("asset_id"), 
        col("target_prediction_date").alias("bdate"), # Kept alias as bdate for database consistency
        col("target_open"), 
        col("predicted_price_usd"),
        col("prediction") # Ensure the raw percentage prediction is included for the MCP server
    )
    
    print("Sample predictions (shifted to represent the forward-looking target date):")
    results_df.orderBy("bdate", ascending=False).show(10)

    # 5. Write all cross-asset results back to MongoDB simultaneously
    print("Writing cross-asset regression predictions to MongoDB...")
    results_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "spark_regression_results") \
        .save()

    print("Global machine learning workflow completed. Production predictions saved successfully.")
    spark.stop()

if __name__ == "__main__":
    run_production_ml()
    
    #prophet mcp