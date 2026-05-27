"""ML Model Testing-Transforms non-stationary price data into stationary percentage returns,
solving the Tree-based extrapolation problem. Evaluates multiple algorithms."""
import sys
import typing
sys.modules['typing.io'] = typing
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import time

def run_model_test():
    print("Initializing Spark Session for Quantitative Model Evaluation...")
    
    spark = SparkSession.builder \
        .appName("Data Warehouses - Machine Learning Model Testing") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()

    print("Spark Session created. Loading data...")
    database_name = "acme_ltd_db"
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "time_series") \
        .load()

    # 1. Feature Engineering
    asset_id = "btc_usd_crypto_01"
    print(f"Engineering Stationary Features for {asset_id.upper()}...")
    
    asset_df = df.filter(col("meta.asset_id") == asset_id) \
        .select(
            col("business_date").alias("bdate"),
            col("indicators.open").alias("target_open"),
            col("indicators.close").alias("close"),
            col("indicators.volume").alias("volume")
        )

    window_spec = Window.orderBy("bdate")
    window_7d = Window.orderBy("bdate").rowsBetween(-7, -1)

    # Standard Lags
    engineered_df = asset_df \
        .withColumn("prev_open", lag("target_open", 1).over(window_spec)) \
        .withColumn("prev_close", lag("close", 1).over(window_spec)) \
        .withColumn("prev_volume", lag("volume", 1).over(window_spec)) \
        .withColumn("ma_7d_close", avg("close").over(window_7d)) \
        .dropna()

    # 1. Momentum feature
    engineered_df = engineered_df.withColumn("yesterday_trend", col("prev_close") - col("prev_open"))
    
    # 2. STATIONARITY: Calculate the percentage return from yesterday's close to today's open
    engineered_df = engineered_df.withColumn("target_return", (col("target_open") - col("prev_close")) / col("prev_close"))

    assembler = VectorAssembler(
        inputCols=["prev_close", "prev_volume", "ma_7d_close", "yesterday_trend"],
        outputCol="features"
    )
    model_df = assembler.transform(engineered_df)

    # 2. Strict Chronological Split
    split_window = Window.orderBy("bdate")
    model_df = model_df.withColumn("row_num", row_number().over(split_window))
    
    total_rows = model_df.count()
    train_size = int(total_rows * 0.8)
    
    train_data = model_df.filter(col("row_num") <= train_size).cache()
    test_data = model_df.filter(col("row_num") > train_size).cache()

    print(f"Training on {train_data.count()} days, Testing on {test_data.count()} days.")
    print("-" * 50)

    models = {
        "Linear Regression": LinearRegression(featuresCol="features", labelCol="target_return", maxIter=10),
        "Random Forest": RandomForestRegressor(featuresCol="features", labelCol="target_return", numTrees=50, maxDepth=5, seed=42),
        "Gradient Boosted Trees": GBTRegressor(featuresCol="features", labelCol="target_return", maxIter=20, maxDepth=5, seed=42)
    }

    evaluator_rmse = RegressionEvaluator(labelCol="target_return", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="target_return", predictionCol="prediction", metricName="r2")

    leaderboard = []

    for model_name, algorithm in models.items():
        print(f"Training {model_name} on percentage returns...")
        start_time = time.time()
        model = algorithm.fit(train_data)
        predictions = model.transform(test_data)

        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)
        duration = time.time() - start_time
        
        leaderboard.append({
            "Model": model_name,
            "RMSE": rmse,
            "R2": r2,
            "Time (s)": round(duration, 2)
        })
        print(f"Completed {model_name} in {duration:.1f} seconds.")

    print("\n" + "="*65)
    print(" MACHINE LEARNING MODEL EVALUATION RESULTS ")
    print("="*65)
    print(" Note: RMSE represents prediction error as decimal percentage (e.g., 0.02 = 2.0%)")
    print("-" * 65)
    
    # Sort by RMSE (lowest error is best)
    leaderboard.sort(key=lambda x: x["RMSE"], reverse=False)
    
    print(f"{'Rank':<5} | {'Model Name':<25} | {'R² Score':<10} | {'RMSE (Error)'}")
    print("-" * 65)
    
    for i, result in enumerate(leaderboard):
        print(f"#{i+1:<4} | {result['Model']:<25} | {result['R2']:<10.4f} | {result['RMSE']:.4f}")
    
    print("="*65 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    run_model_test()