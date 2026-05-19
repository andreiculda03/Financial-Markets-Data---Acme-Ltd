"""
Use Case B: Advanced Machine Learning Prediction
Engineers quantitative time-series features, performs Hyperparameter Tuning via 
Cross-Validation, evaluates multiple metrics, and writes to MongoDB.
"""
# --- PYTHON 3.13 CLEAN COMPATIBILITY PATCH ---
import sys
import typing
sys.modules['typing.io'] = typing
# ---------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

def run_ml_prediction():
    print("Booting up Spark JVM for Advanced Machine Learning...")
    
    spark = SparkSession.builder \
        .appName("Data Warehouses - Advanced ML") \
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

    # 1. Extract specific asset
    asset_id = "btc_usd_crypto_01"
    
    asset_df = df.filter(col("meta.asset_id") == asset_id) \
        .select(
            col("business_date").alias("bdate"),
            col("indicators.open").alias("target_open"),
            col("indicators.close").alias("close"),
            col("indicators.volume").alias("volume")
        )

    # 2. Master's Level Feature Engineering
    print("Engineering Quantitative Features (Lags, MA7, Momentum)...")
    window_spec = Window.orderBy("bdate")
    window_7d = Window.orderBy("bdate").rowsBetween(-7, -1)

    engineered_df = asset_df \
        .withColumn("prev_open", lag("target_open", 1).over(window_spec)) \
        .withColumn("prev_close", lag("close", 1).over(window_spec)) \
        .withColumn("prev_volume", lag("volume", 1).over(window_spec)) \
        .withColumn("ma_7d_close", avg("close").over(window_7d)) \
        .dropna()

    engineered_df = engineered_df.withColumn("yesterday_trend", col("prev_close") - col("prev_open"))

    assembler = VectorAssembler(
        inputCols=["prev_close", "prev_volume", "ma_7d_close", "yesterday_trend"],
        outputCol="features"
    )
    model_df = assembler.transform(engineered_df)

    # 3. Strict Chronological Time-Series Split (80/20 Holdout)
    print("Performing strict chronological train/test split...")
    split_window = Window.orderBy("bdate")
    model_df = model_df.withColumn("row_num", row_number().over(split_window))
    
    total_rows = model_df.count()
    train_size = int(total_rows * 0.8)
    
    train_data = model_df.filter(col("row_num") <= train_size)
    test_data = model_df.filter(col("row_num") > train_size)

    print(f"Training on {train_data.count()} days, Testing on {test_data.count()} days.")

    # 4. Hyperparameter Tuning & Regularization (Preventing Overfit)
    print("\nStarting Cross-Validation (Testing multiple Random Forest configurations)...")
    print("This may take a few minutes as it trains multiple models!")
    
    rf = RandomForestRegressor(featuresCol="features", labelCol="target_open", seed=42)

    # FIX: We restrict the maxDepth (shallower trees) and introduce minInstancesPerNode 
    # to stop the model from memorizing single specific days.
    paramGrid = (ParamGridBuilder()
                 .addGrid(rf.numTrees, [50, 100])
                 .addGrid(rf.maxDepth, [3, 5])                # Lowered from [5, 7]
                 .addGrid(rf.minInstancesPerNode, [2, 5])     # NEW: Regularization parameter
                 .build())

    evaluator_rmse = RegressionEvaluator(labelCol="target_open", predictionCol="prediction", metricName="rmse")

    # 3-Fold Cross Validator
    cv = CrossValidator(estimator=rf,
                        estimatorParamMaps=paramGrid,
                        evaluator=evaluator_rmse,
                        numFolds=3) 

    cv_model = cv.fit(train_data)
    best_rf = cv_model.bestModel
    
    print(f"Best Model Found: Trees={best_rf.getNumTrees}, Depth={best_rf.getOrDefault('maxDepth')}")

    # 5. Generate Predictions on the Chronological Holdout Set
    predictions = best_rf.transform(test_data)
    
    # 6. Evaluate ALL Metrics
    evaluator_mae = RegressionEvaluator(labelCol="target_open", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="target_open", predictionCol="prediction", metricName="r2")

    final_rmse = evaluator_rmse.evaluate(predictions)
    final_mae = evaluator_mae.evaluate(predictions)
    final_r2 = evaluator_r2.evaluate(predictions)

# Evaluate Training Data to check for Overfitting
    train_predictions = best_rf.transform(train_data)
    train_r2 = evaluator_r2.evaluate(train_predictions)
    
    print(f"--- OVERFIT CHECK ---")
    print(f"Training R2 : {train_r2:.4f}")
    print(f"Testing R2  : {final_r2:.4f}")
    print(f"---------------------")

    print(f"\n======================================")
    print(f" FINAL MODEL EVALUATION METRICS:")
    print(f" RMSE (Root Mean Squared Error) : ${final_rmse:.2f}")
    print(f" MAE  (Mean Absolute Error)     : ${final_mae:.2f}")
    print(f" R2   (Variance Explained)      : {final_r2:.4f}")
    print(f"======================================\n")

    results_df = predictions.select("bdate", "target_open", "prediction")
    print("Sample Predictions (Actual Open vs. Predicted Open):")
    results_df.show(5)

    # 7. Write results back to MongoDB
    print("Saving ML results to MongoDB...")
    results_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "spark_regression_results") \
        .save()

    print("✅ ML workflow complete! Predictions saved to 'spark_regression_results'.")
    spark.stop()

if __name__ == "__main__":
    run_ml_prediction()