"""Aggregation using Apache Spark which reads time-series records, groups by year, computes summary values, 
and persists results back to MongoDB using idempotent overwrites."""
import sys
import typing
sys.modules['typing.io'] = typing

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, count

def run_aggregation():
    print("Booting up Spark JVM...")
    
    spark = SparkSession.builder \
        .appName("Data Warehouses - Compute Total") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .getOrCreate()

    print("Spark Session created successfully. Reading data from MongoDB...")
    database_name = "acme_ltd_db" 
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "time_series") \
        .load()

    print("Crunching the numbers...")
    aggregated_df = df.withColumn("business_date_year", year(col("business_date"))) \
        .groupBy("meta.asset_id", "business_date_year") \
        .agg(count("*").alias("cnt"))

    print("Aggregation complete. Showing top 5 results:")
    aggregated_df.show(5)

    print("Saving results to MongoDB...")
    aggregated_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", "mongodb://127.0.0.1:27017/") \
        .option("database", database_name) \
        .option("collection", "spark_totals") \
        .save()
        
    print("Results successfully saved to the 'spark_totals' collection!")
    spark.stop()

if __name__ == "__main__":
    run_aggregation()