# A.4 – Data Validation and Basic KPI Verification
# Description:
# This script runs basic validation queries and prints KPI metrics from
# the formatted and exploitation zones to verify data quality.

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg, count, col, expr, round as spark_round, when

# --- Spark context initialization ---
appName = "app"
master = "local[*]"

if 'sc' not in globals():
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

spark = SparkSession(sc)

# --- Paths ---
formatted_zone_path = "zones/formatted_zone/idealista"
exploitation_zone_path = "zones/exploitation_zone/idealista"

# --- Load datasets ---
df_formatted = spark.read.parquet(formatted_zone_path)
df_exploited = spark.read.parquet(exploitation_zone_path)

# --- Basic validations ---
print(" Row count (Formatted Zone):", df_formatted.count())
print(" Row count (Exploitation Zone):", df_exploited.count())

print("\n Null counts in Exploitation Zone:")
df_exploited.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_exploited.columns
]).show()

# --- KPI checks ---
print("\n Average price per m² by neighborhood:")
df_exploited.groupBy("neighborhood") \
    .agg(spark_round(avg("calculatedPricePerArea"), 2).alias("avg_price_per_m2")) \
    .orderBy("avg_price_per_m2", ascending=False) \
    .show(10, truncate=False)

print("\n Average property size by district:")
df_exploited.groupBy("district") \
    .agg(spark_round(avg("size"), 1).alias("avg_size_m2")) \
    .orderBy("avg_size_m2", ascending=False) \
    .show()

print("\n Proportion of properties with elevator by neighborhood:")
df_exploited.groupBy("neighborhood") \
    .agg(
        spark_round(avg(expr("CASE WHEN hasLift THEN 1 ELSE 0 END")), 2)
        .alias("elevator_ratio")
    ).orderBy("elevator_ratio", ascending=False) \
    .show(10, truncate=False)

print("\n Average number of rooms per district:")
df_exploited.groupBy("district") \
    .agg(spark_round(avg("rooms"), 2).alias("avg_rooms")) \
    .orderBy("avg_rooms", ascending=False) \
    .show()

# Optional: stop Spark session
spark.stop()