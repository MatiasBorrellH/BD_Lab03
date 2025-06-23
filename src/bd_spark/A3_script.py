# A.3 – Exploitation Zone Preparation
# Description:
# This script loads Parquet data from the formatted zone,
# derives KPI-ready fields, and writes a refined dataset to the exploitation zone.

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg, count, col

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

# --- Load Parquet data from formatted zone ---
df = spark.read.parquet(formatted_zone_path)

# --- Transform data for analysis (cleaned KPIs table) ---
df_kpi = df.select(
    col("district"),
    col("neighborhood"),
    col("price"),
    col("size"),
    col("rooms"),
    col("bathrooms"),
    col("hasLift"),
    col("calculatedPricePerArea")
)

# Optional: filter out extreme or invalid values
df_kpi = df_kpi.filter(
    (col("price") > 10000) & 
    (col("size") > 20) &
    (col("calculatedPricePerArea") > 100)
)

# --- Write to exploitation zone in Parquet format ---
df_kpi.write \
    .mode("overwrite") \
    .parquet(exploitation_zone_path)

# Optional: stop Spark session
spark.stop()