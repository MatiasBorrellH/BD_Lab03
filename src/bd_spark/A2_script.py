# A.2 – Data Formatting Process
# Description:
# Load Idealista JSON listings from a local landing zone,
# transform the data using PySpark, and store it in a formatted zone (Parquet),
# partitioned by district for efficiency.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# --- Spark context initialization ---
appName = "app"
master = "local[*]"  # Use all local cores

if 'sc' not in globals():
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

# Create SparkSession from existing SparkContext
spark = SparkSession(sc)

# --- Paths (adjust if needed) ---
landing_zone_path = "zones/landing_zone/idealista"        # Raw JSON input
formatted_zone_path = "zones/formatted_zone/idealista"    # Output directory

# --- Load JSON data from Landing Zone ---
df_raw = spark.read.json("zones/landing_zone/idealista")

# Print schema (optional debug)
df_raw.printSchema()

# --- Select and transform fields of interest ---
df_clean = df_raw.select(
    col("propertyCode"),
    col("price").cast("double"),
    col("size").cast("double"),
    col("rooms").cast("integer"),
    col("bathrooms").cast("integer"),
    col("neighborhood"),
    col("district"),
    col("municipality"),
    col("priceByArea").cast("double"),
    col("hasLift").cast("boolean"),
    col("has3DTour").cast("boolean")
).dropna(subset=["price", "size", "district"])

# Add fallback calculation for price per area
df_clean = df_clean.withColumn(
    "calculatedPricePerArea",
    when(col("priceByArea").isNull(), col("price") / col("size"))
    .otherwise(col("priceByArea"))
)

# --- Write cleaned data to Formatted Zone ---
df_clean.write \
    .partitionBy("district") \
    .mode("overwrite") \
    .parquet(formatted_zone_path)

# Optional: stop Spark session manually if running standalone
spark.stop()