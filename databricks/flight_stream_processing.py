# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlightStreamProcessor") \
    .getOrCreate()

# COMMAND ----------

# Define schema for flight data
flight_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("heading", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# COMMAND ----------

# Read from Kafka (if using Databricks with Kafka)
# Note: This requires Databricks with Kafka integration
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your-kafka-broker:9092") \
    .option("subscribe", "flight-data-raw") \
    .option("startingOffsets", "latest") \
    .load()

# COMMAND ----------

# Alternative: Process data from Delta table (if using backend API)
def process_new_flights():
    """Process new flight data from raw table"""
    # Read latest raw data
    raw_df = spark.sql("""
        SELECT * FROM flight_data_raw 
        WHERE processed_at IS NULL
        ORDER BY timestamp DESC
        LIMIT 1000
    """)
    
    if raw_df.count() > 0:
        # Enrich and process data
        processed_df = raw_df \
            .withColumn("speed_kmh", col("velocity") * 3.6) \
            .withColumn("altitude_ft", col("altitude") * 3.28084) \
            .withColumn("is_climbing", col("vertical_rate") > 0) \
            .withColumn("is_descending", col("vertical_rate") < 0) \
            .withColumn("processed_at", current_timestamp())
        
        # Write to processed table
        processed_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("flight_data_processed")
        
        # Mark as processed in raw table
        raw_df.withColumn("processed_at", current_timestamp()) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("flight_data_raw")

# COMMAND ----------

# Create aggregation view for dashboard
spark.sql("""
CREATE OR REPLACE VIEW flight_dashboard_view AS
SELECT 
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    altitude,
    velocity,
    heading,
    vertical_rate,
    speed_kmh,
    altitude_ft,
    is_climbing,
    is_descending,
    timestamp
FROM flight_data
WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES
ORDER BY timestamp DESC
""")

# COMMAND ----------

# Create real-time aggregations
spark.sql("""
CREATE OR REPLACE VIEW flight_aggregations_view AS
SELECT 
    COUNT(DISTINCT icao24) as total_flights,
    COUNT(DISTINCT origin_country) as active_countries,
    AVG(speed_kmh) as avg_speed_kmh,
    AVG(altitude_ft) as avg_altitude_ft,
    SUM(CASE WHEN is_climbing THEN 1 ELSE 0 END) as climbing_flights,
    SUM(CASE WHEN is_descending THEN 1 ELSE 0 END) as descending_flights,
    CURRENT_TIMESTAMP() as aggregation_time
FROM flight_data
WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES
""")