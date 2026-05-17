import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
BRONZE_S3_PATH = "s3://smart-city-datalake-2026/bronze/cdc/air_quality/"
SILVER_S3_PATH = "s3://smart-city-datalake-2026/silver/air_quality/"
PROCESSING_TIMESTAMP = datetime.utcnow().isoformat()

print(f"Starting Bronze → Silver processing at {PROCESSING_TIMESTAMP}")

# ============================================================================
# STEP 1: Read Bronze JSONL files directly (bypass Glue Catalog)
# ============================================================================
print("Reading Bronze JSONL files...")

# Read JSON Lines format directly
bronze_df = spark.read.json(BRONZE_S3_PATH)

record_count = bronze_df.count()
print(f"Read {record_count} CDC records from Bronze")

if record_count == 0:
    print("No new records to process. Exiting.")
    job.commit()
    sys.exit(0)

# Show sample schema to debug
print("Bronze schema:")
bronze_df.printSchema()

# ============================================================================
# STEP 2: Filter and Deduplicate CDC Events
# ============================================================================
print("Filtering and deduplicating CDC events...")

# Filter only INSERT and MODIFY events
active_records_df = bronze_df.filter(
    F.col("cdc_event_type").isin(["INSERT", "MODIFY"])
)

# Deduplicate by sensor_id + sensor_timestamp (keep latest CDC event)
window_spec = Window.partitionBy("sensor_id", "sensor_timestamp").orderBy(
    F.col("cdc_sequence_number").desc()
)

deduped_df = active_records_df.withColumn(
    "row_num", F.row_number().over(window_spec)
).filter(
    F.col("row_num") == 1
).drop("row_num")

print(f"After deduplication: {deduped_df.count()} unique records")

# ============================================================================
# STEP 3: Extract and Transform to Silver Schema
# ============================================================================
print("Transforming to Silver schema...")

silver_df = deduped_df.select(
    # Business Keys
    F.col("sensor_id"),
    F.col("sensor_timestamp").alias("reading_timestamp"),
    F.to_timestamp("sensor_timestamp").alias("reading_datetime"),
    F.col("sensor_type"),
    
    # Location (already flattened in your Lambda CDC processor)
    F.col("new_image.location.lat").cast("double").alias("latitude"),
    F.col("new_image.location.lon").cast("double").alias("longitude"),
    F.col("new_image.location.zone").alias("zone"),
    F.col("new_image.location.city").alias("city"),
    F.col("new_image.location.country").alias("country"),
    
    # Air Quality Measurements - Direct access (not nested in struct)
    F.col("new_image.measurements.pm25").cast("double").alias("pm25"),
    F.col("new_image.measurements.pm10").cast("double").alias("pm10"),
    F.col("new_image.measurements.co2").cast("double").alias("co2"),
    F.col("new_image.measurements.no2").cast("double").alias("no2"),
    F.col("new_image.measurements.o3").cast("double").alias("o3"),
    F.col("new_image.measurements.temperature").cast("double").alias("temperature_celsius"),
    F.col("new_image.measurements.humidity").cast("double").alias("humidity_percent"),
    F.col("new_image.measurements.pressure").cast("double").alias("pressure_hpa"),
    
    # AQI
    F.col("new_image.aqi.value").cast("int").alias("aqi_value"),
    F.col("new_image.aqi.category").alias("aqi_category"),
    
    # Device Metadata
    F.col("new_image.metadata.device_status").alias("device_status"),
    F.col("new_image.metadata.battery_level").cast("int").alias("battery_level"),
    F.col("new_image.metadata.signal_strength").cast("int").alias("signal_strength_dbm"),
    F.col("new_image.metadata.firmware_version").alias("firmware_version"),
    
    # Audit Fields
    F.col("cdc_event_id").alias("source_cdc_event_id"),
    F.col("cdc_timestamp").alias("cdc_processed_at"),
    F.lit(PROCESSING_TIMESTAMP).alias("silver_processed_at"),
    F.col("new_image.data_source").alias("data_source"),
    
    # Partition Fields (use sensor timestamp for partitioning, not CDC timestamp)
    F.year(F.to_timestamp("sensor_timestamp")).alias("year"),
    F.month(F.to_timestamp("sensor_timestamp")).alias("month"),
    F.dayofmonth(F.to_timestamp("sensor_timestamp")).alias("day")
)

# ============================================================================
# STEP 4: Data Quality Checks
# ============================================================================
print("Applying data quality checks...")

quality_df = silver_df.filter(
    # PM2.5 validation
    (F.col("pm25").isNotNull()) & 
    (F.col("pm25") >= 0) & 
    (F.col("pm25") < 500) &
    
    # Temperature validation
    (F.col("temperature_celsius").isNotNull()) &
    (F.col("temperature_celsius") >= -50) &
    (F.col("temperature_celsius") <= 60) &
    
    # Coordinates validation
    (F.col("latitude").isNotNull()) &
    (F.col("longitude").isNotNull()) &
    (F.col("latitude").between(-90, 90)) &
    (F.col("longitude").between(-180, 180)) &
    
    # Sensor ID validation
    (F.col("sensor_id").isNotNull())
)

rejected_count = silver_df.count() - quality_df.count()
if rejected_count > 0:
    print(f"WARNING: Rejected {rejected_count} records due to data quality issues")

print(f"Final Silver records: {quality_df.count()}")

# Show sample data
print("Sample Silver data:")
quality_df.show(5, truncate=False)

# ============================================================================
# STEP 5: Write to Silver Layer (Parquet, Partitioned, Compressed)
# ============================================================================
print(f"Writing to Silver S3: {SILVER_S3_PATH}")

quality_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(SILVER_S3_PATH)

print("✓ Successfully wrote to Silver layer")

# ============================================================================
# STEP 6: Commit Job (marks processed files if using Job Bookmark)
# ============================================================================
job.commit()
print("✓ Job completed successfully")