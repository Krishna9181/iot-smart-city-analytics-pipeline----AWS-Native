import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SILVER_DATABASE = "smart_city_silver"
SILVER_TABLE = "air_quality"
GOLD_S3_BASE = "s3://smart-city-datalake-2026/gold/"
PROCESSING_TIMESTAMP = datetime.utcnow().isoformat()

print(f"Starting Silver → Gold aggregation at {PROCESSING_TIMESTAMP}")

# ============================================================================
# STEP 1: Read Silver Data
# ============================================================================
print("Reading Silver air quality data...")

silver_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=SILVER_TABLE,
    transformation_ctx="silver_source"
)

silver_df = silver_dyf.toDF()
record_count = silver_df.count()
print(f"Read {record_count} Silver records")

if record_count == 0:
    print("No Silver data to aggregate. Exiting.")
    job.commit()
    sys.exit(0)

# ============================================================================
# GOLD TABLE 1: Hourly Averages by Zone
# ============================================================================
print("Creating Gold Table 1: Hourly Averages by Zone...")

hourly_avg_df = silver_df.groupBy(
    F.date_trunc("hour", F.col("reading_datetime")).alias("hour_timestamp"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    F.hour("reading_datetime").alias("hour"),
    "zone",
    "city",
    "country"
).agg(
    # Air Quality Metrics
    F.avg("pm25").alias("avg_pm25"),
    F.min("pm25").alias("min_pm25"),
    F.max("pm25").alias("max_pm25"),
    F.stddev("pm25").alias("stddev_pm25"),
    
    F.avg("pm10").alias("avg_pm10"),
    F.avg("co2").alias("avg_co2"),
    F.avg("no2").alias("avg_no2"),
    F.avg("o3").alias("avg_o3"),
    
    # Temperature & Weather
    F.avg("temperature_celsius").alias("avg_temperature"),
    F.avg("humidity_percent").alias("avg_humidity"),
    F.avg("pressure_hpa").alias("avg_pressure"),
    
    # AQI Metrics
    F.avg("aqi_value").alias("avg_aqi"),
    F.max("aqi_value").alias("max_aqi"),
    
    # Device Health
    F.avg("battery_level").alias("avg_battery_level"),
    F.countDistinct("sensor_id").alias("active_sensor_count"),
    F.count("*").alias("reading_count"),
    
    # Quality Flags
    F.sum(F.when(F.col("pm25") > 35.4, 1).otherwise(0)).alias("unhealthy_reading_count")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "aqi_status", 
    F.when(F.col("avg_aqi") <= 50, "Good")
     .when(F.col("avg_aqi") <= 100, "Moderate")
     .when(F.col("avg_aqi") <= 150, "Unhealthy for Sensitive Groups")
     .otherwise("Unhealthy")
)

# Write Gold Table 1
gold_hourly_path = f"{GOLD_S3_BASE}air_quality_hourly_avg/"
print(f"Writing to {gold_hourly_path}")

hourly_avg_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_hourly_path)

print(f"✓ Gold Table 1 created: {hourly_avg_df.count()} hourly records")

# ============================================================================
# GOLD TABLE 2: Daily Summary by Zone
# ============================================================================
print("Creating Gold Table 2: Daily Summary by Zone...")

daily_summary_df = silver_df.groupBy(
    F.to_date("reading_datetime").alias("date"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    "zone",
    "city"
).agg(
    # Daily Air Quality
    F.avg("pm25").alias("daily_avg_pm25"),
    F.min("pm25").alias("daily_min_pm25"),
    F.max("pm25").alias("daily_max_pm25"),
    
    F.avg("aqi_value").alias("daily_avg_aqi"),
    F.max("aqi_value").alias("daily_max_aqi"),
    
    # Temperature Range
    F.min("temperature_celsius").alias("daily_min_temp"),
    F.max("temperature_celsius").alias("daily_max_temp"),
    F.avg("temperature_celsius").alias("daily_avg_temp"),
    
    # Readings Count
    F.count("*").alias("total_readings"),
    F.countDistinct("sensor_id").alias("unique_sensors"),
    
    # Alert Counts
    F.sum(F.when(F.col("aqi_value") > 100, 1).otherwise(0)).alias("moderate_plus_hours"),
    F.sum(F.when(F.col("aqi_value") > 150, 1).otherwise(0)).alias("unhealthy_hours"),
    
    # Peak Pollution Hour
    F.max(F.struct("pm25", "reading_datetime")).alias("peak_pollution")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "peak_pollution_hour", F.hour(F.col("peak_pollution.reading_datetime"))
).withColumn(
    "air_quality_rating",
    F.when(F.col("daily_avg_aqi") <= 50, "Excellent")
     .when(F.col("daily_avg_aqi") <= 100, "Good")
     .when(F.col("daily_avg_aqi") <= 150, "Fair")
     .otherwise("Poor")
).drop("peak_pollution")

# Write Gold Table 2
gold_daily_path = f"{GOLD_S3_BASE}air_quality_daily_summary/"
print(f"Writing to {gold_daily_path}")

daily_summary_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_daily_path)

print(f"✓ Gold Table 2 created: {daily_summary_df.count()} daily records")

# ============================================================================
# GOLD TABLE 3: Sensor Health Metrics
# ============================================================================
print("Creating Gold Table 3: Sensor Health Metrics...")

sensor_health_df = silver_df.groupBy(
    F.to_date("reading_datetime").alias("date"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    "sensor_id",
    "zone"
).agg(
    # Device Status
    F.last("device_status").alias("latest_status"),
    F.last("firmware_version").alias("firmware_version"),
    
    # Battery Health
    F.avg("battery_level").alias("avg_battery_level"),
    F.min("battery_level").alias("min_battery_level"),
    F.last("battery_level").alias("latest_battery_level"),
    
    # Signal Quality
    F.avg("signal_strength_dbm").alias("avg_signal_strength"),
    
    # Data Quality
    F.count("*").alias("readings_count"),
    F.countDistinct(F.hour("reading_datetime")).alias("active_hours"),
    
    # Last Seen
    F.max("reading_datetime").alias("last_reading_time")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "health_status",
    F.when((F.col("latest_battery_level") < 20) | (F.col("latest_status") == "maintenance"), "Critical")
     .when((F.col("latest_battery_level") < 50) | (F.col("avg_signal_strength") < -70), "Warning")
     .otherwise("Healthy")
).withColumn(
    "uptime_percent",
    (F.col("active_hours") / 24.0 * 100).cast("decimal(5,2)")
)

# Write Gold Table 3
gold_sensor_health_path = f"{GOLD_S3_BASE}sensor_health_metrics/"
print(f"Writing to {gold_sensor_health_path}")

sensor_health_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_sensor_health_path)

print(f"✓ Gold Table 3 created: {sensor_health_df.count()} sensor-day records")

# ============================================================================
# GOLD TABLE 4: AQI Trend Analysis
# ============================================================================
print("Creating Gold Table 4: AQI Trend Analysis...")

aqi_trends_df = silver_df.withColumn(
    "date", F.to_date("reading_datetime")
).withColumn(
    "hour", F.hour("reading_datetime")
).groupBy("date", "hour", "zone").agg(
    F.countDistinct("aqi_category").alias("aqi_category_count"),
    F.collect_list("aqi_category").alias("aqi_categories"),
    F.avg("aqi_value").alias("avg_aqi"),
    F.count("*").alias("reading_count")
).withColumn(
    "year", F.year("date")
).withColumn(
    "month", F.month("date")
).withColumn(
    "day", F.dayofmonth("date")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
)

# Write Gold Table 4
gold_aqi_trends_path = f"{GOLD_S3_BASE}aqi_trends/"
print(f"Writing to {gold_aqi_trends_path}")

aqi_trends_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_aqi_trends_path)

print(f"✓ Gold Table 4 created: {aqi_trends_df.count()} hour-zone records")

# ============================================================================
# Complete Job
# ============================================================================
job.commit()
print("✓ All Gold tables created successfully!")
print(f"""
Gold Tables Summary:
1. Hourly Averages: {gold_hourly_path}
2. Daily Summary: {gold_daily_path}
3. Sensor Health: {gold_sensor_health_path}
4. AQI Trends: {gold_aqi_trends_path}
""")