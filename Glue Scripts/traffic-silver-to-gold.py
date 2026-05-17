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
SILVER_TABLE = "traffic_year_2026"
GOLD_S3_BASE = "s3://smart-city-datalake-2026/gold/"
PROCESSING_TIMESTAMP = datetime.utcnow().isoformat()

print("=" * 80)
print("Traffic Silver → Gold Processing")
print("=" * 80)
print(f"Processing Timestamp: {PROCESSING_TIMESTAMP}")

# ============================================================================
# STEP 1: Read Silver Traffic Data
# ============================================================================
print("\n[1/5] Reading Silver traffic data...")

silver_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=SILVER_TABLE,
    transformation_ctx="silver_source"
)

silver_df = silver_dyf.toDF()
record_count = silver_df.count()
print(f"✓ Read {record_count:,} Silver traffic records")

if record_count == 0:
    print("No Silver data to process. Exiting.")
    job.commit()
    sys.exit(0)

# Show data range
print("\nData Range:")
silver_df.select(
    F.min("reading_datetime").alias("earliest"),
    F.max("reading_datetime").alias("latest"),
    F.countDistinct("sensor_id").alias("sensors"),
    F.countDistinct("intersection").alias("intersections")
).show(truncate=False)

# ============================================================================
# GOLD TABLE 1: Hourly Metrics by Intersection
# ============================================================================
print("\n[2/5] Creating Gold Table 1: Hourly Traffic by Intersection...")

hourly_intersection_df = silver_df.groupBy(
    F.date_trunc("hour", F.col("reading_datetime")).alias("hour_timestamp"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    F.hour("reading_datetime").alias("hour"),
    "intersection",
    "road_type",
    "city"
).agg(
    # Vehicle Metrics
    F.avg("vehicle_count").alias("avg_vehicle_count"),
    F.min("vehicle_count").alias("min_vehicle_count"),
    F.max("vehicle_count").alias("max_vehicle_count"),
    F.stddev("vehicle_count").alias("stddev_vehicle_count"),
    
    # Speed Metrics
    F.avg("avg_speed_mph").alias("avg_speed_mph"),
    F.min("avg_speed_mph").alias("min_speed_mph"),
    F.max("avg_speed_mph").alias("max_speed_mph"),
    
    # Congestion Analysis
    F.avg("occupancy_percent").alias("avg_occupancy_percent"),
    F.avg("queue_length_meters").alias("avg_queue_length_meters"),
    
    # Count by Congestion Level
    F.sum(F.when(F.col("congestion_level") == "severe", 1).otherwise(0)).alias("severe_congestion_count"),
    F.sum(F.when(F.col("congestion_level") == "high", 1).otherwise(0)).alias("high_congestion_count"),
    F.sum(F.when(F.col("congestion_level") == "moderate", 1).otherwise(0)).alias("moderate_congestion_count"),
    F.sum(F.when(F.col("congestion_level") == "low", 1).otherwise(0)).alias("low_congestion_count"),
    
    # Traffic Flow
    F.avg("traffic_flow_score").alias("avg_traffic_flow_score"),
    
    # Incidents
    F.sum(F.when(F.col("incident_detected") == True, 1).otherwise(0)).alias("incidents_detected"),
    
    # Pedestrian & Bike
    F.sum("pedestrian_count").alias("total_pedestrians"),
    F.sum("bike_count").alias("total_bikes"),
    
    # Readings Count
    F.count("*").alias("reading_count"),
    F.countDistinct("sensor_id").alias("active_sensors")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "dominant_congestion_level",
    F.when(F.col("severe_congestion_count") > 0, "severe")
     .when(F.col("high_congestion_count") > F.col("moderate_congestion_count"), "high")
     .when(F.col("moderate_congestion_count") > 0, "moderate")
     .otherwise("low")
).withColumn(
    "congestion_score",
    # 0-100 score: lower is better
    F.round(
        (F.col("severe_congestion_count") * 100 + 
         F.col("high_congestion_count") * 70 + 
         F.col("moderate_congestion_count") * 40 + 
         F.col("low_congestion_count") * 10) / F.col("reading_count"),
        2
    )
)

# Write Gold Table 1
gold_hourly_path = f"{GOLD_S3_BASE}traffic_hourly_by_intersection/"
print(f"Writing to: {gold_hourly_path}")

hourly_intersection_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_hourly_path)

print(f"✓ Created {hourly_intersection_df.count():,} hourly intersection records")

# ============================================================================
# GOLD TABLE 2: Daily Traffic Summary by Road Type
# ============================================================================
print("\n[3/5] Creating Gold Table 2: Daily Summary by Road Type...")

daily_summary_df = silver_df.groupBy(
    F.to_date("reading_datetime").alias("date"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    "road_type",
    "city"
).agg(
    # Daily Vehicle Metrics
    F.avg("vehicle_count").alias("daily_avg_vehicles"),
    F.max("vehicle_count").alias("daily_peak_vehicles"),
    F.min("vehicle_count").alias("daily_min_vehicles"),
    
    # Daily Speed Metrics
    F.avg("avg_speed_mph").alias("daily_avg_speed"),
    F.min("avg_speed_mph").alias("daily_min_speed"),
    F.max("avg_speed_mph").alias("daily_max_speed"),
    
    # Traffic Flow Score
    F.avg("traffic_flow_score").alias("daily_avg_flow_score"),
    
    # Congestion Hours (readings where congestion is high/severe)
    F.sum(F.when(F.col("congestion_level").isin(["high", "severe"]), 1).otherwise(0)).alias("congested_readings"),
    
    # Incidents
    F.sum(F.when(F.col("incident_detected") == True, 1).otherwise(0)).alias("total_incidents"),
    
    # Pedestrian & Bike Activity
    F.sum("pedestrian_count").alias("daily_pedestrians"),
    F.sum("bike_count").alias("daily_bikes"),
    
    # Coverage Metrics
    F.count("*").alias("total_readings"),
    F.countDistinct("sensor_id").alias("unique_sensors"),
    F.countDistinct("intersection").alias("unique_intersections"),
    F.countDistinct(F.hour("reading_datetime")).alias("active_hours")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "congestion_percentage",
    F.round((F.col("congested_readings") / F.col("total_readings") * 100), 2)
).withColumn(
    "traffic_quality_rating",
    F.when(F.col("daily_avg_flow_score") >= 85, "Excellent")
     .when(F.col("daily_avg_flow_score") >= 70, "Good")
     .when(F.col("daily_avg_flow_score") >= 50, "Fair")
     .otherwise("Poor")
).withColumn(
    "coverage_score",
    F.round((F.col("active_hours") / 24.0 * 100), 2)
)

# Write Gold Table 2
gold_daily_path = f"{GOLD_S3_BASE}traffic_daily_summary/"
print(f"Writing to: {gold_daily_path}")

daily_summary_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_daily_path)

print(f"✓ Created {daily_summary_df.count():,} daily summary records")

# ============================================================================
# GOLD TABLE 3: Traffic Incidents Analysis
# ============================================================================
print("\n[4/5] Creating Gold Table 3: Traffic Incidents...")

# Filter for incidents only
incidents_df = silver_df.filter(
    F.col("incident_detected") == True
).select(
    F.col("reading_datetime").alias("incident_datetime"),
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    F.hour("reading_datetime").alias("hour"),
    "sensor_id",
    "intersection",
    "road_type",
    "city",
    "vehicle_count",
    "avg_speed_mph",
    "congestion_level",
    "queue_length_meters",
    "traffic_flow_score"
).withColumn(
    "incident_severity",
    F.when((F.col("avg_speed_mph") < 5) & (F.col("vehicle_count") > 60), "Critical")
     .when((F.col("avg_speed_mph") < 10) & (F.col("vehicle_count") > 50), "High")
     .otherwise("Moderate")
).withColumn(
    "processed_at", F.lit(PROCESSING_TIMESTAMP)
)

# Write Gold Table 3
gold_incidents_path = f"{GOLD_S3_BASE}traffic_incidents/"
print(f"Writing to: {gold_incidents_path}")

if incidents_df.count() > 0:
    incidents_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(gold_incidents_path)
    
    print(f"✓ Created {incidents_df.count():,} incident records")
else:
    print("⚠ No incidents detected in this batch")

# ============================================================================
# GOLD TABLE 4: Congestion Trends (15-minute intervals)
# ============================================================================
print("\n[5/5] Creating Gold Table 4: Congestion Trends...")

# Create 15-minute time windows using F.window()
congestion_trends_df = silver_df.withColumn(
    "time_window", 
    F.window(F.col("reading_datetime"), "15 minutes") # Fixed: Changed Window() to F.window()
).withColumn(
    "time_window_start", 
    F.col("time_window.start")
).drop("time_window").groupBy(
    "time_window_start",
    F.year("reading_datetime").alias("year"),
    F.month("reading_datetime").alias("month"),
    F.dayofmonth("reading_datetime").alias("day"),
    F.hour("reading_datetime").alias("hour"),
    "city",
    "road_type"
).agg(
    # Congestion Metrics
    F.avg("vehicle_count").alias("avg_vehicles"),
    F.avg("avg_speed_mph").alias("avg_speed"),
    F.avg("occupancy_percent").alias("avg_occupancy"),
    
    # Congestion Level Distribution
    F.count(F.when(F.col("congestion_level") == "severe", 1)).alias("severe_count"),
    F.count(F.when(F.col("congestion_level") == "high", 1)).alias("high_count"),
    F.count(F.when(F.col("congestion_level") == "moderate", 1)).alias("moderate_count"),
    F.count(F.when(F.col("congestion_level") == "low", 1)).alias("low_count"),
    
    # Flow Score
    F.avg("traffic_flow_score").alias("avg_flow_score"),
    
    # Reading Count
    F.count("*").alias("reading_count")
).withColumn(
    "time_window_end", 
    F.expr("time_window_start + INTERVAL 15 MINUTES")
).withColumn(
    "processed_at", 
    F.lit(PROCESSING_TIMESTAMP)
).withColumn(
    "congestion_trend",
    F.when(F.col("avg_flow_score") < 40, "Worsening")
     .when(F.col("avg_flow_score") > 70, "Improving")
     .otherwise("Stable")
)

# Write Gold Table 4
gold_trends_path = f"{GOLD_S3_BASE}traffic_congestion_trends/"
print(f"Writing to: {gold_trends_path}")

congestion_trends_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(gold_trends_path)

print(f"✓ Created {congestion_trends_df.count():,} congestion trend records")

# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 80)
print("GOLD PROCESSING COMPLETE")
print("=" * 80)
print(f"""
Gold Tables Created:
1. traffic_hourly_by_intersection: {gold_hourly_path}
2. traffic_daily_summary: {gold_daily_path}
3. traffic_incidents: {gold_incidents_path}
4. traffic_congestion_trends: {gold_trends_path}

Processing Timestamp: {PROCESSING_TIMESTAMP}
""")

job.commit()
print("✓ Job completed successfully")