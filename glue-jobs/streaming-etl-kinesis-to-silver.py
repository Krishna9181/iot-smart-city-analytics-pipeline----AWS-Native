import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'output_bucket'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sparkContext.setLogLevel("WARN")

def get_sensor_schema():
    """
    Define explicit schema for streaming sensor data
    """
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("location", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("zone", StringType(), True)
        ]), True),
        StructField("measurements", StructType([
            # Air quality measurements
            StructField("pm25", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("co2", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            # Traffic measurements
            StructField("vehicle_count", IntegerType(), True),
            StructField("avg_speed", DoubleType(), True)
        ]), True)
    ])

def main():
    """
    24/7 Streaming job with fixed aggregations
    """
    
    print("=== Starting 24/7 Smart City Streaming (Fixed Aggregations) ===")
    
    source_bucket = args.get('source_bucket', 'smart-city-datalake-2026')
    output_bucket = args.get('output_bucket', 'smart-city-datalake-2026')
    
    # Bronze layer path where Firehose delivers data
    bronze_path = f"s3://{source_bucket}/kinesis-raw/*/*/*/*/"
    
    print(f"Monitoring Bronze layer: {bronze_path}")
    
    # Create streaming DataFrame WITH EXPLICIT SCHEMA
    streaming_df = spark \
        .readStream \
        .format("json") \
        .schema(get_sensor_schema()) \
        .option("path", bronze_path) \
        .option("maxFilesPerTrigger", 10) \
        .option("latestFirst", "true") \
        .load()
    
    print("Streaming DataFrame created with explicit schema")
    
    # Parse and validate incoming sensor data
    parsed_df = parse_sensor_data(streaming_df)
    
    # Start streaming queries with fixed aggregations
    queries = []
    
    # 1. Air Quality Analytics Stream
    air_quality_query = start_air_quality_stream(parsed_df, output_bucket)
    queries.append(air_quality_query)
    
    # 2. Traffic Analytics Stream
    traffic_query = start_traffic_stream(parsed_df, output_bucket)
    queries.append(traffic_query)
    
    # 3. Cross-Correlation Stream
    correlation_query = start_correlation_stream(parsed_df, output_bucket)
    queries.append(correlation_query)
    
    print(f"Started {len(queries)} streaming transformations")
    print("All streams running 24/7 - processing data continuously...")
    
    # Keep job running forever (24/7 streaming)
    spark.streams.awaitAnyTermination()

def parse_sensor_data(streaming_df):
    """
    Parse and validate sensor data with explicit schema
    """
    
    print("Parsing sensor data with schema validation...")
    
    parsed_df = streaming_df \
        .select(
            col("sensor_id"),
            to_timestamp(col("timestamp")).alias("event_time"),
            col("sensor_type"),
            col("location"),
            col("measurements")
        ) \
        .filter(
            col("sensor_id").isNotNull() & 
            col("sensor_type").isNotNull() &
            col("event_time").isNotNull() &
            col("location").isNotNull() &
            col("measurements").isNotNull()
        )
    
    return parsed_df

def start_air_quality_stream(parsed_df, output_bucket):
    """
    24/7 Air Quality Analytics Stream - FIXED AGGREGATIONS
    """
    
    print("Starting Air Quality Analytics Stream...")
    
    # Filter air quality sensors
    air_quality_df = parsed_df \
        .filter(col("sensor_type") == "air_quality") \
        .select(
            col("sensor_id"),
            col("event_time"),
            col("location.zone").alias("zone"),
            col("location.lat").alias("latitude"),
            col("location.lon").alias("longitude"),
            col("measurements.pm25").alias("pm25"),
            col("measurements.pm10").alias("pm10"),
            col("measurements.co2").alias("co2"),
            col("measurements.temperature").alias("temperature"),
            col("measurements.humidity").alias("humidity")
        ) \
        .filter(col("pm25").isNotNull())
    
    # 5-minute windowed aggregations - FIXED: Using approx_count_distinct
    air_quality_analytics = air_quality_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("zone")
        ) \
        .agg(
            # PM2.5 Analytics
            avg("pm25").alias("avg_pm25"),
            max("pm25").alias("max_pm25"),
            min("pm25").alias("min_pm25"),
            stddev("pm25").alias("pm25_stddev"),
            
            # PM10 Analytics
            avg("pm10").alias("avg_pm10"),
            max("pm10").alias("max_pm10"),
            
            # Environmental Analytics
            avg("co2").alias("avg_co2"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            
            # Sensor Coverage - FIXED: approx_count_distinct instead of countDistinct
            count("*").alias("reading_count"),
            approx_count_distinct("sensor_id").alias("unique_sensors"),
            
            # Location Analytics
            avg("latitude").alias("center_lat"),
            avg("longitude").alias("center_lon")
        ) \
        .withColumn("air_quality_index",
            when(col("avg_pm25") <= 12, "GOOD")
            .when(col("avg_pm25") <= 35, "MODERATE")
            .when(col("avg_pm25") <= 55, "UNHEALTHY_SENSITIVE")
            .when(col("avg_pm25") <= 150, "UNHEALTHY")
            .when(col("avg_pm25") <= 250, "VERY_UNHEALTHY")
            .otherwise("HAZARDOUS")
        ) \
        .withColumn("health_alert",
            when(col("avg_pm25") > 150, "IMMEDIATE_ACTION_REQUIRED")
            .when(col("avg_pm25") > 55, "SENSITIVE_GROUPS_ALERT")
            .otherwise("NO_ALERT")
        ) \
        .withColumn("pollution_trend",
            when(col("pm25_stddev") > 10, "HIGH_VARIABILITY")
            .when(col("pm25_stddev") > 5, "MODERATE_VARIABILITY")
            .otherwise("STABLE")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    
    # Stream to Silver layer
    output_path = f"s3://{output_bucket}/silver/realtime-analytics/air-quality/"
    checkpoint_path = f"s3://{output_bucket}/checkpoints/streaming/air-quality/"
    
    query = air_quality_analytics.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("air_quality_index") \
        .trigger(processingTime='1 minute') \
        .queryName("air_quality_analytics_24x7") \
        .start()
    
    print("Air Quality Analytics Stream started - running 24/7")
    return query

def start_traffic_stream(parsed_df, output_bucket):
    """
    24/7 Traffic Analytics Stream - FIXED AGGREGATIONS
    """
    
    print("Starting Traffic Analytics Stream...")
    
    # Filter traffic sensors
    traffic_df = parsed_df \
        .filter(col("sensor_type") == "traffic") \
        .select(
            col("sensor_id"),
            col("event_time"),
            col("location.zone").alias("zone"),
            col("location.lat").alias("latitude"),
            col("location.lon").alias("longitude"),
            col("measurements.vehicle_count").alias("vehicle_count"),
            col("measurements.avg_speed").alias("avg_speed")
        ) \
        .filter(col("vehicle_count").isNotNull() & col("avg_speed").isNotNull())
    
    # 5-minute traffic flow analytics - FIXED: Using approx_count_distinct
    traffic_analytics = traffic_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("zone")
        ) \
        .agg(
            # Vehicle Analytics
            avg("vehicle_count").alias("avg_vehicles"),
            max("vehicle_count").alias("max_vehicles"),
            min("vehicle_count").alias("min_vehicles"),
            stddev("vehicle_count").alias("vehicle_count_stddev"),
            
            # Speed Analytics
            avg("avg_speed").alias("avg_speed"),
            max("avg_speed").alias("max_speed"),
            min("avg_speed").alias("min_speed"),
            stddev("avg_speed").alias("speed_stddev"),
            
            # Traffic Metrics - FIXED: approx_count_distinct instead of countDistinct
            count("*").alias("reading_count"),
            approx_count_distinct("sensor_id").alias("active_sensors"),
            
            # Location Analytics
            avg("latitude").alias("center_lat"),
            avg("longitude").alias("center_lon")
        ) \
        .withColumn("congestion_level",
            when(col("avg_vehicles") > 75, "SEVERE")
            .when(col("avg_vehicles") > 50, "HIGH")
            .when(col("avg_vehicles") > 25, "MODERATE")
            .otherwise("LOW")
        ) \
        .withColumn("speed_category",
            when(col("avg_speed") < 15, "CRAWLING")
            .when(col("avg_speed") < 30, "SLOW")
            .when(col("avg_speed") < 50, "MODERATE")
            .otherwise("FAST")
        ) \
        .withColumn("traffic_efficiency",
            when((col("avg_vehicles") > 40) & (col("avg_speed") < 20), "INEFFICIENT")
            .when((col("avg_vehicles") < 20) & (col("avg_speed") > 40), "EFFICIENT")
            .otherwise("NORMAL")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    
    # Stream to Silver layer
    output_path = f"s3://{output_bucket}/silver/realtime-analytics/traffic/"
    checkpoint_path = f"s3://{output_bucket}/checkpoints/streaming/traffic/"
    
    query = traffic_analytics.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("congestion_level") \
        .trigger(processingTime='1 minute') \
        .queryName("traffic_analytics_24x7") \
        .start()
    
    print("Traffic Analytics Stream started - running 24/7")
    return query

def start_correlation_stream(parsed_df, output_bucket):
    """
    24/7 Cross-Correlation Analytics Stream - SIMPLIFIED
    """
    
    print("Starting Cross-Correlation Analytics Stream...")
    
    # Air quality data for correlation
    aq_windowed = parsed_df \
        .filter(col("sensor_type") == "air_quality") \
        .select(
            col("event_time"),
            col("location.zone").alias("zone"),
            col("measurements.pm25").alias("pm25"),
            col("measurements.temperature").alias("temperature")
        ) \
        .filter(col("pm25").isNotNull()) \
        .withWatermark("event_time", "15 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes"),
            col("zone")
        ) \
        .agg(
            avg("pm25").alias("avg_pm25"),
            avg("temperature").alias("avg_temperature"),
            count("*").alias("aq_readings")
        )
    
    # Traffic data for correlation
    traffic_windowed = parsed_df \
        .filter(col("sensor_type") == "traffic") \
        .select(
            col("event_time"),
            col("location.zone").alias("zone"),
            col("measurements.vehicle_count").alias("vehicle_count"),
            col("measurements.avg_speed").alias("avg_speed")
        ) \
        .filter(col("vehicle_count").isNotNull()) \
        .withWatermark("event_time", "15 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes"),
            col("zone")
        ) \
        .agg(
            avg("vehicle_count").alias("avg_vehicles"),
            avg("avg_speed").alias("avg_speed"),
            count("*").alias("traffic_readings")
        )
    
    # Join and calculate correlations
    correlation_analytics = aq_windowed.join(
        traffic_windowed,
        ["window", "zone"],
        "inner"
    ) \
    .withColumn("pollution_traffic_ratio",
        when(col("avg_vehicles") > 0, col("avg_pm25") / col("avg_vehicles"))
        .otherwise(0)
    ) \
    .withColumn("correlation_strength",
        when(col("pollution_traffic_ratio") > 2, "STRONG_POSITIVE")
        .when(col("pollution_traffic_ratio") > 1, "MODERATE_POSITIVE")
        .when(col("pollution_traffic_ratio") > 0.5, "WEAK_POSITIVE")
        .otherwise("MINIMAL")
    ) \
    .withColumn("environmental_impact",
        when((col("avg_vehicles") > 50) & (col("avg_pm25") > 35), "HIGH_IMPACT")
        .when((col("avg_vehicles") > 25) & (col("avg_pm25") > 20), "MODERATE_IMPACT")
        .otherwise("LOW_IMPACT")
    ) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")
    
    # Stream correlations to Silver layer
    output_path = f"s3://{output_bucket}/silver/realtime-analytics/correlations/"
    checkpoint_path = f"s3://{output_bucket}/checkpoints/streaming/correlations/"
    
    query = correlation_analytics.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("correlation_strength") \
        .trigger(processingTime='2 minutes') \
        .queryName("correlation_analytics_24x7") \
        .start()
    
    print("Cross-Correlation Analytics Stream started - running 24/7")
    return query

if __name__ == "__main__":
    main()
    job.commit()
