import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def main():
    """
    Silver to Gold ETL with proper error handling
    """
    
    print("=== Starting Silver to Gold ETL Processing (Fixed) ===")
    
    source_bucket = args.get('source_bucket', 'smart-city-datalake-2026')
    target_bucket = args.get('target_bucket', 'smart-city-datalake-2026')
    
    # Process last 24 hours of Silver data
    current_time = datetime.now()
    yesterday = current_time - timedelta(days=1)
    
    print(f"Processing data from: {yesterday.isoformat()}")
    
    # Check if Silver data exists first
    silver_paths = {
        'air_quality': f"s3://{source_bucket}/silver/realtime-analytics/air-quality/",
        'traffic': f"s3://{source_bucket}/silver/realtime-analytics/traffic/",
        'correlations': f"s3://{source_bucket}/silver/realtime-analytics/correlations/"
    }
    
    # Process each data type
    for data_type, path in silver_paths.items():
        try:
            print(f"Checking {data_type} data at: {path}")
            
            # Try to read and check if data exists
            df = spark.read.parquet(path)
            count = df.count()
            
            if count > 0:
                print(f"Found {count} records for {data_type}")
                
                if data_type == 'air_quality':
                    create_hourly_air_quality_gold(df, target_bucket, yesterday)
                elif data_type == 'traffic':
                    create_hourly_traffic_gold(df, target_bucket, yesterday)
                elif data_type == 'correlations':
                    create_daily_correlations_gold(df, target_bucket, yesterday)
            else:
                print(f"No data found for {data_type}")
                
        except Exception as e:
            print(f"Error processing {data_type}: {str(e)}")
            continue
    
    # Create curated datasets (only if we have Gold data)
    try:
        create_public_health_alerts(target_bucket, yesterday)
        create_ml_features(target_bucket, yesterday)
    except Exception as e:
        print(f"Could not create curated datasets: {str(e)}")
    
    print("Silver to Gold ETL completed")

def create_hourly_air_quality_gold(silver_df, target_bucket, start_date):
    """
    Create hourly aggregated air quality data
    """
    
    print("Creating hourly air quality aggregations...")
    
    # Filter to recent data and validate required columns
    recent_df = silver_df \
        .filter(col("window_start") >= lit(start_date.isoformat())) \
        .filter(
            col("zone").isNotNull() & 
            col("avg_pm25").isNotNull() &
            col("window_start").isNotNull()
        )
    
    if recent_df.count() == 0:
        print("No recent air quality data to process")
        return
    
    # Create hourly aggregations
    hourly_agg = recent_df \
        .withColumn("hour_timestamp", date_trunc("hour", col("window_start"))) \
        .groupBy("zone", "hour_timestamp") \
        .agg(
            # Air Quality Metrics
            avg("avg_pm25").alias("hourly_avg_pm25"),
            max("max_pm25").alias("hourly_max_pm25"),
            min("min_pm25").alias("hourly_min_pm25"),
            
            # Environmental Metrics (handle nulls)
            avg(coalesce(col("avg_pm10"), lit(0))).alias("hourly_avg_pm10"),
            avg(coalesce(col("avg_co2"), lit(0))).alias("hourly_avg_co2"),
            avg(coalesce(col("avg_temperature"), lit(0))).alias("hourly_avg_temperature"),
            avg(coalesce(col("avg_humidity"), lit(0))).alias("hourly_avg_humidity"),
            
            # Operational Metrics
            sum(coalesce(col("reading_count"), lit(0))).alias("total_readings"),
            avg(coalesce(col("unique_sensors"), lit(0))).alias("avg_active_sensors"),
            count("*").alias("measurement_windows")
        ) \
        .withColumn("air_quality_index",
            when(col("hourly_avg_pm25") <= 12, "GOOD")
            .when(col("hourly_avg_pm25") <= 35, "MODERATE")
            .when(col("hourly_avg_pm25") <= 55, "UNHEALTHY_SENSITIVE")
            .when(col("hourly_avg_pm25") <= 150, "UNHEALTHY")
            .when(col("hourly_avg_pm25") <= 250, "VERY_UNHEALTHY")
            .otherwise("HAZARDOUS")
        ) \
        .withColumn("health_risk_score",
            when(col("hourly_avg_pm25") <= 12, 1)
            .when(col("hourly_avg_pm25") <= 35, 2)
            .when(col("hourly_avg_pm25") <= 55, 3)
            .when(col("hourly_avg_pm25") <= 150, 4)
            .when(col("hourly_avg_pm25") <= 250, 5)
            .otherwise(6)
        ) \
        .withColumn("processing_date", current_date()) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Write to Gold layer
    gold_path = f"s3://{target_bucket}/gold/aggregated/air-quality-hourly/"
    
    print(f"Writing {hourly_agg.count()} hourly air quality records to Gold layer")
    
    hourly_agg.write \
        .mode("append") \
        .partitionBy("processing_date", "air_quality_index") \
        .parquet(gold_path)
    
    print(f"Hourly air quality aggregations written to {gold_path}")

def create_hourly_traffic_gold(silver_df, target_bucket, start_date):
    """
    Create hourly aggregated traffic data
    """
    
    print("Creating hourly traffic aggregations...")
    
    # Filter to recent data and validate required columns
    recent_df = silver_df \
        .filter(col("window_start") >= lit(start_date.isoformat())) \
        .filter(
            col("zone").isNotNull() & 
            col("avg_vehicles").isNotNull() &
            col("window_start").isNotNull()
        )
    
    if recent_df.count() == 0:
        print("No recent traffic data to process")
        return
    
    # Create hourly aggregations
    hourly_agg = recent_df \
        .withColumn("hour_timestamp", date_trunc("hour", col("window_start"))) \
        .groupBy("zone", "hour_timestamp") \
        .agg(
            # Vehicle Metrics
            avg("avg_vehicles").alias("hourly_avg_vehicles"),
            max(coalesce(col("max_vehicles"), col("avg_vehicles"))).alias("hourly_max_vehicles"),
            min(coalesce(col("min_vehicles"), col("avg_vehicles"))).alias("hourly_min_vehicles"),
            
            # Speed Metrics
            avg(coalesce(col("avg_speed"), lit(0))).alias("hourly_avg_speed"),
            max(coalesce(col("max_speed"), col("avg_speed"), lit(0))).alias("hourly_max_speed"),
            min(coalesce(col("min_speed"), col("avg_speed"), lit(0))).alias("hourly_min_speed"),
            
            # Operational Metrics
            sum(coalesce(col("reading_count"), lit(0))).alias("total_readings"),
            avg(coalesce(col("active_sensors"), lit(0))).alias("avg_active_sensors"),
            count("*").alias("measurement_windows")
        ) \
        .withColumn("congestion_level",
            when(col("hourly_avg_vehicles") > 75, "SEVERE")
            .when(col("hourly_avg_vehicles") > 50, "HIGH")
            .when(col("hourly_avg_vehicles") > 25, "MODERATE")
            .otherwise("LOW")
        ) \
        .withColumn("traffic_efficiency_score",
            when((col("hourly_avg_vehicles") < 20) & (col("hourly_avg_speed") > 40), 5)
            .when((col("hourly_avg_vehicles") < 40) & (col("hourly_avg_speed") > 30), 4)
            .when((col("hourly_avg_vehicles") < 60) & (col("hourly_avg_speed") > 20), 3)
            .when((col("hourly_avg_vehicles") < 80) & (col("hourly_avg_speed") > 15), 2)
            .otherwise(1)
        ) \
        .withColumn("peak_hour_indicator",
            when(
                (hour(col("hour_timestamp")).between(7, 9)) | 
                (hour(col("hour_timestamp")).between(17, 19)), 
                "PEAK"
            ).otherwise("OFF_PEAK")
        ) \
        .withColumn("processing_date", current_date()) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Write to Gold layer
    gold_path = f"s3://{target_bucket}/gold/aggregated/traffic-hourly/"
    
    print(f"Writing {hourly_agg.count()} hourly traffic records to Gold layer")
    
    hourly_agg.write \
        .mode("append") \
        .partitionBy("processing_date", "congestion_level") \
        .parquet(gold_path)
    
    print(f"Hourly traffic aggregations written to {gold_path}")

def create_daily_correlations_gold(silver_df, target_bucket, start_date):
    """
    Create daily correlation analysis
    """
    
    print("Creating daily correlation analysis...")
    
    # Filter to recent data and validate required columns
    recent_df = silver_df \
        .filter(col("window_start") >= lit(start_date.isoformat())) \
        .filter(
            col("zone").isNotNull() & 
            col("avg_pm25").isNotNull() &
            col("avg_vehicles").isNotNull() &
            col("window_start").isNotNull()
        )
    
    if recent_df.count() == 0:
        print("No recent correlation data to process")
        return
    
    # Create daily aggregations
    daily_agg = recent_df \
        .withColumn("day_date", date_trunc("day", col("window_start"))) \
        .groupBy("zone", "day_date") \
        .agg(
            avg("avg_pm25").alias("daily_avg_pm25"),
            avg("avg_vehicles").alias("daily_avg_vehicles"),
            avg(coalesce(col("pollution_traffic_ratio"), lit(0))).alias("daily_pollution_traffic_ratio"),
            count("*").alias("correlation_measurements")
        ) \
        .withColumn("correlation_strength",
            when(col("daily_pollution_traffic_ratio") > 2, "STRONG_POSITIVE")
            .when(col("daily_pollution_traffic_ratio") > 1, "MODERATE_POSITIVE")
            .when(col("daily_pollution_traffic_ratio") > 0.5, "WEAK_POSITIVE")
            .otherwise("MINIMAL")
        ) \
        .withColumn("environmental_impact_score",
            when((col("daily_avg_vehicles") > 50) & (col("daily_avg_pm25") > 35), 5)
            .when((col("daily_avg_vehicles") > 30) & (col("daily_avg_pm25") > 25), 4)
            .when((col("daily_avg_vehicles") > 20) & (col("daily_avg_pm25") > 15), 3)
            .when((col("daily_avg_vehicles") > 10) & (col("daily_avg_pm25") > 10), 2)
            .otherwise(1)
        ) \
        .withColumn("processing_date", current_date()) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Write to Gold layer
    gold_path = f"s3://{target_bucket}/gold/aggregated/correlations-daily/"
    
    print(f"Writing {daily_agg.count()} daily correlation records to Gold layer")
    
    daily_agg.write \
        .mode("append") \
        .partitionBy("processing_date", "correlation_strength") \
        .parquet(gold_path)
    
    print(f"Daily correlations written to {gold_path}")

def create_public_health_alerts(target_bucket, start_date):
    """
    Create public health alerts from Gold air quality data
    """
    
    print("Creating public health alerts...")
    
    # Try to read Gold air quality data
    gold_aq_path = f"s3://{target_bucket}/gold/aggregated/air-quality-hourly/"
    
    try:
        # Check if Gold data exists
        aq_df = spark.read.parquet(gold_aq_path) \
            .filter(col("hour_timestamp") >= lit(start_date.isoformat()))
        
        if aq_df.count() == 0:
            print("No Gold air quality data available for alerts")
            return
        
        # Generate health alerts for high-risk areas
        alerts_df = aq_df \
            .filter(col("health_risk_score") >= 4) \
            .select(
                col("zone"),
                col("hour_timestamp"),
                col("hourly_avg_pm25"),
                col("air_quality_index"),
                col("health_risk_score")
            ) \
            .withColumn("alert_type",
                when(col("health_risk_score") >= 5, "EMERGENCY")
                .when(col("health_risk_score") >= 4, "WARNING")
                .otherwise("ADVISORY")
            ) \
            .withColumn("alert_message",
                concat(
                    col("alert_type"), lit(": "), 
                    col("air_quality_index"), lit(" air quality in "), col("zone"), 
                    lit(". PM2.5: "), round(col("hourly_avg_pm25"), 1), lit(" μg/m³")
                )
            ) \
            .withColumn("processing_timestamp", current_timestamp())
        
        if alerts_df.count() > 0:
            # Write alerts to Gold layer
            alerts_path = f"s3://{target_bucket}/gold/curated/public-health-alerts/"
            
            alerts_df.write \
                .mode("append") \
                .partitionBy("alert_type") \
                .parquet(alerts_path)
            
            print(f"Created {alerts_df.count()} public health alerts")
        else:
            print("No health alerts needed - air quality is acceptable")
        
    except Exception as e:
        print(f"Could not create health alerts: {str(e)}")

def create_ml_features(target_bucket, start_date):
    """
    Create ML feature sets from Gold data
    """
    
    print("Creating ML feature sets...")
    
    try:
        # Try to read Gold aggregated data
        aq_path = f"s3://{target_bucket}/gold/aggregated/air-quality-hourly/"
        traffic_path = f"s3://{target_bucket}/gold/aggregated/traffic-hourly/"
        
        aq_df = spark.read.parquet(aq_path) \
            .filter(col("hour_timestamp") >= lit(start_date.isoformat()))
        
        traffic_df = spark.read.parquet(traffic_path) \
            .filter(col("hour_timestamp") >= lit(start_date.isoformat()))
        
        if aq_df.count() == 0 or traffic_df.count() == 0:
            print("Insufficient Gold data for ML features")
            return
        
        # Join air quality and traffic data for ML features
        ml_features = aq_df.join(
            traffic_df,
            ["zone", "hour_timestamp"],
            "inner"
        ) \
        .select(
            col("zone"),
            col("hour_timestamp"),
            # Air Quality Features
            col("hourly_avg_pm25").alias("pm25"),
            col("hourly_avg_pm10").alias("pm10"),
            col("hourly_avg_temperature").alias("temperature"),
            col("hourly_avg_humidity").alias("humidity"),
            col("health_risk_score"),
            # Traffic Features
            col("hourly_avg_vehicles").alias("vehicle_count"),
            col("hourly_avg_speed").alias("avg_speed"),
            col("traffic_efficiency_score"),
            # Time Features
            hour(col("hour_timestamp")).alias("hour_of_day"),
            dayofweek(col("hour_timestamp")).alias("day_of_week"),
            month(col("hour_timestamp")).alias("month")
        )
        
        if ml_features.count() > 0:
            # Write ML features to Gold layer
            ml_path = f"s3://{target_bucket}/gold/ml-features/combined-features/"
            
            ml_features.write \
                .mode("append") \
                .partitionBy("zone") \
                .parquet(ml_path)
            
            print(f"Created {ml_features.count()} ML feature records")
        else:
            print("No ML features created - insufficient joined data")
        
    except Exception as e:
        print(f"Could not create ML features: {str(e)}")

if __name__ == "__main__":
    main()
    job.commit()
