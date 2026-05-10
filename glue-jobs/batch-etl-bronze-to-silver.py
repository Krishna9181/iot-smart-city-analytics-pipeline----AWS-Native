import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import boto3

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket', 
    'source_prefix',
    'target_prefix'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def main():
    """
    Main SCD Type 2 processing function - Bronze to Silver only
    """
    
    print("Starting Smart City SCD Type 2 Processing...")
    
    # Read CDC data from Bronze layer
    bronze_df = read_bronze_cdc_data()
    
    if bronze_df.count() == 0:
        print("No new CDC data to process")
        job.commit()
        return
    
    # Process CDC records and extract sensor data
    sensor_changes_df = process_cdc_records(bronze_df)
    
    # Read existing Silver layer data (if exists)
    existing_silver_df = read_existing_silver_data()
    
    # Apply SCD Type 2 logic
    updated_silver_df = apply_scd_type2_logic(sensor_changes_df, existing_silver_df)
    
    # Write to Silver layer only
    write_to_silver_layer(updated_silver_df)
    
    print("SCD Type 2 processing completed successfully")
    job.commit()

def read_bronze_cdc_data():
    """Read CDC data from Bronze S3 layer"""
    
    try:
        bronze_path = f"s3://{args['source_bucket']}/{args['source_prefix']}"
        print(f"Reading CDC data from: {bronze_path}")
        
        # Read JSON files from Bronze layer
        bronze_df = spark.read.option("multiline", "true").json(bronze_path)
        
        print(f"Found {bronze_df.count()} CDC batch files")
        return bronze_df
        
    except Exception as e:
        print(f"Error reading Bronze data: {str(e)}")
        return spark.createDataFrame([], StructType([]))

def process_cdc_records(bronze_df):
    """Extract and flatten CDC records into sensor changes"""
    
    try:
        # Explode the records array to get individual CDC records
        exploded_df = bronze_df.select(
            col("batch_metadata.processing_timestamp").alias("batch_timestamp"),
            explode(col("records")).alias("cdc_record")
        )
        
        # Extract CDC metadata and sensor data with flexible measurements
        sensor_changes_df = exploded_df.select(
            col("batch_timestamp"),
            col("cdc_record.cdc_metadata.event_name").alias("change_type"),
            col("cdc_record.cdc_metadata.processing_timestamp").alias("cdc_timestamp"),
            col("cdc_record.new_image.sensor_id").alias("sensor_id"),
            col("cdc_record.new_image.sensor_type").alias("sensor_type"),
            col("cdc_record.new_image.timestamp").alias("original_timestamp"),
            col("cdc_record.new_image.location").alias("location"),
            col("cdc_record.new_image.measurements").alias("measurements"),
            col("cdc_record.new_image.metadata").alias("metadata"),
            col("cdc_record.new_image.created_at").alias("created_at"),
            col("cdc_record.new_image.event_id").alias("event_id")
        ).filter(col("change_type").isin(["INSERT", "MODIFY"]))
        
        # Add SCD metadata
        sensor_changes_df = sensor_changes_df.withColumn(
            "effective_date", 
            to_timestamp(col("cdc_timestamp"))
        ).withColumn(
            "end_date",
            lit(None).cast(TimestampType())
        ).withColumn(
            "is_current", 
            lit(True)
        ).withColumn(
            "record_version", 
            lit(1)
        ).withColumn(
            "scd_hash",
            sha2(concat_ws("|", 
                coalesce(col("sensor_id"), lit("")),
                coalesce(col("sensor_type"), lit("")),
                coalesce(col("measurements").cast("string"), lit("")),
                coalesce(col("metadata").cast("string"), lit(""))
            ), 256)
        )
        
        print(f"Processed {sensor_changes_df.count()} sensor changes")
        return sensor_changes_df
        
    except Exception as e:
        print(f"Error processing CDC records: {str(e)}")
        return spark.createDataFrame([], get_silver_schema())

def get_silver_schema():
    """Define flexible Silver layer schema for both sensor types"""
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("original_timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("zone", StringType(), True),
            StructField("intersection", StringType(), True)  # For traffic sensors
        ]), True),
        StructField("measurements", MapType(StringType(), StringType()), True),  # Flexible measurements
        StructField("metadata", MapType(StringType(), StringType()), True),     # Flexible metadata
        StructField("created_at", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("effective_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("record_version", IntegerType(), True),
        StructField("scd_hash", StringType(), True)
    ])

def read_existing_silver_data():
    """Read existing Silver layer data for SCD processing"""
    
    try:
        silver_path = f"s3://{args['target_bucket']}/{args['target_prefix']}"
        print(f"Reading existing Silver data from: {silver_path}")
        
        existing_df = spark.read.parquet(silver_path)
        print(f"Found {existing_df.count()} existing Silver records")
        return existing_df
        
    except Exception as e:
        print(f"No existing Silver data found (first run): {str(e)}")
        return spark.createDataFrame([], get_silver_schema())

def apply_scd_type2_logic(new_df, existing_df):
    """Apply SCD Type 2 logic to handle slowly changing dimensions"""
    
    try:
        if existing_df.count() == 0:
            print("First run: All records are new")
            return new_df
        
        # Find records that have changed (different hash)
        changed_records = new_df.alias("new").join(
            existing_df.filter(col("is_current") == True).alias("existing"),
            (col("new.sensor_id") == col("existing.sensor_id")) & 
            (col("new.scd_hash") != col("existing.scd_hash")),
            "inner"
        ).select("new.*")
        
        # Find completely new sensors
        new_sensors = new_df.alias("new").join(
            existing_df.alias("existing"),
            col("new.sensor_id") == col("existing.sensor_id"),
            "left_anti"
        )
        
        # Close out old records for changed sensors
        sensors_to_close = changed_records.select("sensor_id").distinct()
        
        closed_records = existing_df.filter(col("is_current") == True).join(
            sensors_to_close,
            "sensor_id",
            "inner"
        ).withColumn("end_date", current_timestamp()) \
         .withColumn("is_current", lit(False))
        
        # Keep unchanged current records
        unchanged_records = existing_df.filter(col("is_current") == True).join(
            sensors_to_close,
            "sensor_id", 
            "left_anti"
        )
        
        # Keep all historical records
        historical_records = existing_df.filter(col("is_current") == False)
        
        # Combine all records
        final_df = historical_records \
                  .union(closed_records) \
                  .union(unchanged_records) \
                  .union(changed_records) \
                  .union(new_sensors)
        
        print(f"SCD Type 2 processing complete: {final_df.count()} total records")
        return final_df
        
    except Exception as e:
        print(f"Error in SCD Type 2 processing: {str(e)}")
        return new_df

def write_to_silver_layer(df):
    """Write processed data to Silver layer in Parquet format"""
    
    try:
        silver_path = f"s3://{args['target_bucket']}/{args['target_prefix']}"
        
        # Write partitioned by sensor_type and date
        df.withColumn("partition_date", date_format(col("effective_date"), "yyyy-MM-dd")) \
          .write \
          .mode("overwrite") \
          .partitionBy("sensor_type", "partition_date") \
          .parquet(silver_path)
        
        print(f"Successfully wrote Silver layer data to: {silver_path}")
        
    except Exception as e:
        print(f"Error writing to Silver layer: {str(e)}")
        raise e

# Run main function
if __name__ == "__main__":
    main()
