import json
import boto3
import uuid
from datetime import datetime, timezone
from decimal import Decimal
import os

# Initialize AWS clients
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_PREFIX', 'bronze/cdc/air_quality/')

def lambda_handler(event, context):
    """
    CDC Processor - Processes DynamoDB Stream events and stores in S3 Bronze (JSON Lines)
    Handles INSERT, MODIFY, REMOVE events with full CDC tracking
    """
    
    try:
        print(f"Processing {len(event['Records'])} DynamoDB Stream records")
        
        processed_records = []
        failed_records = []
        
        # Process each DynamoDB Stream record
        for record in event['Records']:
            try:
                processed = process_dynamodb_record(record)
                if processed:
                    processed_records.append(processed)
            except Exception as e:
                print(f"Failed to process record: {str(e)}")
                failed_records.append({
                    'record_id': record.get('eventID', 'unknown'),
                    'error': str(e)
                })
        
        # Store processed records in S3 Bronze (JSON Lines format)
        s3_location = None
        if processed_records:
            s3_location = store_in_s3_bronze_jsonl(processed_records)
            
            # # Send CloudWatch metrics
            # send_cloudwatch_metrics(
            #     namespace='SmartCity/CDC',
            #     metric_name='RecordsProcessed',
            #     value=len(processed_records),
            #     unit='Count'
            # )
        
        result = {
            'statusCode': 200 if not failed_records else 206,
            'body': json.dumps({
                'message': 'CDC processing complete',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'records_processed': len(processed_records),
                'records_failed': len(failed_records),
                's3_location': s3_location or 'No records to store',
                'failed_records': failed_records[:5]
            })
        }
        
        print(f"Completed: {len(processed_records)} processed, {len(failed_records)} failed")
        return result
        
    except Exception as e:
        print(f"Fatal error in CDC processor: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def process_dynamodb_record(record):
    """
    Process individual DynamoDB stream record
    Returns: Dict with CDC metadata + extracted sensor data
    """
    
    event_name = record['eventName']
    event_source = record['eventSource']
    
    if event_source != 'aws:dynamodb':
        print(f"Skipping non-DynamoDB event: {event_source}")
        return None
    
    processing_time = datetime.now(timezone.utc)
    
    # Base CDC record
    cdc_record = {
        'cdc_event_id': str(uuid.uuid4()),
        'cdc_event_type': event_name,
        'cdc_timestamp': processing_time.isoformat(),
        'cdc_source': 'dynamodb-streams',
        'cdc_sequence_number': record['dynamodb']['SequenceNumber'],
        'cdc_size_bytes': record['dynamodb']['SizeBytes'],
        'aws_region': record['awsRegion'],
        'table_arn': record['eventSourceARN'],
        'partition_year': processing_time.strftime('%Y'),
        'partition_month': processing_time.strftime('%m'),
        'partition_day': processing_time.strftime('%d'),
        'partition_hour': processing_time.strftime('%H'),
    }
    
    # Extract keys
    if 'Keys' in record['dynamodb']:
        cdc_record['keys'] = convert_dynamodb_to_json(record['dynamodb']['Keys'])
    
    # Extract old image (BEFORE state)
    if 'OldImage' in record['dynamodb']:
        cdc_record['old_image'] = convert_dynamodb_to_json(record['dynamodb']['OldImage'])
    
    # Extract new image (AFTER state)
    if 'NewImage' in record['dynamodb']:
        new_image = convert_dynamodb_to_json(record['dynamodb']['NewImage'])
        cdc_record['new_image'] = new_image
        
        # Flatten sensor data for easier querying
        if event_name in ['INSERT', 'MODIFY']:
            cdc_record['sensor_id'] = new_image.get('sensor_id')
            cdc_record['sensor_type'] = new_image.get('sensor_type')
            cdc_record['sensor_timestamp'] = new_image.get('timestamp')
            cdc_record['zone'] = new_image.get('location', {}).get('zone')
            
            # Extract key measurements
            measurements = new_image.get('measurements', {})
            cdc_record['pm25'] = measurements.get('pm25')
            cdc_record['temperature'] = measurements.get('temperature')
            cdc_record['aqi_value'] = new_image.get('aqi', {}).get('value')
    
    return cdc_record

def convert_dynamodb_to_json(dynamodb_item):
    """
    Convert DynamoDB types to native Python/JSON types
    """
    
    def convert_value(value):
        if 'S' in value:
            return value['S']
        elif 'N' in value:
            num_str = value['N']
            try:
                if '.' not in num_str:
                    return int(num_str)
                return float(num_str)
            except ValueError:
                return num_str
        elif 'BOOL' in value:
            return value['BOOL']
        elif 'NULL' in value:
            return None
        elif 'M' in value:
            return {k: convert_value(v) for k, v in value['M'].items()}
        elif 'L' in value:
            return [convert_value(item) for item in value['L']]
        elif 'SS' in value:
            return value['SS']
        elif 'NS' in value:
            return [float(n) for n in value['NS']]
        elif 'BS' in value:
            return value['BS']
        else:
            return value
    
    return {key: convert_value(value) for key, value in dynamodb_item.items()}

def store_in_s3_bronze_jsonl(records):
    """
    Store CDC records in S3 Bronze using JSON Lines format
    """
    
    try:
        now = datetime.now(timezone.utc)
        
        # Partition by date + hour
        partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        
        # Filename with timestamp and batch ID
        filename = f"cdc_air_quality_{now.strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}.jsonl"
        
        # Full S3 key
        s3_key = f"{S3_PREFIX}{partition_path}{filename}"
        
        # Convert records to JSON Lines format
        jsonl_content = '\n'.join([json.dumps(record) for record in records])
        jsonl_content += '\n'
        
        # Upload to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/x-ndjson',
            Metadata={
                'record_count': str(len(records)),
                'processing_timestamp': now.isoformat(),
                'source': 'dynamodb-cdc-processor',
                'format': 'jsonl'
            }
        )
        
        s3_location = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Stored {len(records)} CDC records to {s3_location}")
        
        return s3_location
        
    except Exception as e:
        print(f"Error storing in S3: {str(e)}")
        raise e

# def send_cloudwatch_metrics(namespace, metric_name, value, unit='Count'):
#     """Send custom metrics to CloudWatch"""
#     try:
#         cloudwatch.put_metric_data(
#             Namespace=namespace,
#             MetricData=[
#                 {
#                     'MetricName': metric_name,
#                     'Value': value,
#                     'Unit': unit,
#                     'Timestamp': datetime.now(timezone.utc)
#                 }
#             ]
#         )
#     except Exception as e:
#         print(f"Warning: Failed to send CloudWatch metric: {str(e)}")