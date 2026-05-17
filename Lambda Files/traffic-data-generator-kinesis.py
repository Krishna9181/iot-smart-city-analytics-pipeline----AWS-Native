import json
import boto3
import uuid
import random
from datetime import datetime, timezone
import os

# Initialize AWS clients
kinesis = boto3.client('kinesis')

# Environment variables
KINESIS_STREAM = os.environ['KINESIS_STREAM']  # smart-city-traffic-stream

def lambda_handler(event, context):
    """
    Traffic IoT Data Simulator - Generates real-time traffic sensor data for Kinesis streaming
    """
    
    try:
        current_time = datetime.now(timezone.utc)
        timestamp_str = current_time.isoformat()
        
        # Generate traffic sensor readings
        sensors_data = generate_traffic_sensors(current_time)
        
        # Send to Kinesis in batch
        kinesis_result = send_batch_to_kinesis(sensors_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully sent {len(sensors_data)} traffic readings to Kinesis',
                'timestamp': timestamp_str,
                'stream': KINESIS_STREAM,
                'total_sensors': len(sensors_data)
            })
        }
        
    except Exception as e:
        print(f"Error in traffic simulator: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def generate_traffic_sensors(current_time):
    """Generate realistic traffic sensor data - REAL-TIME CHARACTERISTICS"""
    
    sensors = []
    hour = current_time.hour
    minute = current_time.minute
    
    # Traffic intersections with different patterns
    intersections = [
        {'name': 'Main_St_5th_Ave', 'lat_base': 40.7128, 'lon_base': -74.0060, 'type': 'major'},
        {'name': 'Broadway_42nd_St', 'lat_base': 40.7589, 'lon_base': -73.9851, 'type': 'major'},
        {'name': 'Park_Ave_23rd_St', 'lat_base': 40.7399, 'lon_base': -73.9850, 'type': 'arterial'},
        {'name': 'Amsterdam_96th_St', 'lat_base': 40.7946, 'lon_base': -73.9697, 'type': 'arterial'},
        {'name': 'West_Houston_St', 'lat_base': 40.7280, 'lon_base': -74.0020, 'type': 'local'},
        {'name': 'FDR_Drive_23rd', 'lat_base': 40.7350, 'lon_base': -73.9750, 'type': 'highway'},
        {'name': 'Brooklyn_Bridge', 'lat_base': 40.7061, 'lon_base': -73.9969, 'type': 'major'},
        {'name': 'Queens_Midtown_Tunnel', 'lat_base': 40.7434, 'lon_base': -73.9694, 'type': 'highway'}
    ]
    
    sensor_counter = 1
    
    for intersection in intersections:
        sensor_id = f"TR_{sensor_counter:04d}"
        
        # Traffic patterns based on time and intersection type
        base_vehicles = 15
        
        # Rush hour patterns (7-9 AM, 5-7 PM)
        if hour in [7, 8, 9]:  # Morning rush
            base_vehicles += random.randint(40, 80)
            if intersection['type'] in ['major', 'highway']:
                base_vehicles += 30  # Extra congestion on major routes
        elif hour in [17, 18, 19]:  # Evening rush
            base_vehicles += random.randint(45, 90)
            if intersection['type'] in ['major', 'highway']:
                base_vehicles += 40
        elif hour in [22, 23, 0, 1, 2, 3, 4, 5]:  # Night
            base_vehicles = random.randint(3, 12)
        else:  # Mid-day
            base_vehicles += random.randint(15, 35)
        
        # Minute-level variation (simulates real-time fluctuation)
        minute_variation = random.randint(-8, 8)
        vehicle_count = max(0, base_vehicles + minute_variation)
        
        # Speed inversely related to vehicle count (congestion)
        if vehicle_count > 70:
            avg_speed = random.uniform(5, 15)  # Heavy traffic
            congestion_level = 'severe'
        elif vehicle_count > 50:
            avg_speed = random.uniform(15, 25)  # Moderate traffic
            congestion_level = 'high'
        elif vehicle_count > 25:
            avg_speed = random.uniform(25, 35)  # Light traffic
            congestion_level = 'moderate'
        else:
            avg_speed = random.uniform(35, 55)  # Free flow
            congestion_level = 'low'
        
        # Highway speeds are higher
        if intersection['type'] == 'highway':
            avg_speed = min(avg_speed * 1.5, 65)
        
        sensor_data = {
            'sensor_id': sensor_id,
            'timestamp': current_time.isoformat(),
            'event_id': str(uuid.uuid4()),
            'sensor_type': 'traffic',
            
            # Location
            'location': {
                'lat': round(intersection['lat_base'] + random.uniform(-0.0005, 0.0005), 6),
                'lon': round(intersection['lon_base'] + random.uniform(-0.0005, 0.0005), 6),
                'intersection': intersection['name'],
                'road_type': intersection['type'],
                'city': 'New_York',
                'country': 'USA'
            },
            
            # Traffic Measurements (real-time metrics)
            'measurements': {
                'vehicle_count': vehicle_count,
                'avg_speed_mph': round(avg_speed, 1),
                'congestion_level': congestion_level,
                'pedestrian_count': random.randint(0, 30 if hour in range(7, 22) else 5),
                'bike_count': random.randint(0, 10 if hour in range(7, 20) else 1),
                'occupancy_percent': round(min(vehicle_count / 100 * 100, 100), 1),  # Road capacity utilization
                'queue_length_meters': round(max(0, (70 - avg_speed) * 2 + random.uniform(-5, 5)), 1)
            },
            
            # Derived Metrics
            'traffic_flow_score': calculate_flow_score(vehicle_count, avg_speed),
            'incident_detected': detect_incident(vehicle_count, avg_speed),
            
            # Device Metadata
            'metadata': {
                'device_status': random.choice(['active'] * 97 + ['maintenance'] * 3),
                'signal_strength': random.randint(-70, -30),
                'firmware_version': '3.2.1',
                'last_calibration': '2024-02-01T00:00:00Z'
            },
            
            # Partition Fields
            'partition_date': current_time.strftime('%Y-%m-%d'),
            'partition_year': current_time.strftime('%Y'),
            'partition_month': current_time.strftime('%m'),
            'partition_day': current_time.strftime('%d'),
            'partition_hour': current_time.strftime('%H'),
            
            # Audit
            'created_at': current_time.isoformat(),
            'data_source': 'synthetic_streaming',
            'ingestion_type': 'kinesis_stream'
        }
        
        sensors.append(sensor_data)
        sensor_counter += 1
    
    return sensors

def calculate_flow_score(vehicle_count, avg_speed):
    """
    Traffic flow efficiency score (0-100)
    Optimal: moderate vehicle count + high speed
    """
    if avg_speed > 40 and vehicle_count < 30:
        return random.randint(85, 100)  # Excellent flow
    elif avg_speed > 25 and vehicle_count < 50:
        return random.randint(65, 84)  # Good flow
    elif avg_speed > 15:
        return random.randint(40, 64)  # Fair flow
    else:
        return random.randint(10, 39)  # Poor flow

def detect_incident(vehicle_count, avg_speed):
    """
    Simulate incident detection (accident, stalled vehicle)
    Low speed + high count = potential incident
    """
    if avg_speed < 8 and vehicle_count > 60:
        return random.choice([True] * 3 + [False] * 7)  # 30% chance
    return False

def send_batch_to_kinesis(sensors_data):
    """Send traffic data to Kinesis in batch (up to 500 records)"""
    try:
        records = [
            {
                'Data': json.dumps(sensor),
                'PartitionKey': sensor['sensor_id']  # Ensures same sensor goes to same shard
            }
            for sensor in sensors_data
        ]
        
        response = kinesis.put_records(
            StreamName=KINESIS_STREAM,
            Records=records
        )
        
        failed_count = response.get('FailedRecordCount', 0)
        if failed_count > 0:
            print(f"Warning: {failed_count} records failed to send to Kinesis")
        
        return {
            'success_count': len(records) - failed_count,
            'failed_count': failed_count
        }
        
    except Exception as e:
        print(f"Kinesis error: {str(e)}")
        raise e