import json
import boto3
import uuid
import random
import math
from datetime import datetime, timezone
from decimal import Decimal
import os

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']  # iot_air_quality_sensors

def lambda_handler(event, context):
    """
    Air Quality IoT Data Simulator - Generates sensor data for DynamoDB batch processing
    """
    
    try:
        current_time = datetime.now(timezone.utc)
        timestamp_str = current_time.isoformat()
        
        # Generate air quality sensor readings (increased volume)
        sensors_data = generate_air_quality_sensors(current_time)
        
        # Store in DynamoDB for CDC batch processing
        results = []
        for sensor_data in sensors_data:
            dynamo_result = store_in_dynamodb(sensor_data)
            results.append({
                'sensor_id': sensor_data['sensor_id'],
                'status': dynamo_result
            })
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully stored {len(sensors_data)} air quality readings',
                'timestamp': timestamp_str,
                'total_sensors': len(sensors_data)
            })
        }
        
    except Exception as e:
        print(f"Error in air quality simulator: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def generate_air_quality_sensors(current_time):
    """Generate realistic air quality sensor data - INCREASED VOLUME"""
    
    sensors = []
    hour = current_time.hour
    
    # City zones with different sensor densities
    zones = [
        {'name': 'downtown', 'lat_base': 40.7128, 'lon_base': -74.0060, 'sensor_count': 5},
        {'name': 'midtown', 'lat_base': 40.7589, 'lon_base': -73.9851, 'sensor_count': 4},
        {'name': 'uptown', 'lat_base': 40.7831, 'lon_base': -73.9712, 'sensor_count': 3},
        {'name': 'industrial', 'lat_base': 40.6782, 'lon_base': -73.9442, 'sensor_count': 4},
        {'name': 'residential', 'lat_base': 40.7489, 'lon_base': -73.9680, 'sensor_count': 3}
    ]
    
    sensor_counter = 1
    
    for zone in zones:
        for i in range(zone['sensor_count']):
            sensor_id = f"AQ_{sensor_counter:04d}"
            
            # Zone-specific pollution patterns
            base_pm25 = 12
            
            # Industrial zones have higher baseline
            if zone['name'] == 'industrial':
                base_pm25 += 8
            
            # Time-based patterns (rush hours)
            if hour in [7, 8, 9, 17, 18, 19]:  # Rush hours
                base_pm25 += random.uniform(8, 15)
            elif hour in [22, 23, 0, 1, 2, 3, 4, 5]:  # Night hours
                base_pm25 += random.uniform(-3, 2)
            else:  # Regular hours
                base_pm25 += random.uniform(2, 8)
            
            pm25 = max(5, base_pm25 + random.gauss(0, 3))
            pm10 = pm25 * random.uniform(1.3, 1.5)
            
            # Temperature varies by time of day
            temp_base = 15 + 10 * math.sin((hour - 6) * math.pi / 12)
            temperature = temp_base + random.uniform(-2, 2)
            
            sensor_data = {
                'sensor_id': sensor_id,
                'timestamp': current_time.isoformat(),
                'event_id': str(uuid.uuid4()),
                'sensor_type': 'air_quality',
                
                # Location data
                'location': {
                    'lat': round(zone['lat_base'] + random.uniform(-0.01, 0.01), 6),
                    'lon': round(zone['lon_base'] + random.uniform(-0.01, 0.01), 6),
                    'zone': zone['name'],
                    'city': 'New_York',
                    'country': 'USA'
                },
                
                # Air quality measurements
                'measurements': {
                    'pm25': round(pm25, 2),
                    'pm10': round(pm10, 2),
                    'co2': round(400 + random.uniform(0, 100), 1),
                    'no2': round(random.uniform(10, 40), 2),
                    'o3': round(random.uniform(20, 60), 2),
                    'temperature': round(temperature, 1),
                    'humidity': round(50 + 20 * random.random(), 1),
                    'pressure': round(1013 + random.uniform(-10, 10), 1)
                },
                
                # Air Quality Index calculation (simplified)
                'aqi': {
                    'value': calculate_aqi(pm25),
                    'category': get_aqi_category(calculate_aqi(pm25))
                },
                
                # Device metadata
                'metadata': {
                    'device_status': random.choice(['active'] * 95 + ['maintenance'] * 5),
                    'battery_level': random.randint(60, 100),
                    'signal_strength': random.randint(-70, -30),
                    'firmware_version': '2.1.4',
                    'last_calibration': '2024-01-10T00:00:00Z'
                },
                
                # Partition fields for efficient S3/Glue querying
                'partition_date': current_time.strftime('%Y-%m-%d'),
                'partition_year': current_time.strftime('%Y'),
                'partition_month': current_time.strftime('%m'),
                'partition_day': current_time.strftime('%d'),
                'partition_hour': current_time.strftime('%H'),
                
                # Audit fields
                'created_at': current_time.isoformat(),
                'updated_at': current_time.isoformat(),
                'is_active': True,
                'data_source': 'synthetic_iot'
            }
            
            sensors.append(sensor_data)
            sensor_counter += 1
    
    return sensors

def calculate_aqi(pm25):
    """Simple AQI calculation based on PM2.5"""
    if pm25 <= 12:
        return int((50/12) * pm25)
    elif pm25 <= 35.4:
        return int(50 + ((100-50)/(35.4-12)) * (pm25-12))
    elif pm25 <= 55.4:
        return int(100 + ((150-100)/(55.4-35.4)) * (pm25-35.4))
    else:
        return int(150 + ((200-150)/(150.4-55.4)) * (pm25-55.4))

def get_aqi_category(aqi):
    """Get AQI category label"""
    if aqi <= 50:
        return 'Good'
    elif aqi <= 100:
        return 'Moderate'
    elif aqi <= 150:
        return 'Unhealthy for Sensitive Groups'
    elif aqi <= 200:
        return 'Unhealthy'
    else:
        return 'Very Unhealthy'

def convert_floats_to_decimal(obj):
    """Recursively convert float values to Decimal for DynamoDB"""
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def store_in_dynamodb(sensor_data):
    """Store sensor data in DynamoDB for CDC batch processing"""
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        item = convert_floats_to_decimal(sensor_data)
        table.put_item(Item=item)
        return 'success'
    except Exception as e:
        print(f"DynamoDB error for {sensor_data['sensor_id']}: {str(e)}")
        return f'error: {str(e)}'