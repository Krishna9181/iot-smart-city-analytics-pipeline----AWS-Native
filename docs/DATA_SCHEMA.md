# Smart City IoT Analytics - Data Schema Documentation

**Total Tables:** 8 Gold Layer Tables + 2 Silver Layer Tables  
**Data Catalog:** AWS Glue Data Catalog

---

## 📊 Data Lake Layers Overview

| Layer | Purpose | Format | Compression | Retention |
|-------|---------|--------|-------------|-----------|
| **Bronze** | Raw, unprocessed data | JSON Lines | None/GZIP | 90 days → Glacier |
| **Silver** | Cleaned, validated data | Parquet | Snappy | 2 years |
| **Gold** | Aggregated, analytics-ready | Parquet | Snappy | Indefinite |

---

## 🥉 Bronze Layer Schemas

### 1. Air Quality CDC Data (Bronze)

**Location:** `s3://smart-city-datalake-2026/bronze/cdc/air_quality/`  
**Format:** JSON Lines (.jsonl)  
**Source:** DynamoDB Streams → Lambda CDC Processor

**Schema:**
```json
{
  "event_type": "INSERT|MODIFY|REMOVE",
  "timestamp": 1747123456789,
  "sensor_id": "AQ-NYC-001",
  "reading_timestamp": 1747123456,
  "data": {
    "sensor_type": "air_quality",
    "location": {
      "name": "Central Park",
      "latitude": 40.7829,
      "longitude": -73.9654,
      "city": "New York",
      "state": "NY",
      "country": "USA",
      "timezone": "America/New_York"
    },
    "measurements": {
      "pm2_5_ugm3": 12.5,
      "pm10_ugm3": 25.3,
      "no2_ppb": 15.8,
      "o3_ppb": 35.2,
      "co_ppm": 0.8,
      "temperature_celsius": 22.5,
      "humidity_percent": 65.0,
      "pressure_hpa": 1013.25
    },
    "device": {
      "status": "online",
      "battery_percent": 85,
      "firmware_version": "v2.1.0",
      "calibration_date": "2026-05-01"
    }
  }
}
```

### 2. Traffic Events (Bronze)

**Location:** `s3://smart-city-datalake-2026/bronze/traffic-events/traffic/`  
**Format:** JSON Lines (.jsonl.gz)  
**Source:** Kinesis Firehose

**Schema:**
```json
{
  "event_id": "TRF-20260517-001234",
  "timestamp": "2026-05-17T12:30:45Z",
  "sensor_id": "TR-NYC-TIMES-SQ",
  "sensor_type": "traffic_camera",
  "location": {
    "intersection": "Times Square & Broadway",
    "latitude": 40.7580,
    "longitude": -73.9855,
    "road_type": "arterial",
    "city": "New York",
    "country": "USA"
  },
  "traffic_data": {
    "vehicle_count": 145,
    "avg_speed_mph": 15.5,
    "congestion_level": 0.75,
    "pedestrian_count": 89,
    "bike_count": 5,
    "occupancy_percent": 0.85,
    "queue_length_meters": 45.2,
    "traffic_flow_score": 0.35
  },
  "incident": {
    "detected": false
  },
  "device": {
    "status": "online",
    "signal_strength_dbm": -65,
    "firmware_version": "v3.0.1"
  }
}
```

---

## 🥈 Silver Layer Schemas

### 1. Air Quality (Silver)

**Database:** `smart_city_silver`  
**Table:** `air_quality`  
**Location:** `s3://smart-city-datalake-2026/silver/air_quality/`  
**Format:** Parquet (Snappy)  
**Partitioning:** year/month/day

| Column Name | Data Type | Description | Nullable |
|-------------|-----------|-------------|----------|
| `sensor_id` | STRING | Unique sensor identifier | No |
| `reading_timestamp` | BIGINT | Unix timestamp (seconds) | No |
| `reading_datetime` | TIMESTAMP | Converted datetime | No |
| `sensor_type` | STRING | Always "air_quality" | No |
| `location_name` | STRING | Human-readable location | No |
| `latitude` | DOUBLE | Geographic latitude | No |
| `longitude` | DOUBLE | Geographic longitude | No |
| `city` | STRING | City name | No |
| `state` | STRING | State/province | No |
| `country` | STRING | Country code (USA, etc.) | No |
| `timezone` | STRING | IANA timezone | No |
| `pm2_5_ugm3` | DOUBLE | PM2.5 (µg/m³) | Yes |
| `pm10_ugm3` | DOUBLE | PM10 (µg/m³) | Yes |
| `no2_ppb` | DOUBLE | NO2 (ppb) | Yes |
| `o3_ppb` | DOUBLE | Ozone (ppb) | Yes |
| `co_ppm` | DOUBLE | CO (ppm) | Yes |
| `temperature_celsius` | DOUBLE | Temperature (°C) | Yes |
| `humidity_percent` | DOUBLE | Humidity (%) | Yes |
| `pressure_hpa` | DOUBLE | Pressure (hPa) | Yes |
| `aqi_pm2_5` | INT | AQI from PM2.5 | Yes |
| `aqi_pm10` | INT | AQI from PM10 | Yes |
| `overall_aqi` | INT | Maximum AQI | Yes |
| `aqi_category` | STRING | Good/Moderate/Unhealthy/etc. | Yes |
| `device_status` | STRING | online/offline | No |
| `battery_percent` | INT | Battery level (0-100) | Yes |
| `firmware_version` | STRING | Device firmware | Yes |
| `calibration_date` | DATE | Last calibration | Yes |
| `silver_processed_at` | TIMESTAMP | ETL processing time | No |
| `data_source` | STRING | Always "dynamodb_cdc" | No |
| **Partitions** | | | |
| `year` | INT | Year (YYYY) | No |
| `month` | INT | Month (MM) | No |
| `day` | INT | Day (DD) | No |

### 2. Traffic (Silver)

**Database:** `smart_city_silver`  
**Table:** `traffic_year_2026`  
**Location:** `s3://smart-city-datalake-2026/silver/traffic/`  
**Format:** Parquet (Snappy)  
**Partitioning:** year/month/day/hour

| Column Name | Data Type | Description | Nullable |
|-------------|-----------|-------------|----------|
| `sensor_id` | STRING | Unique sensor identifier | No |
| `reading_timestamp` | BIGINT | Unix timestamp | No |
| `reading_datetime` | TIMESTAMP | Converted datetime | No |
| `sensor_type` | STRING | Always "traffic_camera" | No |
| `event_id` | STRING | Unique event identifier | No |
| `latitude` | DOUBLE | Geographic latitude | No |
| `longitude` | DOUBLE | Geographic longitude | No |
| `intersection` | STRING | Intersection/location name | No |
| `road_type` | STRING | arterial/highway/residential | No |
| `city` | STRING | City name | No |
| `country` | STRING | Country code | No |
| `vehicle_count` | INT | Number of vehicles | Yes |
| `avg_speed_mph` | DOUBLE | Average speed (mph) | Yes |
| `congestion_level` | DOUBLE | 0.0-1.0 scale | Yes |
| `pedestrian_count` | INT | Number of pedestrians | Yes |
| `bike_count` | INT | Number of bicycles | Yes |
| `occupancy_percent` | DOUBLE | Road occupancy (0-1) | Yes |
| `queue_length_meters` | DOUBLE | Queue length (meters) | Yes |
| `traffic_flow_score` | DOUBLE | Flow quality (0-1) | Yes |
| `incident_detected` | BOOLEAN | Incident flag | No |
| `device_status` | STRING | online/offline | No |
| `signal_strength_dbm` | INT | Signal strength | Yes |
| `firmware_version` | STRING | Device firmware | Yes |
| `silver_processed_at` | TIMESTAMP | ETL processing time | No |
| `data_source` | STRING | Always "kinesis_stream" | No |
| `ingestion_type` | STRING | Always "streaming" | No |
| **Partitions** | | | |
| `year` | INT | Year (YYYY) | No |
| `month` | INT | Month (MM) | No |
| `day` | INT | Day (DD) | No |
| `hour` | INT | Hour (HH) | No |

---

## 🥇 Gold Layer Schemas

### Air Quality Gold Tables

#### 1. air_quality_hourly_avg

**Purpose:** Hourly aggregated air quality metrics by sensor and location  
**Update Frequency:** Every hour  
**Location:** `s3://smart-city-datalake-2026/gold/air_quality_hourly_avg/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `sensor_id` | STRING | Sensor identifier |
| `location_name` | STRING | Location name |
| `city` | STRING | City name |
| `state` | STRING | State/province |
| `hour` | TIMESTAMP | Hour timestamp |
| `hourly_avg_pm2_5` | DOUBLE | Avg PM2.5 (µg/m³) |
| `hourly_avg_pm10` | DOUBLE | Avg PM10 (µg/m³) |
| `hourly_avg_no2` | DOUBLE | Avg NO2 (ppb) |
| `hourly_avg_o3` | DOUBLE | Avg Ozone (ppb) |
| `hourly_avg_co` | DOUBLE | Avg CO (ppm) |
| `hourly_avg_temperature` | DOUBLE | Avg temperature (°C) |
| `hourly_avg_humidity` | DOUBLE | Avg humidity (%) |
| `hourly_reading_count` | BIGINT | Number of readings |
| `hourly_max_aqi` | INT | Max AQI in hour |
| `hourly_min_aqi` | INT | Min AQI in hour |

**Sample Query:**
```sql
SELECT city, hour, hourly_avg_pm2_5, hourly_max_aqi
FROM smart_city_gold.air_quality_hourly_avg
WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY hourly_avg_pm2_5 DESC
LIMIT 10;
```

#### 2. air_quality_daily_summary

**Purpose:** Daily aggregated statistics and AQI categories  
**Update Frequency:** Daily  
**Location:** `s3://smart-city-datalake-2026/gold/air_quality_daily_summary/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `date` | DATE | Date |
| `city` | STRING | City name |
| `state` | STRING | State/province |
| `sensor_count` | BIGINT | Active sensors |
| `total_readings` | BIGINT | Total readings |
| `avg_pm2_5_ugm3` | DOUBLE | Daily avg PM2.5 |
| `avg_pm10_ugm3` | DOUBLE | Daily avg PM10 |
| `avg_no2_ppb` | DOUBLE | Daily avg NO2 |
| `avg_o3_ppb` | DOUBLE | Daily avg O3 |
| `avg_co_ppm` | DOUBLE | Daily avg CO |
| `max_pm2_5_ugm3` | DOUBLE | Max PM2.5 |
| `max_pm10_ugm3` | DOUBLE | Max PM10 |
| `overall_aqi` | INT | Daily max AQI |
| `aqi_category` | STRING | Good/Moderate/Unhealthy |
| `unhealthy_hours` | BIGINT | Hours with AQI > 100 |
| `good_air_quality_percentage` | DOUBLE | % hours with good AQI |

#### 3. sensor_health_metrics

**Purpose:** Sensor device health, uptime, and data quality  
**Update Frequency:** Daily  
**Location:** `s3://smart-city-datalake-2026/gold/sensor_health_metrics/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `date` | DATE | Date |
| `sensor_id` | STRING | Sensor identifier |
| `sensor_type` | STRING | air_quality |
| `location_name` | STRING | Location name |
| `city` | STRING | City |
| `device_status` | STRING | online/offline |
| `total_readings` | BIGINT | Readings count |
| `expected_readings` | BIGINT | Expected count (288/day) |
| `uptime_percent` | DOUBLE | Uptime % |
| `avg_battery_percent` | DOUBLE | Avg battery level |
| `min_battery_percent` | INT | Min battery level |
| `battery_status` | STRING | healthy/low/critical |
| `firmware_version` | STRING | Firmware version |
| `last_calibration_date` | DATE | Last calibration |
| `days_since_calibration` | INT | Days since calibrated |
| `data_quality_score` | DOUBLE | Quality score (0-1) |

#### 4. aqi_trends

**Purpose:** Air Quality Index trends and alerts  
**Update Frequency:** Daily  
**Location:** `s3://smart-city-datalake-2026/gold/aqi_trends/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `date` | DATE | Date |
| `city` | STRING | City name |
| `daily_max_aqi` | INT | Max AQI |
| `daily_avg_aqi` | DOUBLE | Avg AQI |
| `aqi_category` | STRING | Category |
| `previous_day_aqi` | INT | Previous AQI |
| `aqi_change` | INT | Day-over-day change |
| `aqi_trend` | STRING | improving/worsening/stable |
| `alert_triggered` | BOOLEAN | Alert flag |
| `alert_reason` | STRING | Alert reason |
| `consecutive_unhealthy_days` | INT | Streak of bad days |

---

### Traffic Gold Tables

#### 5. traffic_hourly_by_intersection

**Purpose:** Hourly traffic metrics aggregated by intersection  
**Update Frequency:** Every hour  
**Location:** `s3://smart-city-datalake-2026/gold/traffic_hourly_by_intersection/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `hour` | TIMESTAMP | Hour timestamp |
| `intersection` | STRING | Intersection name |
| `city` | STRING | City name |
| `road_type` | STRING | Road classification |
| `hourly_avg_vehicles` | DOUBLE | Avg vehicle count |
| `hourly_max_vehicles` | INT | Max vehicles |
| `hourly_min_vehicles` | INT | Min vehicles |
| `hourly_avg_speed_mph` | DOUBLE | Avg speed (mph) |
| `hourly_max_speed_mph` | DOUBLE | Max speed |
| `hourly_min_speed_mph` | DOUBLE | Min speed |
| `hourly_avg_congestion` | DOUBLE | Avg congestion (0-1) |
| `hourly_max_congestion` | DOUBLE | Max congestion |
| `hourly_total_pedestrians` | BIGINT | Total pedestrians |
| `hourly_total_bikes` | BIGINT | Total bikes |
| `hourly_incidents` | BIGINT | Incident count |
| `reading_count` | BIGINT | Number of readings |
| `congestion_score` | STRING | low/medium/high/severe |

**Sample Query:**
```sql
SELECT intersection, city, hourly_avg_congestion, hourly_incidents
FROM smart_city_gold.traffic_hourly_by_intersection
WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND hourly_avg_congestion > 0.7
ORDER BY hourly_avg_congestion DESC;
```

#### 6. traffic_daily_summary

**Purpose:** Daily traffic aggregates by city and road type  
**Update Frequency:** Daily  
**Location:** `s3://smart-city-datalake-2026/gold/traffic_daily_summary/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `date` | DATE | Date |
| `city` | STRING | City name |
| `road_type` | STRING | Road classification |
| `daily_avg_vehicles` | DOUBLE | Avg vehicles |
| `daily_peak_vehicles` | INT | Peak traffic |
| `daily_min_vehicles` | INT | Min traffic |
| `daily_avg_speed_mph` | DOUBLE | Avg speed |
| `daily_peak_speed_mph` | DOUBLE | Max speed |
| `daily_avg_congestion` | DOUBLE | Avg congestion |
| `daily_total_pedestrians` | BIGINT | Total pedestrians |
| `daily_total_bikes` | BIGINT | Total bikes |
| `daily_incidents` | BIGINT | Incidents |
| `congestion_hours` | BIGINT | Hours with congestion > 0.7 |
| `congestion_percentage` | DOUBLE | % of day congested |
| `traffic_quality_rating` | STRING | excellent/good/fair/poor |
| `coverage_score` | DOUBLE | Data coverage (0-1) |

#### 7. traffic_incidents

**Purpose:** Incident extraction with severity classification  
**Update Frequency:** Every hour  
**Location:** `s3://smart-city-datalake-2026/gold/traffic_incidents/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `event_id` | STRING | Event identifier |
| `incident_datetime` | TIMESTAMP | Incident time |
| `sensor_id` | STRING | Sensor ID |
| `intersection` | STRING | Location |
| `city` | STRING | City |
| `road_type` | STRING | Road type |
| `vehicle_count` | INT | Vehicles present |
| `avg_speed_mph` | DOUBLE | Speed at time |
| `congestion_level` | DOUBLE | Congestion (0-1) |
| `incident_detected` | BOOLEAN | Always true |
| `severity` | STRING | critical/high/moderate |
| `severity_score` | DOUBLE | Severity (0-1) |

**Severity Logic:**
- **Critical:** congestion > 0.9 OR speed < 5 mph
- **High:** congestion > 0.7 OR speed < 15 mph  
- **Moderate:** Other incidents

#### 8. traffic_congestion_trends

**Purpose:** 15-minute interval congestion analysis  
**Update Frequency:** Every hour  
**Location:** `s3://smart-city-datalake-2026/gold/traffic_congestion_trends/`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `time_window` | TIMESTAMP | 15-min window start |
| `city` | STRING | City name |
| `road_type` | STRING | Road classification |
| `window_avg_congestion` | DOUBLE | Avg congestion |
| `window_max_congestion` | DOUBLE | Max congestion |
| `window_avg_speed_mph` | DOUBLE | Avg speed |
| `window_vehicle_count` | BIGINT | Vehicle count |
| `previous_window_congestion` | DOUBLE | Previous 15-min avg |
| `congestion_change` | DOUBLE | Change from previous |
| `trend_indicator` | STRING | worsening/improving/stable |
| `peak_period` | BOOLEAN | Rush hour flag |

---

## 🔑 Key Relationships

### Entity Relationships

```
Silver Air Quality (1) ──▶ (N) Gold Hourly Avg
                      ├──▶ (N) Gold Daily Summary
                      ├──▶ (N) Sensor Health
                      └──▶ (N) AQI Trends

Silver Traffic (1) ──▶ (N) Gold Hourly by Intersection
                  ├──▶ (N) Gold Daily Summary
                  ├──▶ (N) Incidents
                  └──▶ (N) Congestion Trends
```

### Common Join Patterns

**Join air quality with traffic by city and time:**
```sql
SELECT 
    aq.city,
    aq.hour,
    aq.hourly_max_aqi,
    t.hourly_avg_congestion
FROM smart_city_gold.air_quality_hourly_avg aq
JOIN smart_city_gold.traffic_hourly_by_intersection t
  ON aq.city = t.city 
  AND aq.hour = t.hour
WHERE aq.hour >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
```

---

## 📏 Data Quality Rules

### Silver Layer Validations

**Air Quality:**
- PM2.5, PM10: 0-500 µg/m³ (EPA scale)
- NO2, O3: 0-500 ppb
- CO: 0-50 ppm
- Temperature: -50 to 60°C
- Humidity: 0-100%
- Nulls allowed for measurements (sensor failures)

**Traffic:**
- Vehicle count: ≥ 0
- Speed: 0-120 mph
- Congestion: 0.0-1.0
- Occupancy: 0.0-1.0
- Nulls allowed for metrics

### Gold Layer Constraints

- All aggregations must have `reading_count` > 0
- Dates must be within last 10 years
- Hourly windows aligned to :00 minutes
- 15-min windows aligned to :00, :15, :30, :45

---

## 📐 Naming Conventions

**Tables:**
- Snake_case with underscores
- Descriptive names indicating content
- Suffix indicates granularity (_hourly, _daily)

**Columns:**
- Snake_case with underscores
- Prefix indicates scope (hourly_, daily_, avg_, max_, min_)
- Units in name where applicable (\_mph, \_ugm3, \_ppb)

**Partitions:**
- Lowercase, single word (year, month, day, hour)

---

