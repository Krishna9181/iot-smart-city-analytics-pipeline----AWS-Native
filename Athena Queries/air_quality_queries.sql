-- Smart City IoT Analytics - Air Quality Sample Queries
-- Database: smart_city_gold
-- Tables: air_quality_hourly_avg, air_quality_daily_summary, sensor_health_metrics, aqi_trends

-- ============================================================================
-- Query 1: Weekly Air Quality Trends by Location
-- ============================================================================
-- Purpose: Analyze average AQI over the past 7 days for each location
-- Use Case: Weekly air quality reports for city officials

SELECT 
    reading_date,
    location,
    AVG(avg_aqi) as daily_avg_aqi,
    AVG(avg_pm25) as daily_avg_pm25,
    AVG(avg_pm10) as daily_avg_pm10,
    CASE 
        WHEN AVG(avg_aqi) <= 50 THEN 'Good'
        WHEN AVG(avg_aqi) <= 100 THEN 'Moderate'
        WHEN AVG(avg_aqi) <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN AVG(avg_aqi) <= 200 THEN 'Unhealthy'
        WHEN AVG(avg_aqi) <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
    END as air_quality_category,
    COUNT(DISTINCT sensor_id) as active_sensors
FROM smart_city_gold.air_quality_daily_summary
WHERE reading_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY reading_date, location
ORDER BY reading_date DESC, location;

-- ============================================================================
-- Query 2: Hourly Air Quality by Sensor (Today)
-- ============================================================================
-- Purpose: Real-time monitoring dashboard showing current day's hourly averages
-- Use Case: Operations dashboard for environmental monitoring team

SELECT 
    reading_hour,
    sensor_id,
    location,
    avg_aqi,
    avg_pm25,
    avg_pm10,
    avg_temperature,
    avg_humidity,
    reading_count,
    CASE 
        WHEN avg_aqi <= 50 THEN '🟢 Good'
        WHEN avg_aqi <= 100 THEN '🟡 Moderate'
        WHEN avg_aqi <= 150 THEN '🟠 USG'  -- Unhealthy for Sensitive Groups
        ELSE '🔴 Unhealthy'
    END as status_emoji
FROM smart_city_gold.air_quality_hourly_avg
WHERE DATE(reading_hour) = CURRENT_DATE
ORDER BY reading_hour DESC, sensor_id;

-- ============================================================================
-- Query 3: Top 10 Most Polluted Locations (Last 24 Hours)
-- ============================================================================
-- Purpose: Identify pollution hotspots requiring immediate attention
-- Use Case: Alert system for public health warnings

SELECT 
    location,
    AVG(avg_aqi) as avg_aqi_24h,
    MAX(avg_aqi) as peak_aqi,
    AVG(avg_pm25) as avg_pm25_24h,
    MAX(avg_pm25) as peak_pm25,
    COUNT(*) as hourly_readings,
    COUNT(DISTINCT sensor_id) as sensor_count
FROM smart_city_gold.air_quality_hourly_avg
WHERE reading_hour >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY location
HAVING AVG(avg_aqi) > 100  -- Filter for Moderate or worse
ORDER BY avg_aqi_24h DESC
LIMIT 10;

-- ============================================================================
-- Query 4: Sensor Health Monitoring - Critical Devices
-- ============================================================================
-- Purpose: Identify sensors requiring maintenance or replacement
-- Use Case: Operations team maintenance scheduling

SELECT 
    sensor_id,
    location,
    uptime_percentage,
    battery_avg,
    data_quality_score,
    total_readings,
    CASE 
        WHEN uptime_percentage < 90 THEN '🔴 Critical'
        WHEN uptime_percentage < 95 THEN '🟠 Warning'
        WHEN uptime_percentage < 99 THEN '🟡 Monitor'
        ELSE '🟢 Healthy'
    END as health_status,
    CASE 
        WHEN battery_avg < 20 THEN 'Replace Battery'
        WHEN uptime_percentage < 90 THEN 'Check Connection'
        WHEN data_quality_score < 0.8 THEN 'Calibrate Sensor'
        ELSE 'No Action Needed'
    END as recommended_action
FROM smart_city_gold.sensor_health_metrics
WHERE reading_date = CURRENT_DATE
    AND (uptime_percentage < 99 OR battery_avg < 30 OR data_quality_score < 0.9)
ORDER BY 
    CASE 
        WHEN uptime_percentage < 90 THEN 1
        WHEN battery_avg < 20 THEN 2
        WHEN data_quality_score < 0.8 THEN 3
        ELSE 4
    END,
    uptime_percentage ASC;

-- ============================================================================
-- Query 5: AQI Trend Analysis - Week-over-Week Comparison
-- ============================================================================
-- Purpose: Compare current week vs previous week air quality
-- Use Case: Trend reporting for policy makers

WITH current_week AS (
    SELECT 
        location,
        AVG(avg_aqi) as current_avg_aqi
    FROM smart_city_gold.air_quality_daily_summary
    WHERE reading_date >= DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY location
),
previous_week AS (
    SELECT 
        location,
        AVG(avg_aqi) as previous_avg_aqi
    FROM smart_city_gold.air_quality_daily_summary
    WHERE reading_date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY
        AND reading_date < DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY location
)
SELECT 
    cw.location,
    ROUND(cw.current_avg_aqi, 2) as current_week_aqi,
    ROUND(pw.previous_avg_aqi, 2) as previous_week_aqi,
    ROUND(cw.current_avg_aqi - pw.previous_avg_aqi, 2) as aqi_change,
    ROUND(((cw.current_avg_aqi - pw.previous_avg_aqi) / pw.previous_avg_aqi) * 100, 2) as percent_change,
    CASE 
        WHEN cw.current_avg_aqi > pw.previous_avg_aqi THEN '📈 Worsening'
        WHEN cw.current_avg_aqi < pw.previous_avg_aqi THEN '📉 Improving'
        ELSE '➡️ Stable'
    END as trend
FROM current_week cw
JOIN previous_week pw ON cw.location = pw.location
ORDER BY ABS(cw.current_avg_aqi - pw.previous_avg_aqi) DESC;

-- ============================================================================
-- Query 6: Monthly Air Quality Summary Report
-- ============================================================================
-- Purpose: Generate monthly summary for executive reporting
-- Use Case: Monthly board meetings and public reports

SELECT 
    DATE_TRUNC('month', reading_date) as month,
    location,
    COUNT(DISTINCT reading_date) as days_monitored,
    AVG(avg_aqi) as monthly_avg_aqi,
    MAX(avg_aqi) as worst_daily_aqi,
    MIN(avg_aqi) as best_daily_aqi,
    SUM(CASE WHEN avg_aqi <= 50 THEN 1 ELSE 0 END) as good_days,
    SUM(CASE WHEN avg_aqi > 50 AND avg_aqi <= 100 THEN 1 ELSE 0 END) as moderate_days,
    SUM(CASE WHEN avg_aqi > 100 THEN 1 ELSE 0 END) as unhealthy_days,
    ROUND(SUM(CASE WHEN avg_aqi <= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as good_days_percentage
FROM smart_city_gold.air_quality_daily_summary
WHERE reading_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3' MONTH
GROUP BY DATE_TRUNC('month', reading_date), location
ORDER BY month DESC, location;

-- ============================================================================
-- Query 7: Correlation Between Temperature/Humidity and AQI
-- ============================================================================
-- Purpose: Analyze environmental factors affecting air quality
-- Use Case: Research and predictive modeling

SELECT 
    reading_date,
    location,
    AVG(avg_temperature) as avg_temp,
    AVG(avg_humidity) as avg_humidity,
    AVG(avg_aqi) as avg_aqi,
    AVG(avg_pm25) as avg_pm25,
    CASE 
        WHEN AVG(avg_temperature) > 30 THEN 'Hot'
        WHEN AVG(avg_temperature) > 20 THEN 'Warm'
        ELSE 'Cool'
    END as temp_category,
    CASE 
        WHEN AVG(avg_humidity) > 70 THEN 'High'
        WHEN AVG(avg_humidity) > 40 THEN 'Moderate'
        ELSE 'Low'
    END as humidity_category
FROM smart_city_gold.air_quality_daily_summary
WHERE reading_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY reading_date, location
ORDER BY reading_date DESC;

-- ============================================================================
-- Query 8: Data Quality Report - Missing Data Detection
-- ============================================================================
-- Purpose: Identify gaps in sensor data collection
-- Use Case: Data pipeline monitoring and troubleshooting

WITH date_spine AS (
    SELECT 
        DATE(date_add('day', seq, DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '7' DAY)) as expected_date
    FROM (SELECT sequence(0, 6) as seq) 
    CROSS JOIN UNNEST(seq) as t(seq)
),
actual_data AS (
    SELECT 
        reading_date,
        sensor_id,
        location
    FROM smart_city_gold.air_quality_daily_summary
    WHERE reading_date >= CURRENT_DATE - INTERVAL '7' DAY
)
SELECT 
    ds.expected_date,
    s.sensor_id,
    s.location,
    CASE 
        WHEN ad.reading_date IS NULL THEN '❌ Missing'
        ELSE '✅ Present'
    END as data_status
FROM date_spine ds
CROSS JOIN (SELECT DISTINCT sensor_id, location FROM smart_city_gold.sensor_health_metrics WHERE reading_date = CURRENT_DATE) s
LEFT JOIN actual_data ad 
    ON ds.expected_date = ad.reading_date 
    AND s.sensor_id = ad.sensor_id
WHERE ad.reading_date IS NULL
ORDER BY ds.expected_date DESC, s.sensor_id;
