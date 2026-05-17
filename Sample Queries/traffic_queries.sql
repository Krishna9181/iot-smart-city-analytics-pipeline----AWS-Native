-- Smart City IoT Analytics - Traffic Sample Queries
-- Database: smart_city_gold
-- Tables: traffic_hourly_by_intersection, traffic_daily_summary, traffic_incidents, traffic_congestion_trends

-- ============================================================================
-- Query 1: Traffic Congestion Hotspots (Last 24 Hours)
-- ============================================================================
-- Purpose: Identify intersections with highest congestion
-- Use Case: Real-time traffic management and routing optimization

SELECT 
    intersection_id,
    location,
    COUNT(*) as high_congestion_periods,
    AVG(avg_speed_mph) as avg_speed,
    MAX(vehicle_count) as peak_vehicle_count,
    MIN(avg_speed_mph) as slowest_speed,
    ROUND(AVG(vehicle_count), 0) as avg_vehicle_count
FROM smart_city_gold.traffic_congestion_trends
WHERE congestion_level = 'High'
    AND reading_datetime >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY intersection_id, location
HAVING COUNT(*) > 10  -- At least 10 high-congestion periods (2.5 hours)
ORDER BY high_congestion_periods DESC, avg_speed ASC
LIMIT 10;

-- ============================================================================
-- Query 2: Hourly Traffic Volume by Intersection (Today)
-- ============================================================================
-- Purpose: Monitor traffic patterns throughout the day
-- Use Case: Traffic operations dashboard

SELECT 
    reading_hour,
    intersection_id,
    location,
    total_vehicles,
    avg_speed_mph,
    incident_count,
    CASE 
        WHEN avg_speed_mph < 15 THEN '🔴 Severe Congestion'
        WHEN avg_speed_mph < 25 THEN '🟠 Heavy Traffic'
        WHEN avg_speed_mph < 35 THEN '🟡 Moderate Traffic'
        ELSE '🟢 Free Flow'
    END as traffic_status,
    ROUND((total_vehicles * 1.0) / EXTRACT(HOUR FROM CURRENT_TIMESTAMP - reading_hour + INTERVAL '1' HOUR), 0) as vehicles_per_hour
FROM smart_city_gold.traffic_hourly_by_intersection
WHERE DATE(reading_hour) = CURRENT_DATE
ORDER BY reading_hour DESC, total_vehicles DESC;

-- ============================================================================
-- Query 3: Daily Traffic Summary by Road Type and City
-- ============================================================================
-- Purpose: High-level traffic metrics for city planning
-- Use Case: Weekly/monthly reports for transportation department

SELECT 
    reading_date,
    city,
    road_type,
    total_vehicles,
    avg_speed_mph,
    total_incidents,
    unique_intersections,
    ROUND(total_vehicles * 1.0 / unique_intersections, 0) as avg_vehicles_per_intersection,
    ROUND(total_incidents * 1000.0 / total_vehicles, 2) as incidents_per_1000_vehicles
FROM smart_city_gold.traffic_daily_summary
WHERE reading_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY reading_date DESC, total_vehicles DESC;

-- ============================================================================
-- Query 4: Rush Hour Analysis - Peak Traffic Times
-- ============================================================================
-- Purpose: Identify rush hour patterns for traffic signal optimization
-- Use Case: Signal timing adjustments and public transit scheduling

WITH hourly_stats AS (
    SELECT 
        EXTRACT(HOUR FROM reading_hour) as hour_of_day,
        EXTRACT(DOW FROM reading_hour) as day_of_week,  -- 0=Sunday, 6=Saturday
        AVG(total_vehicles) as avg_vehicles,
        AVG(avg_speed_mph) as avg_speed,
        COUNT(DISTINCT intersection_id) as active_intersections
    FROM smart_city_gold.traffic_hourly_by_intersection
    WHERE reading_hour >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    GROUP BY EXTRACT(HOUR FROM reading_hour), EXTRACT(DOW FROM reading_hour)
)
SELECT 
    hour_of_day,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    ROUND(avg_vehicles, 0) as avg_vehicles,
    ROUND(avg_speed, 2) as avg_speed_mph,
    active_intersections,
    CASE 
        WHEN hour_of_day BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Evening Rush'
        WHEN hour_of_day BETWEEN 22 AND 5 THEN 'Night'
        ELSE 'Off-Peak'
    END as time_period
FROM hourly_stats
WHERE day_of_week BETWEEN 1 AND 5  -- Weekdays only
ORDER BY day_of_week, hour_of_day;

-- ============================================================================
-- Query 5: Traffic Incident Analysis - Severity and Frequency
-- ============================================================================
-- Purpose: Analyze incident patterns for safety improvements
-- Use Case: Safety audits and infrastructure improvement planning

SELECT 
    reading_date,
    location,
    incident_type,
    severity_level,
    COUNT(*) as incident_count,
    AVG(vehicle_count) as avg_vehicles_during_incident,
    AVG(avg_speed_mph) as avg_speed_during_incident,
    MIN(avg_speed_mph) as min_speed_observed,
    ROUND(AVG(EXTRACT(HOUR FROM reading_datetime)), 1) as avg_hour_of_occurrence
FROM smart_city_gold.traffic_incidents
WHERE reading_date >= CURRENT_DATE - INTERVAL '30' DAY
    AND incident_type IS NOT NULL
GROUP BY reading_date, location, incident_type, severity_level
ORDER BY reading_date DESC, incident_count DESC;

-- ============================================================================
-- Query 6: Congestion Trend Analysis - 15-Minute Intervals
-- ============================================================================
-- Purpose: Detailed congestion patterns for adaptive signal control
-- Use Case: Real-time traffic signal optimization

SELECT 
    DATE(reading_datetime) as date,
    EXTRACT(HOUR FROM reading_datetime) as hour,
    intersection_id,
    location,
    congestion_level,
    COUNT(*) as interval_count,
    AVG(avg_speed_mph) as avg_speed,
    AVG(vehicle_count) as avg_vehicles,
    MIN(avg_speed_mph) as min_speed,
    MAX(vehicle_count) as max_vehicles
FROM smart_city_gold.traffic_congestion_trends
WHERE reading_datetime >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY 
    DATE(reading_datetime),
    EXTRACT(HOUR FROM reading_datetime),
    intersection_id,
    location,
    congestion_level
HAVING congestion_level IN ('High', 'Severe')
ORDER BY date DESC, hour, interval_count DESC;

-- ============================================================================
-- Query 7: Weekend vs Weekday Traffic Comparison
-- ============================================================================
-- Purpose: Compare traffic patterns between weekdays and weekends
-- Use Case: Resource allocation and maintenance scheduling

WITH traffic_by_day_type AS (
    SELECT 
        CASE 
            WHEN EXTRACT(DOW FROM reading_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        location,
        road_type,
        AVG(total_vehicles) as avg_vehicles,
        AVG(avg_speed_mph) as avg_speed,
        AVG(total_incidents) as avg_incidents
    FROM smart_city_gold.traffic_daily_summary
    WHERE reading_date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY 
        CASE WHEN EXTRACT(DOW FROM reading_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END,
        location,
        road_type
)
SELECT 
    location,
    road_type,
    MAX(CASE WHEN day_type = 'Weekday' THEN avg_vehicles END) as weekday_avg_vehicles,
    MAX(CASE WHEN day_type = 'Weekend' THEN avg_vehicles END) as weekend_avg_vehicles,
    MAX(CASE WHEN day_type = 'Weekday' THEN avg_speed END) as weekday_avg_speed,
    MAX(CASE WHEN day_type = 'Weekend' THEN avg_speed END) as weekend_avg_speed,
    ROUND(
        (MAX(CASE WHEN day_type = 'Weekday' THEN avg_vehicles END) - 
         MAX(CASE WHEN day_type = 'Weekend' THEN avg_vehicles END)) * 100.0 / 
        NULLIF(MAX(CASE WHEN day_type = 'Weekend' THEN avg_vehicles END), 0),
        2
    ) as weekday_increase_pct
FROM traffic_by_day_type
GROUP BY location, road_type
ORDER BY weekday_increase_pct DESC;

-- ============================================================================
-- Query 8: Most Dangerous Intersections - Incident Hot Spots
-- ============================================================================
-- Purpose: Identify high-risk intersections for safety improvements
-- Use Case: Budget allocation for infrastructure upgrades

SELECT 
    i.location,
    i.intersection_id,
    COUNT(DISTINCT i.reading_date) as days_with_incidents,
    COUNT(*) as total_incidents,
    SUM(CASE WHEN i.severity_level = 'Critical' THEN 1 ELSE 0 END) as critical_incidents,
    SUM(CASE WHEN i.severity_level = 'High' THEN 1 ELSE 0 END) as high_severity_incidents,
    AVG(i.avg_speed_mph) as avg_speed_during_incidents,
    AVG(h.avg_speed_mph) as normal_avg_speed,
    ROUND(AVG(h.avg_speed_mph) - AVG(i.avg_speed_mph), 2) as speed_reduction_during_incidents
FROM smart_city_gold.traffic_incidents i
LEFT JOIN smart_city_gold.traffic_hourly_by_intersection h
    ON i.intersection_id = h.intersection_id
    AND DATE(i.reading_datetime) = DATE(h.reading_hour)
WHERE i.reading_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY i.location, i.intersection_id
HAVING COUNT(*) >= 10  -- At least 10 incidents in 90 days
ORDER BY 
    SUM(CASE WHEN i.severity_level = 'Critical' THEN 1 ELSE 0 END) DESC,
    total_incidents DESC
LIMIT 15;

-- ============================================================================
-- Query 9: Traffic Flow Efficiency Score
-- ============================================================================
-- Purpose: Calculate efficiency score for each intersection
-- Use Case: Performance benchmarking and improvement tracking

WITH intersection_metrics AS (
    SELECT 
        intersection_id,
        location,
        road_type,
        AVG(avg_speed_mph) as avg_speed,
        AVG(total_vehicles) as avg_volume,
        SUM(incident_count) as total_incidents
    FROM smart_city_gold.traffic_hourly_by_intersection
    WHERE reading_hour >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    GROUP BY intersection_id, location, road_type
)
SELECT 
    intersection_id,
    location,
    road_type,
    ROUND(avg_speed, 2) as avg_speed_mph,
    ROUND(avg_volume, 0) as avg_daily_vehicles,
    total_incidents,
    -- Efficiency Score: (Speed / Expected Speed) * (1 - Incident Rate)
    ROUND(
        (avg_speed / 
         CASE road_type 
             WHEN 'Highway' THEN 65.0
             WHEN 'Arterial' THEN 45.0
             ELSE 35.0
         END) * 
        (1 - LEAST(total_incidents / 100.0, 0.5))  -- Cap incident penalty at 50%
        * 100,
        2
    ) as efficiency_score,
    CASE 
        WHEN ROUND((avg_speed / CASE road_type WHEN 'Highway' THEN 65.0 WHEN 'Arterial' THEN 45.0 ELSE 35.0 END) * (1 - LEAST(total_incidents / 100.0, 0.5)) * 100, 2) >= 80 THEN '🟢 Excellent'
        WHEN ROUND((avg_speed / CASE road_type WHEN 'Highway' THEN 65.0 WHEN 'Arterial' THEN 45.0 ELSE 35.0 END) * (1 - LEAST(total_incidents / 100.0, 0.5)) * 100, 2) >= 60 THEN '🟡 Good'
        WHEN ROUND((avg_speed / CASE road_type WHEN 'Highway' THEN 65.0 WHEN 'Arterial' THEN 45.0 ELSE 35.0 END) * (1 - LEAST(total_incidents / 100.0, 0.5)) * 100, 2) >= 40 THEN '🟠 Fair'
        ELSE '🔴 Poor'
    END as performance_rating
FROM intersection_metrics
ORDER BY efficiency_score DESC;

-- ============================================================================
-- Query 10: Month-over-Month Traffic Growth
-- ============================================================================
-- Purpose: Track traffic volume changes over time
-- Use Case: Capacity planning and infrastructure investment decisions

WITH monthly_traffic AS (
    SELECT 
        DATE_TRUNC('month', reading_date) as month,
        city,
        SUM(total_vehicles) as total_vehicles,
        AVG(avg_speed_mph) as avg_speed
    FROM smart_city_gold.traffic_daily_summary
    WHERE reading_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH
    GROUP BY DATE_TRUNC('month', reading_date), city
)
SELECT 
    month,
    city,
    total_vehicles,
    ROUND(avg_speed, 2) as avg_speed_mph,
    LAG(total_vehicles) OVER (PARTITION BY city ORDER BY month) as prev_month_vehicles,
    ROUND(
        (total_vehicles - LAG(total_vehicles) OVER (PARTITION BY city ORDER BY month)) * 100.0 / 
        NULLIF(LAG(total_vehicles) OVER (PARTITION BY city ORDER BY month), 0),
        2
    ) as month_over_month_growth_pct
FROM monthly_traffic
ORDER BY month DESC, city;
