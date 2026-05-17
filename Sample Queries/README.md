# Sample SQL Queries - Smart City IoT Analytics

This folder contains production-ready SQL queries for analyzing Smart City IoT data using Amazon Athena or any SQL engine connected to the Glue Data Catalog.

## 📁 Files

* **air_quality_queries.sql** - 8 queries for air quality analysis
* **traffic_queries.sql** - 10 queries for traffic pattern analysis

## 🚀 How to Use

### Option 1: Amazon Athena (Recommended)

1. Open AWS Athena Console
2. Select database: `smart_city_gold`
3. Copy-paste queries from SQL files
4. Execute and download results as CSV/JSON

### Option 2: AWS CLI

```bash
aws athena start-query-execution \
  --query-string "$(cat air_quality_queries.sql)" \
  --query-execution-context Database=smart_city_gold \
  --result-configuration OutputLocation=s3://YOUR-RESULTS-BUCKET/
```

### Option 3: Python + Boto3

```python
import boto3

athena = boto3.client('athena')
response = athena.start_query_execution(
    QueryString=open('air_quality_queries.sql').read(),
    QueryExecutionContext={'Database': 'smart_city_gold'},
    ResultConfiguration={'OutputLocation': 's3://YOUR-RESULTS-BUCKET/'}
)
```

## 📊 Query Categories

### Air Quality Queries

1. **Weekly Trends** - 7-day AQI analysis by location
2. **Hourly Monitoring** - Real-time dashboard (today's data)
3. **Pollution Hotspots** - Top 10 worst locations (24h)
4. **Sensor Health** - Maintenance alerts for critical devices
5. **Trend Comparison** - Week-over-week AQI changes
6. **Monthly Summary** - Executive reports with good/bad days
7. **Environmental Correlation** - Temperature/humidity vs AQI
8. **Data Quality** - Missing data detection

### Traffic Queries

1. **Congestion Hotspots** - High-traffic intersections (24h)
2. **Hourly Volume** - Intersection traffic by hour (today)
3. **Daily Summary** - City-wide metrics by road type
4. **Rush Hour Analysis** - Peak traffic times and patterns
5. **Incident Analysis** - Safety patterns and severity
6. **Congestion Trends** - 15-minute interval analysis
7. **Weekend vs Weekday** - Traffic pattern differences
8. **Dangerous Intersections** - High-incident locations
9. **Efficiency Score** - Performance benchmarking
10. **Growth Tracking** - Month-over-month volume changes

## 💡 Pro Tips

* **Use Partitions:** All queries leverage partition pruning (date/hour) for cost efficiency
* **Limit Results:** Add `LIMIT` clauses when exploring large datasets
* **Query History:** Athena saves recent queries - reuse them!
* **Cost Monitoring:** Each query scans X GB = $0.005/GB scanned
* **Optimization:** Use `WHERE` clauses with partition columns first

## 🔧 Customization

Modify these parameters to fit your needs:

* **Time Ranges:** `INTERVAL '7' DAY` → Change to '30 DAY', '90 DAY', etc.
* **Thresholds:** AQI levels, speed limits, incident counts
* **Aggregations:** `AVG()`, `SUM()`, `COUNT()` → Add `MEDIAN()`, `STDDEV()`
* **Filters:** Add city, road type, sensor type filters

## 📈 Performance Notes

* **Fast Queries (<1s):** Queries 1-4 (both files) with date filters
* **Medium (2-5s):** Queries 5-7 with aggregations
* **Slower (5-15s):** Queries 8-10 with JOINs and window functions

## 🎯 Use Cases

* **Operations Dashboard:** Queries 2, 1, 3 (air quality), Queries 2, 1 (traffic)
* **Executive Reports:** Queries 6, 5 (air quality), Queries 3, 10 (traffic)
* **Safety Audits:** Query 4 (air quality), Queries 5, 8 (traffic)
* **Planning:** Queries 7, 8 (air quality), Queries 4, 7, 9 (traffic)

## 📧 Questions?

Email: polurisaikrishnareddy@gmail.com

---

**Note:** All queries are compatible with AWS Athena (Presto SQL). For other SQL engines (Spark SQL, PostgreSQL), minor syntax adjustments may be needed.
