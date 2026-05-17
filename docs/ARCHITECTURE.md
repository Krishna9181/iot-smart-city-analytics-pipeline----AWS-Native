# Smart City IoT Analytics - Architecture Documentation


## 📐 High-Level Architecture

This project implements a modern data lake architecture using AWS serverless technologies, featuring both batch and streaming data pipelines that follow the **Medallion Architecture** pattern (Bronze → Silver → Gold layers).

### Architecture Principles

1. **Serverless-First:** Minimize operational overhead using managed AWS services
2. **Event-Driven:** EventBridge schedules and DynamoDB Streams trigger automated workflows
3. **Cost-Optimized:** Pay-per-use pricing model, auto-scaling, and efficient resource utilization
4. **Scalable:** Handle growing data volumes without infrastructure changes
5. **Reliable:** Built-in retry logic, error handling, and monitoring

---

## 🏗️ System Components

### **1. Batch Pipeline - Air Quality Monitoring**

**Purpose:** Process air quality sensor data through CDC (Change Data Capture) from DynamoDB

#### Data Flow:
```
┌─────────────┐     ┌────────────┐     ┌────────────┐
│ EventBridge │────▶│   Lambda   │────▶│  DynamoDB  │
│  (5 min)    │     │ Generator  │     │   Table    │
└─────────────┘     └────────────┘     └──────┬─────┘
                                              │
                                       DynamoDB Streams
                                              │
                    ┌─────────────────────────▼
                    │
              ┌─────▼─────┐     ┌────────────┐
              │   Lambda  │────▶│  S3 Bronze │
              │ CDC Proc  │     │  (JSONL)   │
              └───────────┘     └──────┬─────┘
                                       │
                   ┌───────────────────▼
                   │
              ┌────▼────┐     ┌────────────┐
              │  Glue   │────▶│  S3 Silver │
              │  Batch  │     │ (Parquet)  │
              └─────────┘     └──────┬─────┘
                                     │
                  ┌──────────────────▼
                  │
             ┌────▼───┐     ┌────────────┐
             │  Glue  │────▶│  S3 Gold   │
             │  Gold  │     │ 4 Tables   │
             └────────┘     └────────────┘
```

#### Components:

**Data Generation:**
- **Lambda Function:** `smart-city-air-quality-generator`
- **Trigger:** EventBridge (every 1 minute)
- **Runtime:** Python 3.12
- **Memory:** 256 MB
- **Output:** Writes to DynamoDB table `iot_air_quality_sensors`

**CDC Processing:**
- **DynamoDB Streams:** Captures all INSERT/MODIFY/REMOVE operations
- **Lambda CDC Processor:** Processes stream records and writes to S3 Bronze
- **Runtime:** Python 3.12
- **Memory:** 512 MB
- **Output Format:** JSON Lines (.jsonl)

**Bronze → Silver Transformation:**
- **Glue Job:** `job-bronze-to-silver-air-quality`
- **Type:** Batch ETL
- **Workers:** 2x G.1X (4 vCPU, 16 GB RAM each)
- **Glue Version:** 4.0
- **Script Language:** PySpark
- **Transformations:**
  - Parse JSON records
  - Flatten nested structures
  - Data type casting
  - Null handling and validation
  - Add processing timestamp
  - Partition by date (year/month/day)

**Silver → Gold Aggregation:**
- **Glue Job:** `job-silver-to-gold-air-quality`
- **Type:** Batch ETL
- **Aggregations:**
  - Hourly averages by sensor/location
  - Daily summary statistics
  - AQI (Air Quality Index) calculations
  - Sensor health metrics
  - Trend analysis

**Output Tables (Gold Layer):**
1. `air_quality_hourly_avg` - Hourly aggregated sensor readings
2. `air_quality_daily_summary` - Daily statistics with AQI categories
3. `sensor_health_metrics` - Device uptime, battery status, data quality
4. `aqi_trends` - Air Quality Index trends and alerts

---

### **2. Streaming Pipeline - Traffic Monitoring**

**Purpose:** Real-time processing of traffic sensor data via Kinesis streams

#### Data Flow:
```
┌─────────────┐     ┌────────────┐     ┌────────────┐
│ EventBridge │────▶│   Lambda   │────▶│  Kinesis   │
│  (1 min)    │     │ Generator  │     │   Stream   │
└─────────────┘     └────────────┘     └──────┬─────┘
                                              │
                                              │
              ┌───────────────────────────────▼
              │
        ┌─────▼──────┐     ┌────────────┐
        │  Kinesis   │────▶│  S3 Bronze │
        │  Firehose  │     │ (JSONL.gz) │
        └────────────┘     └──────┬─────┘
                                  │
              ┌───────────────────▼
              │
        ┌─────▼─────┐     ┌────────────┐
        │   Glue    │────▶│  S3 Silver │
        │ Streaming │     │ (Parquet)  │
        │  (24/7)   │     └──────┬─────┘
        └───────────┘            │
                                 │
             ┌───────────────────▼
             │
        ┌────▼───┐     ┌────────────┐
        │  Glue  │────▶│  S3 Gold   │
        │ Batch  │     │ 4 Tables   │
        └────────┘     └────────────┘
```

#### Components:

**Data Generation:**
- **Lambda Function:** Traffic sensor data generator
- **Trigger:** EventBridge (every 1 minute)
- **Runtime:** Python 3.12
- **Output:** Writes to Kinesis Data Stream

**Stream Ingestion:**
- **Kinesis Data Stream:** `smart-city-traffic-stream`
  - Shard Count: 2
  - Retention: 24 hours
  - Throughput: ~1 MB/sec
- **Kinesis Firehose:** `smart-city-traffic-firehose`
  - Buffer: 5 MB or 300 seconds
  - Compression: GZIP
  - Dynamic Partitioning: year/month/day/hour

**Bronze → Silver Transformation (Streaming):**
- **Glue Job:** `traffic-streaming-bronze-to-silver`
- **Type:** Glue Streaming ETL
- **Workers:** 2x G.1X
- **Glue Version:** 4.0
- **Processing Mode:** Continuous micro-batches
- **Checkpoint:** `s3://smart-city-datalake-2026/checkpoints/glue-streaming/`
- **Transformations:**
  - Real-time data cleansing
  - Schema validation
  - Deduplication
  - Enrichment with geolocation
  - Partition by datetime (year/month/day/hour)

**Silver → Gold Aggregation (Batch):**
- **Glue Job:** `traffic-silver-to-gold`
- **Type:** Batch ETL (scheduled hourly)
- **Aggregations:**
  - Hourly metrics by intersection
  - Daily summaries by road type
  - Incident extraction and classification
  - 15-minute congestion trend analysis

**Output Tables (Gold Layer):**
1. `traffic_hourly_by_intersection` - Intersection-level hourly metrics
2. `traffic_daily_summary` - Daily aggregates by city and road type
3. `traffic_incidents` - Incident records with severity classification
4. `traffic_congestion_trends` - 15-min interval congestion analysis

---

## 🔄 Orchestration Architecture

### **Step Functions State Machines**

Two independent state machines orchestrate the pipelines:

#### **State Machine 1: Air Quality Pipeline**

**Execution Flow:**
1. Start Glue Job: `job-bronze-to-silver-air-quality` (with .sync for blocking)
2. Wait for job completion (with retry on failure)
3. Start Crawler: `crawler-silver-air-quality`
4. Wait and poll crawler status until READY
5. Start Glue Job: `job-silver-to-gold-air-quality`
6. Wait for job completion
7. Start 4 Gold Crawlers sequentially:
   - `crawler-gold-air-quality-hourly-avg`
   - `crawler-gold-air-quality-daily-summary`
   - `crawler-gold-air-quality-sensor-health-metrics`
   - `crawler-gold-air-quality-aqi-trends`
8. Send SNS success notification
9. On any error → Send SNS failure notification

**Retry Policy:**
- Glue Jobs: 2 attempts, 30s interval, exponential backoff (2x)
- Crawlers: 3 attempts, 30s interval, exponential backoff (1.5x)

**Timeout:** 1800 seconds (30 minutes) per Glue job

**Schedule:** Every 1 hour via EventBridge

#### **State Machine 2: Traffic Pipeline**

**Execution Flow:**
1. Start Crawler: `crawler-silver-traffic-events`
2. Wait for crawler completion
3. Start Glue Job: `traffic-silver-to-gold`
4. Wait for job completion
5. Start 4 Gold Crawlers sequentially:
   - `crawler-gold-traffic-hourly-interactions`
   - `crawler-gold-traffic-daily-summary`
   - `crawler-gold-traffic-incidents`
   - `crawler-gold-traffic-congestion-trends`
6. Send SNS success notification
7. On any error → Send SNS failure notification

**Schedule:** Every 1 hour via EventBridge

---

## 💾 Data Storage Architecture

### **S3 Data Lake Structure**

**Bucket:** `smart-city-datalake-2026`

```
s3://smart-city-datalake-2026/
│
├── bronze/                          # Raw, unprocessed data
│   ├── cdc/
│   │   └── air_quality/            # DynamoDB CDC JSONL files
│   │       └── YYYY-MM-DD/
│   └── traffic-events/
│       └── traffic/                 # Kinesis Firehose JSONL.gz files
│           └── year=YYYY/month=MM/day=DD/hour=HH/
│
├── silver/                          # Cleaned, validated data
│   ├── air_quality/                # Parquet, Snappy compressed
│   │   └── year=YYYY/month=MM/day=DD/
│   └── traffic/                     # Parquet, Snappy compressed
│       └── year=YYYY/month=MM/day=DD/hour=HH/
│
├── gold/                            # Aggregated, analytics-ready
│   ├── air_quality_hourly_avg/
│   ├── air_quality_daily_summary/
│   ├── sensor_health_metrics/
│   ├── aqi_trends/
│   ├── traffic_hourly_by_intersection/
│   ├── traffic_daily_summary/
│   ├── traffic_incidents/
│   └── traffic_congestion_trends/
│
├── checkpoints/
│   └── glue-streaming/             # Glue Streaming ETL checkpoints
│
└── athena-results/                 # Athena query results cache
```

### **Data Format Strategy**

| Layer | Format | Compression | Why? |
|-------|--------|-------------|------|
| Bronze | JSON Lines | None (Air Quality)<br>GZIP (Traffic) | Human-readable, easy debugging<br>Compressed for network efficiency |
| Silver | Parquet | Snappy | Columnar format, optimal for analytics<br>Fast compression/decompression |
| Gold | Parquet | Snappy | Best query performance<br>Minimal storage footprint |

### **Partitioning Strategy**

| Dataset | Partitioning | Rationale |
|---------|--------------|-----------|
| Air Quality Bronze | Not partitioned | Small daily volume |
| Air Quality Silver | year/month/day | Daily batch processing |
| Air Quality Gold | Varies by table | Match query patterns |
| Traffic Bronze | year/month/day/hour | High-volume streaming |
| Traffic Silver | year/month/day/hour | Hourly micro-batches |
| Traffic Gold | Varies by table | Optimized per use case |

---

## 📊 Data Catalog Architecture

### **Glue Data Catalog Databases**

1. **smart_city_bronze**
   - Raw CDC air quality data
   - Raw Kinesis traffic events

2. **smart_city_silver**
   - Cleaned air quality table
   - Cleaned traffic table (`traffic_year_2026` - auto-named by crawler)

3. **smart_city_gold**
   - 8 analytical tables (4 air quality + 4 traffic)

### **Crawler Strategy**

**Sequential Execution:**
- Crawlers run one after another to avoid schema conflicts
- Each crawler updates 60 seconds after previous completion
- Ensures data catalog consistency

**Crawler Configuration:**
- **Silver Crawlers:** Run after Bronze→Silver jobs complete
- **Gold Crawlers:** Run after Silver→Gold jobs complete
- **Schedule:** On-demand via Step Functions (not scheduled independently)

---

## 🔒 Security Architecture

### **IAM Roles & Policies**

**Lambda Execution Roles:**
- Air Quality Generator: DynamoDB PutItem
- CDC Processor: DynamoDB Streams read, S3 PutObject
- Traffic Generator: Kinesis PutRecords

**Glue Service Role:**
- S3 Read/Write (data lake bucket, scripts bucket)
- Glue Data Catalog full access
- CloudWatch Logs write

**Step Functions Execution Roles:**
- Glue StartJobRun, GetJobRun
- Glue StartCrawler, GetCrawler
- SNS Publish
- CloudWatch Logs write

**Kinesis Firehose Role:**
- Kinesis Data Stream read
- S3 PutObject
- CloudWatch Logs write

### **Encryption**

- **S3:** Server-side encryption (SSE-S3)
- **DynamoDB:** Encryption at rest enabled
- **Kinesis:** Server-side encryption with AWS managed keys
- **SNS:** Messages encrypted in transit (HTTPS)

---

## 📈 Scalability Design

### **Horizontal Scaling**

| Component | Scaling Method | Capacity |
|-----------|----------------|----------|
| Lambda | Auto-scales to 1000 concurrent | Per-account limit |
| DynamoDB | On-demand capacity mode | Auto-scales |
| Kinesis | 2 shards (can add more) | 2 MB/sec write |
| Glue | Worker count configurable | Currently 2 workers |
| S3 | Unlimited storage | No limits |

### **Performance Optimization**

1. **Partitioning:** Prune irrelevant data during queries
2. **Parquet Format:** Columnar storage reduces scan time
3. **Snappy Compression:** Balance between compression ratio and speed
4. **Glue Job Bookmarks:** Avoid reprocessing data
5. **Kinesis Batching:** Reduce API calls via Firehose buffering

---

## 🛠️ Technical Decisions

### **Why Glue Streaming Over EMR?**

**Initial Attempt:** AWS EMR with Spark Structured Streaming
**Result:** YARN exit code 13, application failures

**Switch to Glue Streaming:**
✅ Serverless - no cluster management  
✅ Auto-scaling  
✅ Built-in checkpoint management  
✅ Better Kinesis integration  
✅ Simpler error handling  
✅ Pay only for job runtime  

### **Why Two Separate State Machines?**

**Alternative Considered:** Single state machine for both pipelines

**Reasons for Separation:**
✅ **Independent Schedules:** Air quality (5 min gen, 1hr batch) vs Traffic (1 min gen, continuous streaming)  
✅ **Failure Isolation:** Traffic failure doesn't block air quality  
✅ **Easier Debugging:** Clear boundaries for troubleshooting  
✅ **Flexible Scaling:** Scale pipelines independently  

### **Why Sequential Crawler Execution?**

**Issue:** Parallel crawlers caused catalog conflicts when updating same database

**Solution:** Sequential execution with 60s wait between crawlers
- Ensures schema consistency
- Prevents race conditions
- Adds ~8 minutes to pipeline (acceptable trade-off)

---

## 🔮 Future Architecture Enhancements

### **Phase 2: Analytics Layer**
- Amazon QuickSight dashboards
- Athena views for common queries
- AWS Glue Data Quality rules

### **Phase 3: Machine Learning**
- SageMaker endpoints for real-time predictions
- Feature store for ML features
- Model registry for versioning

### **Phase 4: Advanced Features**
- AWS Lake Formation for fine-grained access control
- AWS Glue DataBrew for data profiling
- Amazon Managed Service for Apache Flink for advanced streaming
- API Gateway + Lambda for external data access

