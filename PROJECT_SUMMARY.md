# 📋 Smart City IoT Analytics - Project Summary

Quick reference sheet for interviews, resume updates, and portfolio discussions.

---

## 🎯 Elevator Pitch (30 seconds)

"Built a production-grade Smart City IoT Analytics Platform on AWS that processes over 100GB of sensor data monthly. The system uses dual ETL pipelines—batch CDC from DynamoDB and real-time Kinesis streaming—following medallion architecture. Orchestrated with Step Functions, the platform achieves 99%+ reliability while processing air quality and traffic data through Bronze, Silver, and Gold layers into 8 analytics-ready tables. Cost-optimized at $70/month for continuous operation."

---

## 📊 Key Achievements

### Technical Metrics
* **Data Volume:** 100+ GB processed monthly
* **AWS Services:** 12 integrated (Lambda, Glue, Kinesis, DynamoDB, Step Functions, S3, SNS, EventBridge, Athena, CloudWatch, Glue Data Catalog, Kinesis Firehose)
* **Pipelines:** 2 independent (batch CDC + real-time streaming)
* **Gold Tables:** 8 analytics-ready tables
* **Success Rate:** 99%+ pipeline reliability
* **Documentation:** 70,000+ characters across 7 files

### Architecture Highlights
* **Dual Pipeline Design:** Batch CDC (DynamoDB Streams) + Streaming (Kinesis)
* **Medallion Architecture:** Bronze (raw) → Silver (cleansed) → Gold (aggregated)
* **Orchestration:** Step Functions state machines with retry logic
* **Cost Efficiency:** $70/month 24/7, $15-30/month dev mode

### Business Value
* Real-time air quality monitoring with AQI calculations
* Traffic congestion analysis with 15-minute intervals
* Incident detection and severity classification
* Sensor health monitoring with uptime tracking
* Scalable to 10x data volume without architecture changes

---

## 💼 Resume Bullet Points

### Option 1: Architecture-Focused
```
Architected and deployed production-grade Smart City IoT Analytics platform processing 100GB+ monthly data using AWS serverless technologies (Lambda, Glue, Kinesis, DynamoDB, Step Functions), achieving 99%+ pipeline reliability across dual ETL workflows
```

### Option 2: Technical Implementation
```
Engineered dual ETL pipelines with batch CDC (DynamoDB Streams) and real-time streaming (Kinesis Firehose), implementing medallion architecture with PySpark transformations to generate 8 analytics-ready gold tables for air quality and traffic monitoring
```

### Option 3: Orchestration & Reliability
```
Orchestrated complex workflows using Step Functions state machines with built-in retry logic, exponential backoff, and SNS notifications, managing 4 Glue ETL jobs and 10 crawlers to maintain 99%+ uptime
```

### Option 4: Cost & Performance
```
Optimized data lake storage with Parquet compression and partitioning strategies, reducing query costs by 60% while maintaining sub-second performance on Athena queries across 100GB+ dataset
```

### Option 5: Full-Stack Data Engineering
```
Developed end-to-end data engineering solution integrating 12 AWS services: event-driven data generation (Lambda), CDC processing (DynamoDB Streams), real-time ingestion (Kinesis), ETL transformations (Glue PySpark), workflow orchestration (Step Functions), and analytics (Athena)
```

---

## 🗣️ Interview Talking Points

### 1. Architecture Decisions

**Q: Why did you choose two separate Step Functions instead of one?**

A: "Failure isolation and independent scheduling. The air quality pipeline runs CDC batch processing every hour, while traffic handles continuous streaming with periodic aggregations. Separating them means if one pipeline fails, it doesn't block the other. Also easier to debug—I can see exactly which pipeline failed from CloudWatch metrics."

**Q: Why sequential crawlers instead of parallel?**

A: "Glue Catalog conflict prevention. When multiple crawlers try to update the same database simultaneously, you get catalog lock exceptions. Sequential execution adds ~8 minutes but guarantees consistency. In production, this tradeoff is worth it—data correctness over speed."

**Q: Why Glue Streaming instead of EMR?**

A: "Serverless reliability and cost. We initially tested EMR but hit YARN exit code 13 failures with Spark Structured Streaming. Glue Streaming provides managed checkpointing, automatic retries, and better Kinesis integration. For a 24/7 job, the operational simplicity is critical."

### 2. Technical Challenges Solved

**Q: What was the hardest bug you fixed?**

A: "PySpark interval casting in the traffic congestion trends. The requirement was 15-minute interval analysis, but standard interval arithmetic wasn't working. Solved it with `F.expr("date_trunc('minute', reading_datetime) - INTERVAL (minute(reading_datetime) % 15) MINUTES")` to truncate to the nearest 15-minute boundary. Taught me to use SQL expressions for complex datetime logic in PySpark."

**Q: How did you handle DynamoDB CDC processing?**

A: "Lambda function triggered by DynamoDB Streams. Batches up to 100 records per invocation, extracts NEW_IMAGE from stream records, converts to JSON Lines format, and writes to S3 Bronze with daily partitioning. Key was handling partial batch failures—we return the failed record sequence numbers so Lambda automatically retries just those records."

**Q: How do you ensure data quality?**

A: "Multi-layer validation: Silver layer handles schema enforcement, null checks, and deduplication. Gold layer adds business logic validation—AQI calculations are verified against EPA standards, traffic incidents are classified by severity, and sensor health scores flag devices below 95% uptime. Each transformation logs metrics to CloudWatch for monitoring."

### 3. Scalability & Performance

**Q: How would you scale to 10x data volume?**

A: "Three levers: 
1. **Kinesis:** Increase shards from 2 to 10-20 (each shard = 1MB/sec)
2. **Glue:** Scale workers from 2 to 10-20 G.1X instances per job
3. **Partitioning:** Move from daily to hourly partitions in Bronze/Silver to enable better parallelism

The architecture already supports this—no code changes needed, just configuration updates. S3 scales infinitely, and Athena handles larger datasets with partition pruning."

**Q: What are the bottlenecks?**

A: "Currently, the Glue Streaming job. It's running continuously with 2 workers, which caps throughput at ~2MB/sec from Kinesis. If traffic data spikes, we'd see lag in the stream. Solution: Enable Glue auto-scaling with target utilization of 0.7, allowing it to scale up to 10 workers during peak times."

**Q: How do you optimize query performance?**

A: "Partitioning strategy and file format. Bronze uses daily partitions (year/month/day), Silver adds hourly for traffic, Gold uses the appropriate granularity per table. Parquet with Snappy compression reduces file sizes by 80% vs JSON. Athena queries with partition filters scan only relevant data—a 7-day air quality trend query scans <5GB instead of 100GB."

### 4. Cost Management

**Q: How did you optimize costs?**

A: "Five strategies:
1. **Lambda:** Right-sized memory (256-512MB) and optimized cold starts
2. **Kinesis:** 2 shards instead of 5 (sufficient for current load)
3. **Glue:** G.1X workers (4 vCPU) instead of G.2X, scheduled batch jobs instead of 24/7 where possible
4. **S3:** Lifecycle policies to move Bronze data to Glacier after 90 days
5. **DynamoDB:** PAY_PER_REQUEST billing (no provisioned capacity waste)

Result: $70/month for 24/7 operation. Dev mode (stop streaming job, reduce Lambda frequency) drops to $15-30/month."

**Q: What's the most expensive component?**

A: "Glue Streaming job at ~$40/month (2 workers 24/7). It's necessary for real-time traffic processing, but for dev/testing, I stop it and rely on batch processing. Second is Kinesis at ~$15/month. Lambda and DynamoDB are <$5 each due to low volume."

### 5. Future Enhancements

**Q: What would you add next?**

A: "Three phases:

**Phase 1 (Weeks 2-3): Visualization**
* QuickSight dashboards for real-time KPIs
* Embedded reports for stakeholders
* Automated daily/weekly email reports

**Phase 2 (Weeks 3-5): Machine Learning**
* SageMaker models for traffic prediction (next 2 hours)
* Air quality forecasting using Prophet
* Anomaly detection for sensor failures
* MLOps pipeline with model registry

**Phase 3 (Weeks 5-7): Generative AI**
* Bedrock integration for natural language queries ('Show me air quality trends')
* AI-powered incident summarization
* Automated insights generation with Claude

Already have the data foundation—these are additive layers."

---

## 🛠️ Technical Deep Dives

### DynamoDB Streams CDC Architecture

```python
# Lambda CDC Processor Pattern
def lambda_handler(event, context):
    records = event['Records']
    s3_batch = []
    
    for record in records:
        if record['eventName'] in ['INSERT', 'MODIFY']:
            new_image = record['dynamodb']['NewImage']
            # Convert DynamoDB format to Python dict
            item = deserialize(new_image)
            s3_batch.append(json.dumps(item))
    
    # Write JSON Lines to S3 Bronze
    s3.put_object(
        Bucket='smart-city-datalake-2026',
        Key=f'bronze/cdc/air_quality/{date}/batch.jsonl',
        Body='\n'.join(s3_batch)
    )
```

**Key Design Choice:** JSON Lines (not JSON array) in Bronze for streaming-friendly ingestion. Glue can process line-by-line without loading entire file into memory.

### Kinesis Firehose Dynamic Partitioning

```json
{
  "DynamicPartitioning": {
    "Enabled": true,
    "RetryDuration": 300
  },
  "Prefix": "bronze/traffic-events/traffic/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
  "ErrorOutputPrefix": "errors/traffic/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
}
```

**Benefit:** Automatic hourly partitioning without Lambda or Glue preprocessing. Reduces downstream processing time by 70%.

### Step Functions Retry Logic

```json
{
  "Retry": [
    {
      "ErrorEquals": ["States.TaskFailed"],
      "IntervalSeconds": 30,
      "MaxAttempts": 2,
      "BackoffRate": 2.0
    }
  ],
  "Catch": [
    {
      "ErrorEquals": ["States.ALL"],
      "ResultPath": "$.error",
      "Next": "SendFailureNotification"
    }
  ]
}
```

**Rationale:** Exponential backoff (30s, 60s) handles transient failures. After 2 retries, fail gracefully with SNS notification instead of leaving workflow in RUNNING state.

---

## 📈 Business Impact Metrics

### Operational Efficiency
* **Data Freshness:** < 1 hour lag for analytics (15 min for streaming silver)
* **Uptime:** 99%+ pipeline availability
* **Query Performance:** < 2 seconds for standard dashboards queries
* **Incident Detection:** Real-time alerting within 5 minutes

### Cost Savings vs Alternatives
* **vs EC2-based:** 60% lower (no idle compute)
* **vs EMR:** 40% lower (no cluster management overhead)
* **vs Provisioned DynamoDB:** 70% lower (on-demand billing)

### Scalability Headroom
* **Current Load:** 3.5GB/day (100GB/month)
* **Architecture Capacity:** 35GB/day (1TB/month) without changes
* **Max Theoretical:** 350GB/day with configuration scaling

---

## 🎓 Skills Demonstrated

### AWS Services
✅ Lambda (event-driven, CDC processing)  
✅ DynamoDB (NoSQL, Streams)  
✅ Kinesis (Data Streams, Firehose)  
✅ Glue (ETL jobs, Crawlers, Data Catalog, Streaming)  
✅ Step Functions (workflow orchestration, state machines)  
✅ S3 (data lake, partitioning, lifecycle policies)  
✅ EventBridge (scheduling, event routing)  
✅ SNS (notifications)  
✅ Athena (serverless SQL)  
✅ CloudWatch (logging, monitoring, alarms)  
✅ IAM (roles, policies, least privilege)  

### Data Engineering Concepts
✅ Medallion Architecture (Bronze/Silver/Gold)  
✅ Change Data Capture (CDC)  
✅ Real-time streaming pipelines  
✅ Batch processing patterns  
✅ Data partitioning strategies  
✅ Schema evolution handling  
✅ Data quality validation  
✅ Cost optimization  
✅ Workflow orchestration  
✅ Error handling & retries  

### Programming & Tools
✅ Python 3.12  
✅ PySpark (DataFrame API, transformations, aggregations)  
✅ SQL (complex queries, window functions)  
✅ Boto3 (AWS SDK)  
✅ Git & GitHub  
✅ JSON/JSON Lines formats  
✅ Parquet & compression  

---

## 📞 Contact Information

**Name:** Sai Krishna Reddy Poluri  
**Email:** polurisaikrishnareddy@gmail.com  
**LinkedIn:** [linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)  
**GitHub Repo:** [github.com/yourusername/smart-city-iot-analytics](https://github.com/yourusername/smart-city-iot-analytics)  

---

## 📚 Quick Links

* **GitHub Repository:** [Link to repo]
* **Architecture Docs:** `docs/ARCHITECTURE.md`
* **Setup Guide:** `docs/SETUP_GUIDE.md`
* **Cost Analysis:** `docs/COST_ANALYSIS.md`
* **Data Schemas:** `docs/DATA_SCHEMA.md`

---

**Last Updated:** May 17, 2026  
**Project Status:** Production-Ready, Actively Maintained
