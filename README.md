# 🌆 Smart City IoT Analytics Platform

> **Production-grade data engineering platform processing 100GB+ IoT sensor data monthly using AWS serverless architecture, dual ETL pipelines (batch CDC + streaming), and medallion data lake design.**

## 📖 Overview

Comprehensive **Smart City IoT Analytics Platform** built on AWS serverless technologies, demonstrating real-world data engineering patterns for processing air quality and traffic sensor data at scale.

### Key Highlights

* **🔄 Dual Pipeline Architecture:** Batch CDC (DynamoDB Streams) + Real-time streaming (Kinesis)
* **🏗️ Medallion Data Lake:** Bronze → Silver → Gold layers
* **⚡ Serverless & Event-Driven:** 12 AWS services, auto-scaling, pay-per-use
* **📊 8 Analytics Tables:** Hourly/daily aggregations, incident tracking, sensor health
* **🎯 Production-Ready:** Retry logic, error handling, SNS notifications
* **💰 Cost-Optimized:** ~$70/month 24/7, $15-30/month dev mode

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    SMART CITY IOT PLATFORM                      │
├────────────────────────────────────────────────────────────────┤
│  BATCH (Air Quality)          STREAMING (Traffic)              │
│  EventBridge → Lambda         EventBridge → Lambda             │
│       ↓                            ↓                            │
│  DynamoDB + Streams           Kinesis Stream                    │
│       ↓                            ↓                            │
│  Lambda CDC → S3              Firehose → S3                     │
│       ↓                            ↓                            │
│  Glue Batch ETL               Glue Streaming (24/7)            │
│       ↓                            ↓                            │
│  S3 Silver (Parquet)          S3 Silver (Parquet)              │
│       ↓                            ↓                            │
│  Glue Aggregation             Glue Batch                        │
│       ↓                            ↓                            │
│  4 Gold Tables                4 Gold Tables                     │
│       ↓                            ↓                            │
│  Step Functions → SNS Notifications                             │
└────────────────────────────────────────────────────────────────┘
```

📐 **Detailed Docs:** [Architecture](docs/ARCHITECTURE.md) | [Data Schemas](docs/DATA_SCHEMA.md) | [Setup Guide](docs/SETUP_GUIDE.md)

## 🛠️ Tech Stack

| Service | Purpose | Config |
|---------|---------|--------|
| **S3** | Data Lake | Bronze/Silver/Gold layers, 2 buckets |
| **DynamoDB** | Operational Store | Streams enabled, PAY_PER_REQUEST |
| **Kinesis** | Real-time Ingestion | 2 shards, Firehose GZIP |
| **Lambda** | Generators & CDC | 3 functions, Python 3.12 |
| **Glue** | ETL Processing | 4 jobs (batch + streaming) |
| **Step Functions** | Orchestration | 2 state machines, retry logic |
| **EventBridge** | Scheduling | 3 rules (1min, 5min, 1hr) |
| **SNS** | Notifications | Success/failure alerts |

## 📊 Gold Layer Tables (8)

**Air Quality:**
* `air_quality_hourly_avg` - Hourly sensor aggregations
* `air_quality_daily_summary` - Daily stats with AQI
* `sensor_health_metrics` - Device uptime, battery
* `aqi_trends` - Air Quality Index trends

**Traffic:**
* `traffic_hourly_by_intersection` - Intersection metrics
* `traffic_daily_summary` - Daily aggregates by road type
* `traffic_incidents` - Classified incidents with severity
* `traffic_congestion_trends` - 15-min congestion analysis

## 🚀 Quick Start

### Prerequisites
* AWS Account with IAM, S3, Glue, Lambda, DynamoDB, Kinesis permissions
* AWS CLI installed and configured
* Python 3.12, basic PySpark knowledge

### Setup
See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for 9-phase deployment guide.

Quick commands:
```bash
# Create buckets
aws s3 mb s3://smart-city-datalake-2026
aws s3 mb s3://smart-city-scripts-2026

# Upload Glue scripts
aws s3 cp "Glue Scripts/" s3://smart-city-scripts-2026/glue-scripts/ --recursive

# Deploy Lambda functions (zip and upload)
# Create DynamoDB, Kinesis, Step Functions (see setup guide)
```

## 💡 Sample Queries

**Air Quality Trends:**
```sql
SELECT reading_date, location, AVG(avg_aqi) as daily_aqi,
  CASE WHEN AVG(avg_aqi) <= 50 THEN 'Good'
       WHEN AVG(avg_aqi) <= 100 THEN 'Moderate'
       ELSE 'Unhealthy' END as category
FROM smart_city_gold.air_quality_daily_summary
WHERE reading_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY reading_date, location;
```

**Traffic Congestion Hotspots:**
```sql
SELECT intersection_id, location, COUNT(*) as high_congestion_periods
FROM smart_city_gold.traffic_congestion_trends
WHERE congestion_level = 'High'
  AND reading_datetime >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY intersection_id, location
ORDER BY high_congestion_periods DESC LIMIT 10;
```

## 📈 Project Metrics

| Metric | Value |
|--------|-------|
| Data Processed | 100+ GB/month |
| AWS Services | 12 integrated |
| Pipelines | 2 (CDC + streaming) |
| Lambda Functions | 3 |
| Glue Jobs | 4 |
| Gold Tables | 8 |
| Success Rate | 99%+ |
| Monthly Cost | $70 (24/7) |

💰 Full breakdown: [docs/COST_ANALYSIS.md](docs/COST_ANALYSIS.md)

## 🗂️ Repository Structure

```
smart-city-iot-analytics/
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
├── docs/                           # Complete documentation
├── Lambda Files/                   # 3 Python functions
├── Glue Scripts/                   # 4 ETL job configs
├── Step Functions State Machines/  # 2 orchestration workflows
└── Step Functions Graph Images/    # Visual diagrams
```

## 🚀 Future Enhancements

### Phase 2: BI Dashboards (Weeks 2-3)
* [ ] Amazon QuickSight real-time dashboards
* [ ] Executive reports with scheduled exports

### Phase 3: Machine Learning (Weeks 3-5)
* [ ] SageMaker traffic prediction models
* [ ] Air quality forecasting (Prophet/ARIMA)
* [ ] Anomaly detection for sensor failures

### Phase 4: Generative AI (Weeks 5-7)
* [ ] Amazon Bedrock natural language queries
* [ ] AI-powered incident summarization
* [ ] Chatbot for stakeholders

### Phase 5: Advanced
* [ ] AWS Lake Formation governance
* [ ] Managed Grafana operational dashboards
* [ ] DataZone cataloging
* [ ] Mobile app via API Gateway

## 📚 Documentation

* [Architecture Overview](docs/ARCHITECTURE.md) - Data flows, components, design decisions
* [Data Schemas](docs/DATA_SCHEMA.md) - Bronze/Silver/Gold table definitions
* [Setup Guide](docs/SETUP_GUIDE.md) - 9-phase deployment with troubleshooting
* [Cost Analysis](docs/COST_ANALYSIS.md) - Breakdown and optimization strategies
* [GitHub Push Guide](GITHUB_PUSH_GUIDE.md) - Step-by-step Git instructions
* [Project Summary](PROJECT_SUMMARY.md) - Quick reference for interviews

## 🛡️ Security

✅ IAM least-privilege roles  
✅ S3 encryption at rest  
✅ Secrets Manager for configs  
✅ CloudTrail audit logging  
✅ Cost monitoring alarms  

## 🐛 Troubleshooting

**Glue Job Fails:** Check S3 Bronze data, Lambda logs, DynamoDB/Kinesis streams  
**Step Functions Timeout:** Increase timeout (default 1800s), check Glue workers  
**Crawler Not Updating:** Verify timing, IAM permissions, S3 path patterns  
**High Costs:** Reduce Lambda frequency, lower Kinesis shards, stop streaming when idle  

Full guide: [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md)

## 📧 Contact

**Author:** Sai Krishna Reddy Poluri  
**Email:** polurisaikrishnareddy@gmail.com  
**LinkedIn:** [linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)  

## 📄 License

MIT License - see [LICENSE](LICENSE)

---

⭐ **Star this repo if helpful!**

**Built with ❤️ by Sai Krishna Reddy Poluri | May 2026**
