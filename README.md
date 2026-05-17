# 🌆 Smart City IoT Analytics Platform

> **End-to-end data engineering platform processing IoT sensor data using AWS serverless architecture, dual ETL pipelines (batch CDC + streaming), and medallion data lake design.**

## 📖 Overview

Comprehensive **Smart City IoT Analytics Platform** built on AWS serverless technologies, demonstrating real-world data engineering patterns for processing air quality and traffic sensor data at scale.

### Key Highlights

* **🔄 Dual Pipeline Architecture:** Batch CDC (DynamoDB Streams) + Real-time streaming (Kinesis)
* **🏗️ Medallion Data Lake:** Bronze → Silver → Gold layers
* **⚡ Serverless & Event-Driven:** 12 AWS services, auto-scaling, pay-per-use
* **📊 8 Analytics Tables:** Hourly/daily aggregations, incident tracking, sensor health
* **🎯 Production-Ready:** Retry logic, error handling, SNS notifications

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

📐 **Detailed Docs:** [Architecture](docs/ARCHITECTURE.md) | [Data Schemas](docs/DATA_SCHEMA.md)

## 🛠️ Tech Stack

| Service | Purpose | Config |
|---------|---------|--------|
| **S3** | Data Lake | Bronze/Silver/Gold layers, 2 buckets |
| **DynamoDB** | Operational Store | Streams enabled, PAY_PER_REQUEST |
| **Kinesis** | Real-time Ingestion | 2 shards, Firehose GZIP |
| **Lambda** | Generators & CDC | 3 functions, Python 3.12 |
| **Glue** | ETL Processing | 4 jobs (batch + streaming) |
| **Step Functions** | Orchestration | 2 state machines, retry logic |
| **EventBridge** | Scheduling | 3 rules (1min, 1min, 1hr) |
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

