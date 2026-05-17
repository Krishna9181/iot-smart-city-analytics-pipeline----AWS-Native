# Smart City IoT Analytics - Setup Guide

**Deployment Time:** ~2-3 hours for complete setup  
**Skill Level:** Intermediate AWS knowledge required  
**Cost:** ~$70/month (24/7 operation)

---

## 📋 Prerequisites

### AWS Account Requirements
- Active AWS account with admin or appropriate IAM permissions
- AWS CLI installed and configured
- Familiarity with AWS Console

### Required AWS Services Access
- AWS Lambda
- Amazon DynamoDB
- Amazon Kinesis (Data Streams + Firehose)
- AWS Glue (Jobs, Crawlers, Data Catalog)
- AWS Step Functions
- Amazon EventBridge
- Amazon S3
- Amazon SNS
- AWS IAM
- Amazon CloudWatch

### Local Development Tools
- Python 3.12+
- Text editor or IDE
- Git (for cloning repository)
- Optional: AWS SAM CLI

### Knowledge Prerequisites
- AWS services fundamentals
- Basic Python programming
- SQL and data concepts
- Understanding of ETL pipelines

---

## 🚀 Deployment Steps

### Phase 1: S3 Data Lake Setup (15 minutes)

#### Step 1.1: Create Data Lake Bucket
1. Go to **AWS S3 Console**
2. Click **Create bucket**
3. **Bucket name:** `smart-city-datalake-YYYY` (replace YYYY with current year)
4. **Region:** Choose your preferred region (e.g., us-east-1)
5. **Block Public Access:** Enable (keep all checkboxes checked)
6. **Bucket Versioning:** Disabled (optional: enable for production)
7. **Encryption:** Enable (SSE-S3)
8. Click **Create bucket**

#### Step 1.2: Create Folder Structure
Create these folders in the bucket:
- `bronze/cdc/air_quality/`
- `bronze/traffic-events/traffic/`
- `silver/air_quality/`
- `silver/traffic/`
- `gold/`
- `checkpoints/glue-streaming/`
- `athena-results/`

#### Step 1.3: Create Scripts Bucket
1. Create another bucket: `smart-city-scripts-YYYY`
2. Create folder: `glue-scripts/`

---

### Phase 2: DynamoDB Setup (10 minutes)

#### Step 2.1: Create Air Quality Table
1. Go to **DynamoDB Console**
2. Click **Create table**
3. **Table name:** `iot_air_quality_sensors`
4. **Partition key:** `sensor_id` (String)
5. **Sort key:** `reading_timestamp` (Number)
6. **Table settings:** On-demand capacity
7. Click **Create table**

#### Step 2.2: Enable DynamoDB Streams
1. Go to table → **Exports and streams** tab
2. Click **Turn on** under DynamoDB stream details
3. **View type:** New and old images
4. Click **Turn on stream**
5. **Copy the Stream ARN** (needed for Lambda trigger)

---

### Phase 3: Kinesis Setup (10 minutes)

#### Step 3.1: Create Kinesis Data Stream
1. Go to **Kinesis Console** → **Data streams**
2. Click **Create data stream**
3. **Name:** `smart-city-traffic-stream`
4. **Capacity mode:** Provisioned
5. **Provisioned shards:** 2
6. Click **Create data stream**

#### Step 3.2: Create Kinesis Firehose
1. Go to **Kinesis Console** → **Delivery streams**
2. Click **Create delivery stream**
3. **Source:** Amazon Kinesis Data Streams
4. **Kinesis data stream:** Select `smart-city-traffic-stream`
5. **Delivery stream name:** `smart-city-traffic-firehose`
6. **Destination:** Amazon S3
7. **S3 bucket:** Select your data lake bucket
8. **S3 prefix:** `bronze/traffic-events/traffic/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`
9. **Error prefix:** `errors/traffic/`
10. **Buffer size:** 5 MB
11. **Buffer interval:** 300 seconds
12. **Compression:** GZIP
13. **Dynamic partitioning:** Enabled
14. Click **Create delivery stream**

---

### Phase 4: Lambda Functions (30 minutes)

#### Step 4.1: Create IAM Execution Roles

**Role 1: Air Quality Generator**
- Service: Lambda
- Permissions: 
  - `AWSLambdaBasicExecutionRole`
  - Inline policy: DynamoDB `PutItem` on `iot_air_quality_sensors`

**Role 2: CDC Processor**
- Service: Lambda
- Permissions:
  - `AWSLambdaBasicExecutionRole`
  - `AWSLambdaDynamoDBExecutionRole`
  - Inline policy: S3 `PutObject` on data lake bucket

**Role 3: Traffic Generator**
- Service: Lambda
- Permissions:
  - `AWSLambdaBasicExecutionRole`
  - Inline policy: Kinesis `PutRecords` on traffic stream

#### Step 4.2: Deploy Lambda Functions

**Lambda 1: Air Quality Generator**
1. Go to **Lambda Console** → **Create function**
2. **Name:** `smart-city-air-quality-generator`
3. **Runtime:** Python 3.12
4. **Execution role:** Use existing role (from Step 4.1)
5. **Code:** Upload from `Lambda Files/smart-city-iot-simulator.py`
6. **Memory:** 256 MB
7. **Timeout:** 5 minutes
8. **Environment variables:**
   - `DYNAMODB_TABLE`: `iot_air_quality_sensors`

**Lambda 2: CDC Processor**
1. Create function: `smart-city-cdc-processor`
2. **Runtime:** Python 3.12
3. **Code:** Upload from `Lambda Files/smart-city-cdc-processor.py`
4. **Memory:** 512 MB
5. **Timeout:** 5 minutes
6. **Trigger:** Add DynamoDB trigger (use Stream ARN from Phase 2)
7. **Environment variables:**
   - `S3_BUCKET`: Your data lake bucket
   - `S3_PREFIX`: `bronze/cdc/air_quality/`

**Lambda 3: Traffic Generator**
1. Create function: `traffic-data-generator-kinesis`
2. **Runtime:** Python 3.12
3. **Code:** Upload from `Lambda Files/traffic-data-generator-kinesis.py`
4. **Memory:** 256 MB
5. **Timeout:** 5 minutes
6. **Environment variables:**
   - `KINESIS_STREAM_NAME`: `smart-city-traffic-stream`

---

### Phase 5: AWS Glue Setup (45 minutes)

#### Step 5.1: Create Glue Service Role
1. Go to **IAM Console** → **Roles** → **Create role**
2. **Service:** Glue
3. **Policies:** `AWSGlueServiceRole`
4. **Inline policy:** S3 full access to both buckets
5. **Role name:** `AWSGlueServiceRole-SmartCity`

#### Step 5.2: Upload Glue Scripts
```bash
aws s3 cp "Glue Scripts/" s3://smart-city-scripts-YYYY/glue-scripts/ --recursive
```

#### Step 5.3: Create Glue Databases
1. Go to **Glue Console** → **Databases**
2. Create three databases:
   - `smart_city_bronze`
   - `smart_city_silver`
   - `smart_city_gold`

#### Step 5.4: Create Glue Jobs

**Job 1: Air Quality Bronze to Silver**
- **Name:** `job-bronze-to-silver-air-quality`
- **Type:** Spark (Python)
- **Glue version:** 4.0
- **Language:** Python 3
- **Worker type:** G.1X
- **Number of workers:** 2
- **Script path:** `s3://smart-city-scripts-YYYY/glue-scripts/[script-name].py`
- **IAM role:** AWSGlueServiceRole-SmartCity

**Job 2: Air Quality Silver to Gold**
- **Name:** `job-silver-to-gold-air-quality`
- Same configuration as Job 1
- Different script

**Job 3: Traffic Streaming Bronze to Silver**
- **Name:** `traffic-streaming-bronze-to-silver`
- **Type:** Streaming (Glue Streaming)
- Same configuration as above
- **Additional:** Enable job bookmarks

**Job 4: Traffic Silver to Gold**
- **Name:** `traffic-silver-to-gold`
- **Type:** Spark (Batch)
- Same configuration

#### Step 5.5: Create Glue Crawlers

Create 10 crawlers total with these settings:
- **IAM role:** AWSGlueServiceRole-SmartCity
- **Schedule:** None (triggered by Step Functions)
- **Configuration:** Add new tables only

**Silver Crawlers:**
1. `crawler-silver-air-quality` → Database: smart_city_silver, Path: s3://bucket/silver/air_quality/
2. `crawler-silver-traffic-events` → Database: smart_city_silver, Path: s3://bucket/silver/traffic/

**Gold Crawlers (Air Quality):**
3. `crawler-gold-air-quality-hourly-avg`
4. `crawler-gold-air-quality-daily-summary`
5. `crawler-gold-air-quality-sensor-health-metrics`
6. `crawler-gold-air-quality-aqi-trends`

**Gold Crawlers (Traffic):**
7. `crawler-gold-traffic-hourly-interactions`
8. `crawler-gold-traffic-daily-summary`
9. `crawler-gold-traffic-incidents`
10. `crawler-gold-traffic-congestion-trends`

---

### Phase 6: SNS Notifications (10 minutes)

#### Step 6.1: Create SNS Topics
1. Go to **SNS Console** → **Topics**
2. Create topic: `smart-city-pipeline-success`
   - Type: Standard
3. Create topic: `smart-city-pipeline-failure`
   - Type: Standard

#### Step 6.2: Subscribe Email
1. For each topic, click **Create subscription**
2. Protocol: **Email**
3. Endpoint: Your email address
4. Click **Create subscription**
5. **Confirm subscription** via email

---

### Phase 7: Step Functions Orchestration (30 minutes)

#### Step 7.1: Create IAM Roles

**Role for Air Quality Pipeline:**
- Name: `StepFunctions-AirQuality-Pipeline-Role`
- Trust policy: Step Functions
- Permissions: See `infrastructure/iam-policies/stepfunctions-air-quality-role.json`

**Role for Traffic Pipeline:**
- Name: `StepFunctions-Traffic-Pipeline-Role`
- Trust policy: Step Functions
- Permissions: See `infrastructure/iam-policies/stepfunctions-traffic-role.json`

#### Step 7.2: Create State Machines

**State Machine 1: Air Quality**
1. Go to **Step Functions Console**
2. Click **Create state machine**
3. **Authoring method:** Write workflow in code
4. **Type:** Standard
5. **Definition:** Copy from `Step Functions State Machines/air-quality-pipeline.json`
6. **Update placeholders:** YOUR_REGION, YOUR_ACCOUNT_ID, SNS ARNs
7. **Name:** `smart-city-air-quality-pipeline`
8. **Execution role:** StepFunctions-AirQuality-Pipeline-Role
9. Click **Create state machine**

**State Machine 2: Traffic**
- Same steps, use traffic-pipeline.json
- Name: `smart-city-traffic-pipeline`

---

### Phase 8: EventBridge Scheduling (15 minutes)

#### Step 8.1: Create Air Quality Schedule
1. Go to **EventBridge Console** → **Rules**
2. Click **Create rule**
3. **Name:** `air-quality-pipeline-hourly-trigger`
4. **Rule type:** Schedule
5. **Schedule pattern:** Rate expression → Every 1 Hours
6. **Target:** Step Functions state machine → smart-city-air-quality-pipeline
7. **Execution role:** Create new role
8. **Configure input:** Constant JSON: `{"triggered_by": "EventBridge"}`
9. Click **Create rule**

#### Step 8.2: Create Traffic Schedule
- Same steps
- Name: `traffic-pipeline-hourly-trigger`
- Target: smart-city-traffic-pipeline

#### Step 8.3: Create Data Generator Schedules

**Schedule 1: Air Quality Generator**
- Name: `air-quality-generator-5min`
- Schedule: Rate → Every 5 Minutes
- Target: Lambda → smart-city-air-quality-generator

**Schedule 2: Traffic Generator**
- Name: `traffic-generator-1min`
- Schedule: Rate → Every 1 Minute
- Target: Lambda → traffic-data-generator-kinesis

---

### Phase 9: Testing & Validation (20 minutes)

#### Step 9.1: Test Lambda Functions
1. Go to each Lambda function
2. Click **Test** → Create test event
3. Invoke and verify CloudWatch Logs
4. Check DynamoDB/Kinesis for data

#### Step 9.2: Run Glue Jobs Manually
1. Start traffic streaming job (will run continuously)
2. Wait for Bronze data to accumulate (10-15 minutes)
3. Run air quality batch jobs manually
4. Verify S3 Silver/Gold folders

#### Step 9.3: Test Step Functions
1. Go to each state machine
2. Click **Start execution**
3. Input: `{}`
4. Monitor execution graph
5. Verify SNS email notifications

#### Step 9.4: Verify Data Catalog
1. Go to **Glue Console** → **Databases**
2. Check that all 3 databases have tables
3. Expected: 2 silver + 8 gold tables

#### Step 9.5: Query with Athena
1. Go to **Athena Console**
2. Select database: `smart_city_gold`
3. Run sample queries (see repo athena/ folder)
4. Verify data is queryable

---

## 🔧 Configuration Reference

### Environment Variables Summary

**Lambda - Air Quality Generator:**
```
DYNAMODB_TABLE=iot_air_quality_sensors
SENSOR_COUNT=10
```

**Lambda - CDC Processor:**
```
S3_BUCKET=smart-city-datalake-YYYY
S3_PREFIX=bronze/cdc/air_quality/
```

**Lambda - Traffic Generator:**
```
KINESIS_STREAM_NAME=smart-city-traffic-stream
SENSOR_COUNT=15
```

### AWS Resource Names (Update for Your Setup)

| Resource Type | Name Pattern | Example |
|---------------|--------------|---------|
| S3 Buckets | smart-city-*-YYYY | smart-city-datalake-2026 |
| DynamoDB Tables | iot_* | iot_air_quality_sensors |
| Kinesis Streams | smart-city-*-stream | smart-city-traffic-stream |
| Lambda Functions | smart-city-* | smart-city-air-quality-generator |
| Glue Jobs | job-* | job-bronze-to-silver-air-quality |
| Glue Crawlers | crawler-* | crawler-silver-air-quality |
| Step Functions | smart-city-*-pipeline | smart-city-air-quality-pipeline |
| SNS Topics | smart-city-pipeline-* | smart-city-pipeline-success |
| EventBridge Rules | *-trigger | air-quality-pipeline-hourly-trigger |

---

## 🐛 Troubleshooting

### Issue: Lambda Function Times Out
**Symptoms:** Function exceeds 5-minute timeout  
**Solutions:**
- Increase timeout to 10 minutes
- Reduce batch size in generator
- Check network connectivity to AWS services

### Issue: Glue Job Fails with "Table Not Found"
**Symptoms:** Glue job can't read from Glue Catalog  
**Solutions:**
- Run crawler first to create table
- Verify database and table names
- Check IAM permissions for Glue role

### Issue: Step Functions Execution Hangs
**Symptoms:** State machine stuck in "Running"  
**Solutions:**
- Check Glue job is actually running (Glue Console)
- Verify crawler isn't already running
- Check CloudWatch Logs for errors
- Increase timeout in state machine definition

### Issue: No Data in Gold Tables
**Symptoms:** Athena queries return 0 rows  
**Solutions:**
- Verify Lambda generators are creating data
- Check S3 Bronze folder has files
- Run Glue jobs manually to test
- Check Glue job logs in CloudWatch

### Issue: Crawler Naming Issues
**Symptoms:** Silver traffic table named "traffic_year_2026"  
**Solutions:**
- This is expected (Glue auto-naming from partitions)
- Update Glue job scripts to reference correct table name
- Or rename table manually in Data Catalog

---

## 🔐 Security Best Practices

### IAM Principles
- ✅ Use least privilege access
- ✅ Separate roles for each service
- ✅ No hardcoded credentials in code
- ✅ Use IAM roles, not access keys

### S3 Security
- ✅ Enable bucket encryption (SSE-S3)
- ✅ Block public access
- ✅ Enable access logging (optional)
- ✅ Use bucket policies for cross-account access

### Network Security
- ✅ Use VPC endpoints for AWS services (optional)
- ✅ Enable CloudTrail for audit logs
- ✅ Monitor with CloudWatch

---

## 📊 Monitoring Setup

### CloudWatch Alarms (Optional but Recommended)

**Lambda Alarms:**
- Invocation errors > 5 in 5 minutes
- Duration > 4 minutes (near timeout)

**Glue Job Alarms:**
- Job failures > 1 in 1 hour
- Job duration > 45 minutes

**Step Functions Alarms:**
- Execution failures > 1 in 1 hour
- Executions timing out

**Kinesis Alarms:**
- Iterator age > 10 minutes
- Throttled records > 100

---

## 💰 Cost Optimization Tips

1. **Use Glue Job Bookmarks:** Avoid reprocessing data
2. **Right-size Glue Workers:** Start with 2, scale if needed
3. **S3 Lifecycle Policies:** Archive Bronze data to Glacier after 90 days
4. **Kinesis Auto-scaling:** Adjust shards based on traffic
5. **Lambda Memory:** Start small, increase if needed
6. **Athena Query Optimization:** Use partitions, limit scans

---

## 🎓 Next Steps

After successful deployment:

1. **Week 1-2:** Monitor pipelines, validate data quality
2. **Week 2-3:** Build Athena views and QuickSight dashboards
3. **Week 3-4:** Implement data quality rules with Glue Data Quality
4. **Week 4-5:** Develop ML models with SageMaker
5. **Week 5-6:** Integrate Bedrock for NLP queries

---

## 📞 Support & Resources

- **AWS Documentation:** https://docs.aws.amazon.com/
- **Project Issues:** [GitHub Issues]
- **Contact:** polurisaikrishnareddy@gmail.com

---

**Document Version:** 1.0  
**Last Updated:** May 17, 2026
