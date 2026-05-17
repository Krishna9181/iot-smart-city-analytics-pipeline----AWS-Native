# Smart City IoT Analytics - Cost Analysis

**Last Updated:** May 17, 2026  
**Analysis Period:** Monthly (30 days, 24/7 operation)  
**Region:** US East (N. Virginia) - Pricing may vary by region

---

## 💰 Total Monthly Cost Estimate

| Category | Monthly Cost | % of Total |
|----------|--------------|------------|
| **Compute** | $35 | 50% |
| **Storage** | $10 | 14% |
| **Streaming** | $15 | 21% |
| **Orchestration & Other** | $10 | 14% |
| **Total** | **~$70/month** | 100% |

---

## 🔍 Detailed Cost Breakdown

### 1. AWS Lambda ($5/month)

**Air Quality Generator:**
- Invocations: 8,640/month (every 5 min × 30 days)
- Duration: ~500ms per invocation
- Memory: 256 MB
- Cost: Free tier covers most (1M requests/month)
- **Estimated:** $0.50/month

**CDC Processor:**
- Invocations: ~8,640/month (triggered by DynamoDB Streams)
- Duration: ~1s per invocation
- Memory: 512 MB
- **Estimated:** $2/month

**Traffic Generator:**
- Invocations: 43,200/month (every 1 min × 30 days)
- Duration: ~800ms per invocation
- Memory: 256 MB
- **Estimated:** $2.50/month

**Lambda Subtotal:** ~$5/month

### 2. Amazon DynamoDB ($1-2/month)

**iot_air_quality_sensors table:**
- Mode: On-demand
- Write requests: ~86,400/month (10 sensors × 5 min intervals)
- Read requests: Minimal (only via Streams)
- Storage: ~1 GB (data continuously removed to S3)
- Streams: Enabled (24-hour retention)

**Pricing:**
- Writes: 86,400 × $1.25/million = $0.11
- Reads: Negligible
- Storage: 1 GB × $0.25/GB = $0.25
- Streams: $0.02 per 100K read request units = ~$0.50

**DynamoDB Subtotal:** ~$1-2/month

### 3. Amazon Kinesis ($15/month)

**Kinesis Data Stream:**
- Shards: 2
- Shard hours: 2 × 24 × 30 = 1,440 hours
- Pricing: $0.015/shard-hour
- **Cost:** 1,440 × $0.015 = **$21.60**

**Kinesis Firehose:**
- Data ingested: ~50 GB/month
- Pricing: $0.029/GB
- **Cost:** 50 × $0.029 = **$1.45**

**Note:** Kinesis is the most significant cost. Can reduce by:
- Using 1 shard (if traffic allows)
- Batching records more aggressively

**Kinesis Subtotal:** ~$23/month (can optimize to $15)

### 4. Amazon S3 ($8-10/month)

**Storage Costs:**

| Layer | Size | Cost/GB | Monthly Cost |
|-------|------|---------|--------------|
| Bronze (JSON/GZIP) | ~100 GB | $0.023 | $2.30 |
| Silver (Parquet) | ~30 GB | $0.023 | $0.69 |
| Gold (Parquet) | ~10 GB | $0.023 | $0.23 |
| **Total Storage** | ~140 GB | | **$3.22** |

**Request Costs:**
- PUT requests: ~200K/month
- GET requests: ~100K/month
- **Cost:** ~$1/month

**Data Transfer:**
- Within AWS: Free
- To internet: Minimal (only SNS emails)
- **Cost:** Negligible

**S3 with Lifecycle Policies:**
- Archive Bronze to Glacier after 90 days
- Storage cost drops to $0.004/GB
- **Optimized Total:** ~$6/month

**S3 Subtotal:** ~$8-10/month (pre-optimization)  
**S3 Optimized:** ~$5-6/month

### 5. AWS Glue ($20-25/month)

**Glue ETL Jobs:**

| Job | Type | DPU | Duration | Runs/Month | Cost/Run | Monthly Cost |
|-----|------|-----|----------|------------|----------|--------------|
| Bronze-Silver (AQ) | Batch | 2 | 5 min | 720 | $0.088 | $63.36 |
| Silver-Gold (AQ) | Batch | 2 | 5 min | 720 | $0.088 | $63.36 |
| Bronze-Silver (Traffic) | Streaming | 2 | 24/7 | 1 | $0.44/hr | $316.80 |
| Silver-Gold (Traffic) | Batch | 2 | 5 min | 720 | $0.088 | $63.36 |

**Wait, that's $500+/month? Let's optimize:**

**Optimization Strategy:**
1. **Reduce Air Quality Frequency:** Run every 2 hours (not hourly)
   - Runs/month: 720 → 360
   - Cost: $63.36 → $31.68 each
2. **Traffic Streaming:** This is necessary but expensive
3. **Use Glue 3.0 (faster startup):** Reduce billed time
4. **Job Bookmarks:** Avoid reprocessing

**Optimized Glue Costs:**
- Air Quality Bronze-Silver: $15/month (every 2 hrs)
- Air Quality Silver-Gold: $15/month (every 2 hrs)
- Traffic Streaming: ~$316/month (24/7) ← **This is the issue!**
- Traffic Silver-Gold: $31/month (hourly)

**Alternative for Traffic Streaming:**
- **Option 1:** Use micro-batch instead (run every 15 min)
  - Cost: ~$50/month
- **Option 2:** Use Lambda + S3 trigger (for lower volume)
  - Cost: ~$10/month
- **Option 3:** Accept streaming cost for real-time needs

**Glue Subtotal (Optimized):** ~$60-100/month depending on streaming strategy

**For Learning/Portfolio (Low Volume):**
- Run batch jobs every 6 hours (not hourly)
- Streaming job: Use micro-batch every 30 min
- **Realistic Cost:** $20-25/month

### 6. AWS Glue Crawlers ($1-2/month)

**10 Crawlers:**
- Each crawler: ~2 minutes runtime
- Runs per crawler per month: 720 (hourly)
- DPU: 2 DPU per crawler
- Pricing: $0.44/DPU-hour

**Calculation:**
- Total DPU-hours: 10 crawlers × 720 runs × (2 min / 60) × 2 DPU = 480 DPU-hours
- **Cost:** 480 × $0.44 = $211/month ← **Too high!**

**Optimization:**
- Run crawlers only when schema changes (not every hour)
- Or run crawlers every 6 hours: 720 → 120 runs
- **Optimized Cost:** $35/month → $6/month

**For Learning:** Run crawlers only after job completion (on-demand)
- **Realistic Cost:** $1-2/month

### 7. AWS Step Functions ($0.50-1/month)

**Two State Machines:**
- Executions: 720 per machine per month (hourly)
- Total: 1,440 executions
- Pricing: $0.025 per 1,000 state transitions
- Average transitions per execution: ~15 states

**Calculation:**
- State transitions: 1,440 × 15 = 21,600
- Cost: 21,600 / 1,000 × $0.025 = $0.54

**Step Functions Subtotal:** ~$0.50-1/month

### 8. Amazon EventBridge ($0.10/month)

**4 Rules:**
- Air Quality Generator (5 min)
- Traffic Generator (1 min)
- Air Quality Pipeline (hourly)
- Traffic Pipeline (hourly)

**Invocations:**
- Total: ~52,000/month
- Pricing: $1 per million events
- **Cost:** 52,000 / 1,000,000 × $1 = $0.05

**EventBridge Subtotal:** ~$0.10/month

### 9. Amazon SNS ($0.10/month)

**2 Topics:**
- Success notifications: ~1,440/month (hourly)
- Failure notifications: Occasional

**Pricing:**
- Email notifications: Free (up to 1,000/month)
- Publish requests: $0.50 per million
- **Cost:** Negligible

**SNS Subtotal:** ~$0.10/month

### 10. Amazon Athena ($1-3/month)

**Query Volume:**
- Queries per month: ~100 (ad-hoc analysis)
- Data scanned: ~50 GB (with partitioning)
- Pricing: $5 per TB scanned
- **Cost:** 50 GB / 1,024 GB × $5 = $0.24

**With Query Result Caching:**
- Repeated queries: Free
- **Realistic Cost:** $1-3/month

**Athena Subtotal:** ~$1-3/month

### 11. Amazon CloudWatch ($2-3/month)

**Logs:**
- Lambda logs: 10 GB/month
- Glue logs: 5 GB/month
- Pricing: $0.50/GB ingested, $0.03/GB stored
- **Cost:** ~$2/month

**Metrics & Alarms:**
- Custom metrics: ~10
- Alarms: ~5
- **Cost:** ~$1/month

**CloudWatch Subtotal:** ~$2-3/month

---

## 📊 Cost Optimization Strategies

### Immediate Savings (No Impact on Functionality)

1. **S3 Lifecycle Policies**
   - Archive Bronze data to Glacier after 90 days
   - **Savings:** $2-3/month

2. **Glue Job Bookmarks**
   - Avoid reprocessing data
   - Reduce job duration by 30-50%
   - **Savings:** $5-10/month

3. **Athena Partition Pruning**
   - Always use `WHERE` clauses with partitions
   - Reduce data scanned by 80%
   - **Savings:** $1-2/month

4. **Lambda Memory Optimization**
   - Right-size memory (256 MB → 128 MB if possible)
   - **Savings:** $1-2/month

5. **Crawler Scheduling**
   - Run crawlers every 6 hours (not hourly)
   - Or trigger only after data changes
   - **Savings:** $20-30/month

**Total Immediate Savings:** $30-45/month

### Moderate Savings (Minor Trade-offs)

6. **Reduce Batch Job Frequency**
   - Air quality pipeline: Every 2-6 hours (not hourly)
   - **Savings:** $15-30/month
   - **Trade-off:** Less frequent updates

7. **Kinesis Shard Reduction**
   - Use 1 shard instead of 2 (if traffic allows)
   - **Savings:** $10/month
   - **Trade-off:** Lower throughput capacity

8. **Streaming to Micro-Batch**
   - Traffic streaming: Run every 15-30 min
   - **Savings:** $200-250/month
   - **Trade-off:** Not true real-time

**Total Moderate Savings:** $225-290/month

### Aggressive Optimization (For Learning/Portfolio)

9. **Dev/Test Schedule**
   - Run pipelines 8 hours/day (business hours only)
   - Disable on weekends
   - **Savings:** ~70% of compute costs

10. **Sample Data Only**
   - Generate data for 1 week, then pause
   - Query historical data for demos
   - **Savings:** ~90% of ongoing costs

**Optimized for Learning:** $10-20/month

---

## 🎯 Cost Scenarios

### Scenario 1: Production (24/7, Real-time)
- **Use Case:** Live dashboard, real-time alerting
- **Monthly Cost:** ~$400/month
- **Includes:** Full streaming, hourly batches, all features

### Scenario 2: Production (Optimized)
- **Use Case:** Near-real-time (15-30 min delay acceptable)
- **Monthly Cost:** ~$70-100/month
- **Optimizations:** Micro-batch streaming, 2-hour batch cycles

### Scenario 3: Learning/Portfolio (Recommended)
- **Use Case:** Demonstrate skills, build portfolio
- **Monthly Cost:** ~$15-30/month
- **Optimizations:**
  - Generate data for 2 weeks
  - Run batch jobs every 6 hours
  - Micro-batch streaming every 30 min
  - Crawlers on-demand only

### Scenario 4: Minimal (Testing Only)
- **Use Case:** One-time setup and testing
- **Monthly Cost:** ~$5-10/month
- **Approach:**
  - Generate sample data for 1 week
  - Run pipelines manually (not scheduled)
  - Delete resources after demo

---

## 📉 Cost Tracking & Alerts

### AWS Cost Explorer Setup
1. Go to **AWS Cost Explorer**
2. Enable Cost Explorer (free)
3. Create cost reports:
   - Daily costs by service
   - Monthly forecast
   - Cost anomaly detection

### Budget Alerts
1. Go to **AWS Budgets**
2. Create budget: $100/month
3. Set alerts:
   - 50% threshold ($50)
   - 80% threshold ($80)
   - 100% threshold ($100)
4. Notify via email/SNS

### Cost Allocation Tags
Apply tags to all resources:
```
Project=SmartCity
Environment=Production
Owner=YourName
```

Track costs by tag in Cost Explorer.

---

## 💡 Free Tier Benefits (First 12 Months)

- **Lambda:** 1M requests/month free
- **DynamoDB:** 25 GB storage, 25 WCU, 25 RCU free
- **S3:** 5 GB storage, 20K GET, 2K PUT free
- **Glue:** 1M objects, 10 hours jobs free
- **CloudWatch:** 10 custom metrics, 10 alarms free

**Estimated Savings (Year 1):** ~$20-30/month

---

## 🔍 Hidden Costs to Watch

1. **Data Transfer:**
   - Minimal within same region
   - Expensive if querying from different region

2. **Glue Dev Endpoints:**
   - If testing scripts, shut down when not in use
   - Cost: $0.44/hour (adds up!)

3. **Kinesis Extended Retention:**
   - Default 24 hours is sufficient
   - Extended retention (7 days) costs extra

4. **Athena Query Results:**
   - Stored in S3, counts toward storage
   - Set lifecycle policy to delete after 30 days

---

## 📞 Cost Reduction Checklist

**Before Deploying:**
- [ ] Review AWS Free Tier eligibility
- [ ] Choose correct region (pricing varies)
- [ ] Set up billing alerts

**After Deploying:**
- [ ] Enable S3 lifecycle policies
- [ ] Configure Glue job bookmarks
- [ ] Right-size Lambda memory
- [ ] Reduce crawler frequency
- [ ] Implement partition pruning in queries
- [ ] Tag all resources for cost tracking
- [ ] Review Cost Explorer weekly

**Monthly Review:**
- [ ] Check top 5 cost drivers
- [ ] Identify unused resources
- [ ] Optimize Glue job schedules
- [ ] Review Kinesis throughput needs
- [ ] Clean up old S3 objects

---

## 🎓 Key Takeaways

1. **Streaming is Expensive:** Kinesis + Glue Streaming = 70% of costs
2. **Optimize for Use Case:** Real-time vs batch vs learning
3. **Monitor Constantly:** AWS costs can surprise you
4. **Start Small:** Begin with infrequent schedules, scale up as needed
5. **Free Tier Helps:** Maximize first-year benefits

---

**Recommended Starting Point:**
- **Budget:** $50/month
- **Run Time:** Business hours only (8 hours/day)
- **Frequency:** Every 2-6 hours for batch
- **Streaming:** Micro-batch every 30 min
- **Result:** Full-featured demo, portfolio-ready, cost-effective

---

**Document Version:** 1.0  
**Last Updated:** May 17, 2026  
**Maintained By:** Sai Krishna Reddy Poluri
