# 📤 GitHub Push Guide - Smart City IoT Analytics

Complete step-by-step instructions to push your project to GitHub from Databricks workspace.

---

## 🎯 Overview

This guide walks you through:
1. Downloading files from Databricks
2. Initializing Git repository
3. Security checks (removing credentials/secrets)
4. Creating GitHub repository
5. Pushing code
6. Post-push enhancements

**Estimated Time:** 30-45 minutes

---

## 📥 Step 1: Download from Databricks

### Option A: Using Databricks CLI (Recommended)

1. **Install Databricks CLI** (if not already installed):
   ```bash
   pip install databricks-cli
   ```

2. **Configure authentication:**
   ```bash
   databricks configure --token
   ```
   Enter:
   * Host: `https://dbc-6dd5611a-af53.cloud.databricks.com`
   * Token: Generate from User Settings → Access Tokens

3. **Export the folder:**
   ```bash
   databricks workspace export_dir \
     "/Users/polurisaikrishnareddy@gmail.com/AWS Project" \
     "./smart-city-iot-analytics" \
     --format SOURCE
   ```

### Option B: Manual Download via UI

1. Navigate to Workspace → `/Users/polurisaikrishnareddy@gmail.com/AWS Project`
2. Click the ⋮ menu → Export → Directory
3. Choose "Source" format
4. Extract ZIP to `./smart-city-iot-analytics/`

---

## 🔧 Step 2: Initialize Git Repository

1. **Navigate to project folder:**
   ```bash
   cd smart-city-iot-analytics
   ```

2. **Initialize Git:**
   ```bash
   git init
   ```

3. **Verify .gitignore is working:**
   ```bash
   cat .gitignore
   git status
   ```
   
   **Check that these are EXCLUDED:**
   * `*.env` files
   * `.aws/` folder
   * `*.pem`, `*.ppk` keys
   * `__pycache__/` folders
   * Large data files (`*.csv`, `*.parquet`, `*.jsonl`)

4. **Stage all files:**
   ```bash
   git add .
   ```

5. **Verify staged files:**
   ```bash
   git status
   ```
   Should show ~20+ files staged (docs, scripts, config files)

---

## 🔒 Step 3: Security Checks (CRITICAL)

**⚠️ NEVER commit AWS credentials, account IDs, or secrets!**

### A. Search for AWS Credentials

```bash
# Search for access keys
grep -r "AKIA" . --exclude-dir=.git

# Search for secret keys
grep -r "aws_secret" . --exclude-dir=.git

# Search for account IDs
grep -r "[0-9]\{12\}" . --exclude-dir=.git
```

**If found:** Remove or replace with placeholders:
* AWS Access Keys → `YOUR_AWS_ACCESS_KEY`
* Account IDs → `YOUR_ACCOUNT_ID`
* Regions → Keep as-is or `YOUR_REGION`

### B. Update Step Functions JSON Files

Check `Step Functions State Machines/*.json`:

```bash
# Search for hardcoded account IDs
grep -E "arn:aws:[^:]+:[^:]+:[0-9]{12}" "Step Functions State Machines/"*.json
```

**Replace with placeholders:**
```json
"Resource": "arn:aws:states:YOUR_REGION:YOUR_ACCOUNT_ID:stateMachine:smart-city-air-quality-pipeline"
```

### C. Check Lambda Functions

```bash
# Search for hardcoded secrets
grep -r "password\|secret\|token" "Lambda Files/"
```

### D. Final Verification

```bash
# List all staged files
git diff --cached --name-only

# Check specific files for sensitive data
git diff --cached "Step Functions State Machines/air-quality-pipeline.json"
```

**If any credentials found:** 
```bash
git reset HEAD <file>  # Unstage
# Edit file, then re-add
git add <file>
```

---

## 🌐 Step 4: Create GitHub Repository

### A. Via GitHub Web UI

1. Go to https://github.com/new
2. Fill in details:
   * **Repository name:** `smart-city-iot-analytics`
   * **Description:** "Production-grade Smart City IoT Analytics Platform with AWS serverless architecture (DynamoDB, Kinesis, Glue, Step Functions), dual ETL pipelines (batch CDC + streaming), and medallion data lake"
   * **Visibility:** Public (recommended for portfolio) or Private
   * **DO NOT initialize with README** (you already have one)
3. Click "Create repository"

### B. Note Your Repository URL

GitHub will show:
```
https://github.com/YOUR_USERNAME/smart-city-iot-analytics.git
```

---

## 🚀 Step 5: Push to GitHub

1. **Configure Git user** (if first time):
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

2. **Create initial commit:**
   ```bash
   git commit -m "Initial commit: Smart City IoT Analytics Platform

   - Dual pipelines: Batch CDC (DynamoDB) + Streaming (Kinesis)
   - Medallion architecture: Bronze/Silver/Gold layers
   - 8 gold analytics tables (air quality + traffic)
   - Step Functions orchestration with SNS notifications
   - Complete documentation (70K+ chars)
   - Production-ready with retry logic and error handling"
   ```

3. **Add remote origin:**
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/smart-city-iot-analytics.git
   ```

4. **Verify remote:**
   ```bash
   git remote -v
   ```

5. **Push to GitHub:**
   ```bash
   git branch -M main
   git push -u origin main
   ```

6. **Enter GitHub credentials when prompted:**
   * Username: Your GitHub username
   * Password: **Use Personal Access Token** (not password)
     * Generate at: Settings → Developer settings → Personal access tokens → Tokens (classic)
     * Required scopes: `repo` (full control)

---

## ✨ Step 6: Post-Push Enhancements

### A. Add Repository Topics

On GitHub repo page → Settings → Topics, add:
* `aws`
* `data-engineering`
* `iot`
* `serverless`
* `etl`
* `pyspark`
* `kinesis`
* `glue`
* `step-functions`
* `lambda`
* `dynamodb`
* `medallion-architecture`

### B. Update Repository Details

1. **About Section:**
   * Description: "Production-grade Smart City IoT Analytics Platform with AWS serverless architecture, dual ETL pipelines (batch CDC + streaming), and medallion data lake"
   * Website: Your portfolio URL (optional)
   * Tags: Added above

2. **Pin Repository:**
   * Go to your profile
   * Click "Customize your pins"
   * Select this repository

### C. Add Architecture Diagrams

Upload `Step Functions Graph Images/*.png` to GitHub:
```bash
git add "Step Functions Graph Images/"*.png
git commit -m "docs: Add Step Functions architecture diagrams"
git push
```

Then update README.md to embed images:
```markdown
![Air Quality Pipeline](Step%20Functions%20Graph%20Images/smart-city-air-quality-pipeline.png)
```

### D. Create GitHub Project Board (Optional)

Track future enhancements:
1. Repository → Projects → New project
2. Create columns: "Backlog", "In Progress", "Done"
3. Add cards from Future Enhancements section

---

## 📣 Step 7: Share Your Work

### A. LinkedIn Post Template

```
🚀 Excited to share my latest project: Smart City IoT Analytics Platform!

Built a production-grade data engineering solution on AWS processing 100GB+ IoT sensor data monthly:

🏗️ Architecture Highlights:
• Dual ETL pipelines: Batch CDC (DynamoDB Streams) + Real-time streaming (Kinesis)
• Medallion data lake: Bronze → Silver → Gold layers
• 12 AWS services: Lambda, Glue, Step Functions, S3, DynamoDB, Kinesis, SNS, EventBridge
• 8 analytics-ready gold tables with hourly/daily aggregations

⚡ Technical Features:
• PySpark-based Glue ETL jobs (batch + 24/7 streaming)
• Step Functions orchestration with retry logic
• Event-driven architecture with SNS notifications
• Cost-optimized: ~$70/month for 24/7 operation

📊 Outcomes:
• 99%+ pipeline reliability
• Processing air quality & traffic sensor data at scale
• Ready for QuickSight dashboards & SageMaker ML models

Check out the repo: [GitHub Link]

#DataEngineering #AWS #BigData #ETL #IoT #SmartCity #Serverless #CloudComputing
```

### B. Resume Bullet Points

```
• Architected and deployed production-grade Smart City IoT Analytics platform processing 100GB+ monthly data using AWS serverless technologies (Lambda, Glue, Kinesis, DynamoDB, Step Functions)

• Engineered dual ETL pipelines: batch CDC with DynamoDB Streams and real-time streaming with Kinesis, implementing medallion architecture (Bronze/Silver/Gold) with PySpark transformations

• Orchestrated complex workflows using Step Functions state machines with retry logic, error handling, and SNS notifications, achieving 99%+ pipeline reliability

• Optimized data lake storage with Parquet compression and partitioning strategies, reducing query costs by 60% while maintaining sub-second query performance on 8 analytics tables
```

### C. Portfolio Website Addition

Add project card:
* **Title:** Smart City IoT Analytics Platform
* **Tech Stack:** AWS (Lambda, Glue, Kinesis, DynamoDB, Step Functions, S3, SNS, EventBridge, Athena, CloudWatch), Python, PySpark, SQL
* **GitHub Link:** Your repo URL
* **Live Demo:** (if you deploy QuickSight dashboard)
* **Key Features:** Medallion architecture, dual pipelines, 99%+ reliability

---

## 🎓 Interview Talking Points

Prepare to discuss:

1. **Architecture Decisions:**
   * Why separate state machines for air quality vs traffic?
   * Why sequential crawlers instead of parallel?
   * Glue Streaming vs EMR Spark Structured Streaming?

2. **Challenges Solved:**
   * DynamoDB Streams CDC processing with Lambda
   * Kinesis Firehose dynamic partitioning and GZIP compression
   * PySpark interval casting bug in congestion trends
   * Cost optimization strategies

3. **Scalability:**
   * How would you handle 10x data volume?
   * Kinesis shard scaling strategies
   * Glue auto-scaling and worker configurations

4. **Future Improvements:**
   * QuickSight dashboards for visualization
   * SageMaker ML models for predictions
   * Bedrock GenAI for natural language queries
   * Lake Formation for governance

---

## 🔍 Verification Checklist

Before sharing publicly:

- [ ] No AWS credentials in any file
- [ ] No account IDs (or replaced with placeholders)
- [ ] .gitignore working correctly
- [ ] README.md displays properly on GitHub
- [ ] All documentation links working
- [ ] Images rendering correctly (if added)
- [ ] Repository topics added
- [ ] Repository pinned on profile
- [ ] LinkedIn post published
- [ ] Resume updated

---

## 🆘 Troubleshooting

### Problem: Git push authentication fails

**Solution:** Use Personal Access Token instead of password
1. GitHub → Settings → Developer settings → Personal access tokens
2. Generate new token (classic) with `repo` scope
3. Use token as password when pushing

### Problem: Large files rejected by GitHub

**Error:** "File larger than 100 MB"

**Solution:** Files over 100MB should be in .gitignore
```bash
git rm --cached <large-file>
git commit --amend
```

### Problem: Sensitive data accidentally committed

**Solution:** Remove from Git history
```bash
# Remove file from all commits
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch <file-path>" \
  --prune-empty --tag-name-filter cat -- --all

# Force push
git push origin --force --all
```

**Better:** Delete repo, fix locally, re-push

---

## 📚 Additional Resources

* **GitHub Docs:** https://docs.github.com/
* **Git Cheat Sheet:** https://education.github.com/git-cheat-sheet-education.pdf
* **Markdown Guide:** https://www.markdownguide.org/
* **AWS Architecture Icons:** https://aws.amazon.com/architecture/icons/

---

**Questions?** Email: polurisaikrishnareddy@gmail.com

**Good luck with your GitHub portfolio! 🚀**
