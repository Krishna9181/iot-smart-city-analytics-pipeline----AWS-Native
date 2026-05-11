# IOT-smart-city-analytics-pipeline----AWS-Native
graph TB
    subgraph "Data Sources"
        IoT[🌡️ IoT Sensors<br/>Air Quality & Traffic]
        API[🔌 External APIs<br/>Weather & Events]
    end

    subgraph "Data Ingestion Layer"
        EB[📅 EventBridge<br/>Scheduled Events]
        L1[⚡ Lambda<br/>Data Collector]
        DDB[🗄️ DynamoDB<br/>Sensor Registry]
        DS[🌊 DynamoDB Streams]
        L2[⚡ Lambda<br/>Stream Processor]
        KDS[🌊 Kinesis Data Streams]
        KDF[🔥 Kinesis Data Firehose]
    end

    subgraph "Storage Layer - Bronze"
        S3B[📦 S3 Bronze Layer<br/>Raw JSON Data<br/>Partitioned by date/hour]
    end

    subgraph "Processing Layer"
        GC[🕷️ Glue Crawler<br/>Schema Discovery]
        GDB[📊 Glue Data Catalog<br/>Metadata Store]
        
        subgraph "ETL Jobs"
            GJ1[⚙️ Batch ETL Job<br/>Bronze → Silver<br/>SCD Type 2]
            GJ2[⚙️ Streaming ETL Job<br/>Real-time Analytics<br/>Kinesis → Silver]
            GJ3[⚙️ Aggregation Job<br/>Silver → Gold<br/>Business KPIs]
        end
        
        SF[🔄 Step Functions<br/>Orchestration]
    end

    subgraph "Storage Layer - Silver"
        S3S[📦 S3 Silver Layer<br/>Cleaned Parquet<br/>Partitioned & Optimized]
    end

    subgraph "Storage Layer - Gold"
        S3G[📦 S3 Gold Layer<br/>Business Aggregations<br/>ML Features & Alerts]
    end

    subgraph "Analytics & ML Layer"
        ATH[🔍 Amazon Athena<br/>SQL Analytics]
