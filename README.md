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
        RS[🏢 Amazon Redshift<br/>Data Warehouse]
        SM[🤖 SageMaker<br/>ML Training & Inference]
        BR[🧠 Amazon Bedrock<br/>GenAI Analytics]
    end

    subgraph "Visualization & Monitoring"
        QS[📊 QuickSight<br/>Dashboards & Reports]
        CW[📈 CloudWatch<br/>Monitoring & Alerts]
        SNS[📧 SNS<br/>Notifications]
    end

    subgraph "Security & Governance"
        IAM[🔐 IAM Roles & Policies]
        KMS[🔑 KMS Encryption]
        LF[🏛️ Lake Formation<br/>Data Governance]
    end

    %% Data Flow
    IoT --> EB
    API --> EB
    EB --> L1
    L1 --> DDB
    DDB --> DS
    DS --> L2
    L2 --> KDS
    KDS --> KDF
    KDF --> S3B

    S3B --> GC
    GC --> GDB
    GDB --> GJ1
    GDB --> GJ2
    GDB --> GJ3
    
    SF --> GJ1
    SF --> GJ3
    
    GJ1 --> S3S
    GJ2 --> S3S
    S3S --> GJ3
    GJ3 --> S3G

    S3G --> ATH
    S3G --> RS
    S3G --> SM
    S3G --> BR

    ATH --> QS
    RS --> QS
    SM --> QS
    
    GJ1 --> CW
    GJ2 --> CW
    GJ3 --> CW
    CW --> SNS

    %% Security
    IAM -.-> L1
    IAM -.-> L2
    IAM -.-> GJ1
    IAM -.-> GJ2
    IAM -.-> GJ3
    KMS -.-> S3B
    KMS -.-> S3S
    KMS -.-> S3G
    LF -.-> GDB

    %% Styling
    classDef storage fill:#e1f5fe
    classDef processing fill:#f3e5f5
    classDef analytics fill:#e8f5e8
    classDef monitoring fill:#fff3e0
    classDef security fill:#ffebee

    class S3B,S3S,S3G,DDB storage
    class GC,GDB,GJ1,GJ2,GJ3,SF processing
    class ATH,RS,SM,BR,QS analytics
    class CW,SNS monitoring
    class IAM,KMS,LF security


    subgraph "Analytics & ML Layer"
        ATH[🔍 Amazon Athena<br/>SQL Analytics]
