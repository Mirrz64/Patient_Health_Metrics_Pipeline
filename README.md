# Real-Time Patient Health Telemetry Pipeline (Medallion Architecture)
An enterprise-grade, streaming data engineering pipeline designed to ingest high-frequency IoT patient vital signs, process data streams under strict security and anonymization standards, and deliver analytical business intelligence. This project demonstrates an end-to-end implementation of a containerized Medallion Lakehouse Architecture handling simulated clinical telemetry.

## 📊 Executive & Operational Dashboards
The final layer of the pipeline features an operational control room dashboard built in Power BI, connected directly to production Parquet binaries to serve critical clinical analytics.

### 1. Hospital Ward Telemetry Overview
Tracks real-time stream acceleration, high/low heart rate event volumes, and baseline ward metrics over a time-series axis.

### 2. Clinical Risk Assessment & Registry
Aggregates distinct patient cohorts into prioritized risk profiles while auditing completely anonymized, SHA-256 masked patient records.

## 🏗️ Architecture & Data Journey
The pipeline manages high-throughput stream processing by breaking transformations into structured storage tiers:

[IoT Patient Vital Simulator]
             │ (High-Frequency JSON Streams)
             ▼
      [Apache Kafka]  ◄─── Ingestion Monitoring via Kafdrop UI
             │ 
             ▼ (Structured Streaming Consumer)
   [Apache Spark Engine]  ───► Cryptographic SHA-256 PII Masking
             │
             ▼
    [Silver Parquet Layer] ───► Anomaly Identification & Delta Deduplication
             │
             ▼ (PySpark Batch Aggregations)
     [Gold Lakehouse] ───► Optimized Patient Profiles & Hourly Trend Tables
             │
             ▼
       [Power BI] ───► Interactive Clinical Control Room

### Data Layer Breakdown
#### 1. Ingestion & Message Broker (Bronze Layer):
A simulated fleet of hospital IoT sensors streams continuous JSON payloads containing raw vitals (device_id, patient_id, timestamp, heart_rate, blood_oxygen_pct) directly into an Apache Kafka cluster topic. Stream integrity is monitored in real time using Kafdrop.

#### 2. Streaming & Security Masking (Silver Layer):
An Apache Spark Structured Streaming engine consumes the raw Kafka topics. The engine drops corrupt payloads, validates structural schemas, evaluates critical thresholds (CRITICAL_HIGH / CRITICAL_LOW), and cryptographically masks sensitive Patient IDs using a SHA-256 hex string to align with simulated HIPAA data-privacy constraints. Data is materialized as optimized, partitioned Parquet binaries.

#### 3. Analytical Transformations (Gold Layer):
A secondary PySpark batch aggregation script processes the Silver layer to compute multi-dimensional metrics, separating data into highly structured, reporting-ready data assets: gold_patient_risk_profiles and gold_hourly_vitals_trend.

## 🛠️ Tech Stack & Infrastructure
- Infrastructure & Orchestration: Docker, Docker Compose
- Message Broker: Apache Kafka, Kafdrop (Cluster Management UI)
- Stream & Batch Processing Engine: Apache Spark (PySpark Structured Streaming)
- Storage Tier: Partitioned Columnar Apache Parquet
- Data Visualization & BI: Power BI Desktop
- Development Environment: Python 3.13, Anaconda, Visual Studio Code

## 🔬 Pipeline Validation Reports
### Broker Telemetry Ingestion (Kafdrop UI)
Validation scan proving 1,218 records successfully captured and distributed across the message broker with sequential zero-loss offsets:

### Spark Processing & Masking Validation
Terminal audit proving successful execution of the streaming engine, structural masking of the patient_id_masked column, and isolation of 82 critical clinical anomalies:

## 🚀 Deployment & Execution Blueprint
### 1. Initialize Cluster Infrastructure
Spin up the containerized Kafka brokers and management interfaces in the background:

##### docker compose up -d

### 2. Launch the Stream Consumer & Masking Job
Initialize the Spark Structured Streaming engine to ingest live Kafka telemetry and write clean records to the Silver layer:

##### python spark_jobs/stream_vitals_to_silver.py

### 3. Materialize Gold Analytical Models
Execute the batch processing layer to run data warehouse transformations and update reporting directories:

##### python spark_jobs/generate_gold_tables.py

### 4. Visual Analytics
Open Power BI Desktop and point the native folder data connector directly to your local /data/gold_* paths to refresh the dashboard canvas.