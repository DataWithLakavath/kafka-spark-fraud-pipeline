# Real-Time Fraud Detection Data Pipeline using Kafka and PySpark

## Overview
This project demonstrates an end-to-end data engineering pipeline for fraud detection using Kafka and PySpark. It simulates real-time transaction ingestion, batch and streaming data processing, and multi-layer data lake storage for analytics.

## Problem Statement
Financial systems generate large volumes of transaction data that must be processed quickly to detect suspicious activity. This project shows how streaming and batch data pipelines can be built using modern data engineering tools.

## Tech Stack
- Python
- Apache Kafka
- PySpark
- Pandas
- Parquet
- Local Data Lake
- VS Code
- GitHub

## Architecture
Transaction Generator / CSV  
↓  
Kafka Producer  
↓  
Kafka Topic  
↓  
Spark Streaming / Batch Processing  
↓  
Data Lake (Raw → Processed → Analytics)  
↓  
Fraud Insights

## Project Structure
- `producer.py` → sends transaction data to Kafka
- `consumer.py` → consumes Kafka messages
- `spark_jobs/spark_stream.py` → real-time Spark processing
- `spark_jobs/spark_batch.py` → batch ingestion from CSV
- `spark_jobs/fraud_aggregation.py` → fraud analytics aggregation
- `data_lake/raw/` → raw layer
- `data_lake/processed/` → processed parquet layer
- `data_lake/analytics/` → aggregated analytics layer

## Pipeline Flow
1. Transaction data is generated and stored in CSV format
2. Kafka Producer streams records into a Kafka topic
3. Spark Structured Streaming consumes live data from Kafka
4. Batch Spark job processes historical transaction data
5. Cleaned data is written into the processed layer in parquet format
6. Aggregation job creates fraud summaries in the analytics layer

## How to Run

### Activate virtual environment
```powershell
.\venv\Scripts\Activate.ps1