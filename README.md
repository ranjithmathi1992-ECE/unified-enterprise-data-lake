# 🏗️ Unified Enterprise Data Lake

## 📌 Project Overview
A production-grade Enterprise Data Lake that unifies 
data from Healthcare, Banking and Social Media domains 
into a single unified Big Data platform. This capstone 
project demonstrates enterprise-level data engineering 
skills by combining 4 complete data pipelines into one 
master automated system.

## 🎯 Business Problem Solved
- Enterprises manage data from multiple disconnected systems
- No unified view of Healthcare Banking and Social data
- Manual integration is time consuming and error prone
- No single automated system to process all domains

## ✅ Solution Built
- Unified Data Lake architecture on Hadoop HDFS
- Master ETL pipeline extracts from 4 MySQL databases
- Master PySpark job processes all domains simultaneously
- Enterprise Hive Data Warehouse with 4 domain tables
- Single Master Airflow DAG orchestrates everything daily
- Cross domain business intelligence queries

## 🛠️ Technologies Used
| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.12 | Master ETL scripting |
| MySQL | 8.0 | 4 source databases |
| Apache Hadoop | 3.3.6 | Unified Data Lake storage |
| Apache Hive | 3.1.3 | Enterprise Data Warehouse |
| Apache Spark PySpark | 3.2.4 | Master analytics engine |
| TextBlob | Latest | NLP sentiment analysis |
| Apache Airflow | 2.9.0 | Master pipeline orchestration |

## 🏗️ Data Lake Architecture
## 📊 Data Lake Contents
| Domain | Tables | Records |
|--------|--------|---------|
| Healthcare Medicines | 1 | 20 |
| Healthcare Billing | 2 | 40 |
| Banking Loans | 3 | 60 |
| Social Media | 1 | 20 |
| *Total* | *7* | *140* |

## 📁 Project Structure
data_lake/
├── README.md
├── .gitignore
├── etl/
│   └── master_etl.py
├── pyspark/
│   └── master_analytics.py
└── airflow/
    └── master_datalake_dag.py

## 🚀 How to Run
Step 1 - Start Hadoop:
start-dfs.sh and start-yarn.sh

Step 2 - Create Data Lake folders:
hdfs dfs -mkdir -p /data_lake/healthcare/medicines
hdfs dfs -mkdir -p /data_lake/healthcare/billing
hdfs dfs -mkdir -p /data_lake/banking/loans
hdfs dfs -mkdir -p /data_lake/social/sentiment
hdfs dfs -mkdir -p /data_lake/results/healthcare/medicines
hdfs dfs -mkdir -p /data_lake/results/healthcare/billing
hdfs dfs -mkdir -p /data_lake/results/banking
hdfs dfs -mkdir -p /data_lake/results/social

Step 3 - Run Master ETL:
python3 etl/master_etl.py

Step 4 - Run Master Spark Analytics:
spark-submit pyspark/master_analytics.py

Step 5 - Start Airflow:
airflow standalone

Step 6 - Access UI:
http://localhost:8080

## 🎯 Skills Demonstrated
- Enterprise Data Lake Architecture
- Multi-source ETL Pipeline Development
- PySpark Big Data Processing
- Spark MLlib Machine Learning
- NLP Sentiment Analysis
- Apache Hive Enterprise Data Warehouse
- Star Schema Design
- Apache Airflow Master DAG
- Cross Domain Data Integration
- Python Programming
- MySQL Database Design
- Hadoop HDFS Storage
- Healthcare Domain Knowledge
- Banking Domain Knowledge
- Social Media Analytics## 📊 Data Lake Contents
