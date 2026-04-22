from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'ranjith',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'unified_data_lake_pipeline',
    default_args=default_args,
    description='Unified Enterprise Data Lake Master Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 22),
    catchup=False
)

def master_etl():
    os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
    PASSWORD = "root"

    def load_to_hdfs(local, hdfs):
        subprocess.run(["hdfs", "dfs", "-put", "-f", local, hdfs])

    engine1 = create_engine(f'mysql+pymysql://root:{PASSWORD}@localhost/medicine_db')
    engine2 = create_engine(f'mysql+pymysql://root:{PASSWORD}@localhost/hospital_billing')
    engine3 = create_engine(f'mysql+pymysql://root:{PASSWORD}@localhost/loan_db')
    engine4 = create_engine(f'mysql+pymysql://root:{PASSWORD}@localhost/sentiment_db')

    pd.read_sql("SELECT * FROM medicines", engine1).to_csv('/tmp/medicines.csv', index=False)
    pd.read_sql("SELECT * FROM hospital_bills", engine2).to_csv('/tmp/hospital_bills.csv', index=False)
    pd.read_sql("SELECT * FROM insurance_claims", engine2).to_csv('/tmp/insurance_claims.csv', index=False)
    pd.read_sql("SELECT * FROM loan_applicants", engine3).to_csv('/tmp/loan_applicants.csv', index=False)
    pd.read_sql("SELECT * FROM loan_details", engine3).to_csv('/tmp/loan_details.csv', index=False)
    pd.read_sql("SELECT * FROM loan_history", engine3).to_csv('/tmp/loan_history.csv', index=False)
    pd.read_sql("SELECT * FROM social_posts", engine4).to_csv('/tmp/social_posts.csv', index=False)

    load_to_hdfs('/tmp/medicines.csv', '/data_lake/healthcare/medicines/')
    load_to_hdfs('/tmp/hospital_bills.csv', '/data_lake/healthcare/billing/')
    load_to_hdfs('/tmp/insurance_claims.csv', '/data_lake/healthcare/billing/')
    load_to_hdfs('/tmp/loan_applicants.csv', '/data_lake/banking/loans/')
    load_to_hdfs('/tmp/loan_details.csv', '/data_lake/banking/loans/')
    load_to_hdfs('/tmp/loan_history.csv', '/data_lake/banking/loans/')
    load_to_hdfs('/tmp/social_posts.csv', '/data_lake/social/sentiment/')
    print("Master ETL Complete!")

task1 = PythonOperator(
    task_id='master_etl_all_sources',
    python_callable=master_etl,
    dag=dag
)

task2 = BashOperator(
    task_id='master_spark_analytics',
    bash_command='source ~/.bashrc && spark-submit ~/projects/data_lake/pyspark/master_analytics.py',
    dag=dag
)

task3 = BashOperator(
    task_id='verify_enterprise_warehouse',
    bash_command='''hive -e "
    USE enterprise_dw;
    SELECT COUNT(*) FROM dim_medicines;
    SELECT COUNT(*) FROM fact_billing;
    SELECT COUNT(*) FROM fact_loan_risk;
    SELECT COUNT(*) FROM fact_sentiment;
    "''',
    dag=dag
)

task1 >> task2 >> task3
