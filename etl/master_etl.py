import os
os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
from sqlalchemy import create_engine
import pandas as pd
import subprocess
from datetime import datetime

print("=" * 50)
print("UNIFIED DATA LAKE - MASTER ETL PIPELINE")
print(f"Started at: {datetime.now()}")
print("=" * 50)

PASSWORD = "YOUR_PASSWORD"

def load_to_hdfs(local_path, hdfs_path):
    subprocess.run([
        "hdfs", "dfs", "-put", "-f",
        local_path, hdfs_path
    ])
    print(f"Loaded {local_path} to {hdfs_path} ✅")

def verify_hdfs(hdfs_path):
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_path],
        capture_output=True, text=True
    )
    print(f"HDFS {hdfs_path}:")
    print(result.stdout)

print("\n--- EXTRACTING HEALTHCARE DATA ---")
engine1 = create_engine(
    f'mysql+pymysql://root:root@localhost/medicine_db'
)
medicines_df = pd.read_sql(
    "SELECT * FROM medicines", engine1
)
medicines_df.to_csv('/tmp/medicines.csv', index=False)
load_to_hdfs(
    '/tmp/medicines.csv',
    '/data_lake/healthcare/medicines/'
)
print(f"Medicines extracted: {len(medicines_df)} records")

print("\n--- EXTRACTING BILLING DATA ---")
engine2 = create_engine(
    f'mysql+pymysql://root:root@localhost/hospital_billing'
)
bills_df = pd.read_sql(
    "SELECT * FROM hospital_bills", engine2
)
claims_df = pd.read_sql(
    "SELECT * FROM insurance_claims", engine2
)
bills_df.to_csv('/tmp/hospital_bills.csv', index=False)
claims_df.to_csv('/tmp/insurance_claims.csv', index=False)
load_to_hdfs(
    '/tmp/hospital_bills.csv',
    '/data_lake/healthcare/billing/'
)
load_to_hdfs(
    '/tmp/insurance_claims.csv',
    '/data_lake/healthcare/billing/'
)
print(f"Bills extracted: {len(bills_df)} records")
print(f"Claims extracted: {len(claims_df)} records")

print("\n--- EXTRACTING BANKING DATA ---")
engine3 = create_engine(
    f'mysql+pymysql://root:root@localhost/loan_db'
)
applicants_df = pd.read_sql(
    "SELECT * FROM loan_applicants", engine3
)
loans_df = pd.read_sql(
    "SELECT * FROM loan_details", engine3
)
history_df = pd.read_sql(
    "SELECT * FROM loan_history", engine3
)
applicants_df.to_csv('/tmp/loan_applicants.csv', index=False)
loans_df.to_csv('/tmp/loan_details.csv', index=False)
history_df.to_csv('/tmp/loan_history.csv', index=False)
load_to_hdfs(
    '/tmp/loan_applicants.csv',
    '/data_lake/banking/loans/'
)
load_to_hdfs(
    '/tmp/loan_details.csv',
    '/data_lake/banking/loans/'
)
load_to_hdfs(
    '/tmp/loan_history.csv',
    '/data_lake/banking/loans/'
)
print(f"Loan applicants extracted: {len(applicants_df)} records")
print(f"Loan details extracted: {len(loans_df)} records")

print("\n--- EXTRACTING SOCIAL MEDIA DATA ---")
engine4 = create_engine(
    f'mysql+pymysql://root:root@localhost/sentiment_db'
)
posts_df = pd.read_sql(
    "SELECT * FROM social_posts", engine4
)
posts_df.to_csv('/tmp/social_posts.csv', index=False)
load_to_hdfs(
    '/tmp/social_posts.csv',
    '/data_lake/social/sentiment/'
)
print(f"Social posts extracted: {len(posts_df)} records")

print("\n--- VERIFYING DATA LAKE ---")
verify_hdfs('/data_lake/healthcare/medicines/')
verify_hdfs('/data_lake/healthcare/billing/')
verify_hdfs('/data_lake/banking/loans/')
verify_hdfs('/data_lake/social/sentiment/')

print("\n" + "=" * 50)
print("MASTER ETL COMPLETE!")
print(f"Finished at: {datetime.now()}")
print("=" * 50)
