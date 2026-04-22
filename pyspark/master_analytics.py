import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['SPARK_HOME'] = '/opt/spark'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum, round
import pandas as pd
from textblob import TextBlob
import subprocess
from datetime import datetime

print("=" * 50)
print("UNIFIED DATA LAKE - MASTER ANALYTICS")
print(f"Started at: {datetime.now()}")
print("=" * 50)

spark = SparkSession.builder \
    .appName("UnifiedDataLake") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Created!")

print("\n--- LOADING ALL DATA FROM HDFS ---")

medicines = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/healthcare/medicines/medicines.csv")

bills = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/healthcare/billing/hospital_bills.csv")

claims = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/healthcare/billing/insurance_claims.csv")

applicants = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/banking/loans/loan_applicants.csv")

loans = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/banking/loans/loan_details.csv")

history = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data_lake/banking/loans/loan_history.csv")

posts_pd = pd.read_csv('/tmp/social_posts.csv')

print(f"Medicines: {medicines.count()} records ✅")
print(f"Bills: {bills.count()} records ✅")
print(f"Claims: {claims.count()} records ✅")
print(f"Loan Applicants: {applicants.count()} records ✅")
print(f"Loans: {loans.count()} records ✅")
print(f"Social Posts: {len(posts_pd)} records ✅")

print("\n--- HEALTHCARE ANALYTICS ---")

medicines_analyzed = medicines.withColumn(
    "stock_status",
    when(col("stock_quantity") < 150, "LOW STOCK")
    .otherwise("SUFFICIENT")
)

print("\n=== Medicine Stock Status ===")
medicines_analyzed.groupBy("stock_status") \
    .count() \
    .show()

print("\n=== Medicine Stock by Branch ===")
medicines_analyzed.groupBy("branch", "stock_status") \
    .count() \
    .orderBy("branch") \
    .show()

billing_joined = bills.join(claims, "bill_id", "inner")
billing_analyzed = billing_joined.withColumn(
    "difference",
    round(col("bill_amount") - col("claimed_amount"), 2)
).withColumn(
    "anomaly_flag",
    when(
        (col("bill_amount") - col("claimed_amount")) / col("bill_amount") * 100 > 20,
        "HIGH ANOMALY"
    ).when(
        (col("bill_amount") - col("claimed_amount")) / col("bill_amount") * 100 > 10,
        "MEDIUM ANOMALY"
    ).otherwise("NORMAL")
)

print("\n=== Billing Anomaly Summary ===")
billing_analyzed.groupBy("anomaly_flag") \
    .count() \
    .show()

print("\n--- BANKING ANALYTICS ---")

loan_joined = applicants \
    .join(loans, "applicant_id", "inner") \
    .join(history, "applicant_id", "inner")

loan_analyzed = loan_joined.withColumn(
    "risk_level",
    when(
        (col("credit_score") < 600) |
        (col("previous_defaults") > 1),
        "HIGH RISK"
    ).when(
        (col("credit_score") < 700) |
        (col("previous_defaults") == 1),
        "MEDIUM RISK"
    ).otherwise("LOW RISK")
)

print("\n=== Loan Risk Distribution ===")
loan_analyzed.groupBy("risk_level") \
    .count() \
    .show()

print("\n=== Average Credit Score by Risk ===")
loan_analyzed.groupBy("risk_level") \
    .agg(
        avg("credit_score").alias("avg_credit_score"),
        avg("income").alias("avg_income"),
        count("applicant_id").alias("total")
    ).show()

print("\n--- SOCIAL MEDIA ANALYTICS ---")

def get_sentiment(text):
    if pd.isna(text):
        return 'NEUTRAL'
    score = TextBlob(str(text)).sentiment.polarity
    if score > 0.1:
        return 'POSITIVE'
    elif score < -0.1:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'

posts_pd['sentiment'] = posts_pd['post_text'].apply(get_sentiment)

print("\n=== Brand Sentiment Summary ===")
sentiment_summary = posts_pd.groupby(
    ['brand', 'sentiment']
).size().reset_index(name='count')
print(sentiment_summary.to_string())

print("\n--- UNIFIED BUSINESS INSIGHTS ---")
print("\n=== Cross Domain Summary ===")
print(f"Total Healthcare Records: {medicines.count() + bills.count()}")
print(f"Total Banking Records: {applicants.count()}")
print(f"Total Social Records: {len(posts_pd)}")
print(f"Total Data Lake Records: {medicines.count() + bills.count() + applicants.count() + len(posts_pd)}")

print("\nSaving unified results to HDFS...")

medicines_analyzed.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/data_lake/results/healthcare/medicines/")

billing_analyzed.select(
    "bill_id", "patient_name", "treatment",
    "bill_amount", "claimed_amount",
    "difference", "anomaly_flag"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/data_lake/results/healthcare/billing/")

loan_analyzed.select(
    "applicant_id", "applicant_name",
    "income", "credit_score",
    "loan_amount", "loan_purpose",
    "previous_defaults", "risk_level"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/data_lake/results/banking/")

posts_pd.to_csv('/tmp/sentiment_final.csv', index=False)
subprocess.run([
    "hdfs", "dfs", "-put", "-f",
    "/tmp/sentiment_final.csv",
    "/data_lake/results/social/"
])

print("All results saved to Data Lake! ✅")
print("\n" + "=" * 50)
print("MASTER ANALYTICS COMPLETE!")
print(f"Finished at: {datetime.now()}")
print("=" * 50)

spark.stop()
