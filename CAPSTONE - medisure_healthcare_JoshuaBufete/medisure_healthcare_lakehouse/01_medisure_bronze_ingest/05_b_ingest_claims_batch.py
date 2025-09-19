# Databricks notebook source
# Define paths
batch_url = "https://raw.githubusercontent.com/ArJoaB/medisure_capstone_joab/refs/heads/main/medisure_data_fact/claims_batch.csv"
csv_path = "/tmp/claims_batch.csv"   # local temp path

# Copy file from GitHub to DBFS
dbutils.fs.cp(batch_url, f"dbfs:/tmp/claims_batch.csv")

# Step 1: Read CSV as raw strings to inspect data
raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")  # keep everything as string first
    .load(f"dbfs:/tmp/claims_batch.csv")
)

# Step 2: Clean and cast columns, handling errors
from pyspark.sql.functions import col, to_date, to_timestamp, when

clean_df = (
    raw_df
    .withColumn("ClaimID", col("ClaimID"))
    .withColumn("MemberID", col("MemberID"))
    .withColumn("ProviderID", col("ProviderID"))
    .withColumn("ClaimDate", to_date(col("ClaimDate"), "yyyy-MM-dd"))
    .withColumn("ServiceDate", to_date(col("ServiceDate"), "yyyy-MM-dd"))
    .withColumn("Status", when(col("Status").rlike("^[A-Za-z]+$"), col("Status")).otherwise("N/A"))
    .withColumn("Amount", when(col("Amount").cast("double").isNotNull(), col("Amount").cast("double")).otherwise(0.0))
    .withColumn("ICD10Codes", col("ICD10Codes"))
    .withColumn("CPTCodes", col("CPTCodes"))
    .withColumn("SubmissionChannel", col("SubmissionChannel"))
    .withColumn("Notes", col("Notes"))
    .withColumn("IngestTimeStamp", to_timestamp(col("IngestTimeStamp")))
)

# Step 3: Write to Delta table
clean_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
    "medisure_capstone_joab.medisure_bronze.medisure_fact_claims"
)

# Step 4: Verify load
display(spark.sql("SELECT * FROM medisure_capstone_joab.medisure_bronze.medisure_fact_claims LIMIT 20"))