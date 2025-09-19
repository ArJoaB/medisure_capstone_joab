# Databricks notebook source
#Step 1: Define paths and create folder in DBFS
volume_path = "medisure_capstone_joab/medisure_bronze/claims_stream_folder"
dbutils.fs.mkdirs(f"/{volume_path}")

# Download a sample JSON file into the folder (for initial load testing)
url_json = "https://raw.githubusercontent.com/ArJoaB/medisure_capstone_joab/main/medisure_data_fact/claims_stream.json"
dbutils.fs.cp(url_json, f"/{volume_path}/claims_stream.json")

# Step 2: Read JSON files as a streaming DataFrame (Auto Loader)
claims_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.includeExistingFiles", "true")  # ✅ process old + new files
    .schema(
        """
        ClaimID STRING,
        MemberID STRING,
        ProviderID STRING,
        ClaimDate STRING,
        Status STRING,
        Amount DOUBLE,
        ICD10Codes STRING,
        CPTCodes STRING
        """
    )
    .load(f"/{volume_path}")
)

# Step 3: Clean and cast columns
from pyspark.sql.functions import col, to_date, when, current_date

clean_stream_df = (
    claims_stream_df
    .withColumn("ClaimDate", to_date(col("ClaimDate"), "yyyy-MM-dd"))
    .withColumn("Status", when(col("Status").rlike("^[A-Za-z]+$"), col("Status")).otherwise("N/A"))
    .withColumn("Amount", when(col("Amount").cast("double").isNotNull(), col("Amount").cast("double")).otherwise(0.0))
    .withColumn("UpdateDate", current_date())
)

# Step 4: Write streaming data into a Delta table (continuous pipeline)
query = (
    clean_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/{volume_path}/_checkpoint")
    .trigger(availableNow=True)
    .toTable("medisure_capstone_joab.medisure_bronze.claims_stream")
)

# Step 5: Query the table (batch read, since the stream is already writing)
display(spark.table("medisure_capstone_joab.medisure_bronze.claims_stream"))

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping streaming query: {query.name}")
    query.stop()