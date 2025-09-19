# Databricks notebook source
import requests
import os

# Step 1: Download providers.json from GitHub to local driver
providers_url = (
    "https://raw.githubusercontent.com/ArJoaB/medisure_capstone_joab/main/medisure_data_ref/providers.json"
)
local_dir = "/tmp/"
os.makedirs(local_dir, exist_ok=True)
local_file = os.path.join(local_dir, "providers.json")

response = requests.get(providers_url)
with open(local_file, "w") as f:
    f.write(response.text)

# Copy file to DBFS
dbfs_raw_path = "dbfs:/bronze/raw/providers.json"
dbutils.fs.put("dbfs:/bronze/raw/providers.json", open(local_file).read(), True)

# Read JSON into Spark DataFrame
df_providers = spark.read.format('json').option('multiLine', True).load(dbfs_raw_path)

# Write DataFrame as Delta
bronze_path = "dbfs:/bronze/"
df_providers.write.format("delta").mode("overwrite").save(bronze_path + "providers")

# Create Delta table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS medisure_capstone_joab.medisure_bronze.medisure_ref_providers
AS SELECT * FROM delta.`{bronze_path}providers`
""")

display(spark.table("medisure_capstone_joab.medisure_bronze.medisure_ref_providers"))