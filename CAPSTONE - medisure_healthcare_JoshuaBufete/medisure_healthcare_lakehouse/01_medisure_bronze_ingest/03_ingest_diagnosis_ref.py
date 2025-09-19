# Databricks notebook source
# Step 1: Define the source URL
diagnoses_url = "https://raw.githubusercontent.com/ArJoaB/medisure_capstone_joab/refs/heads/main/medisure_data_ref/diagnosis_ref.csv"

# Step 2: Define a raw path to store the CSV temporarily
raw_path = "/dbfs/tmp/"; raw_file_path = raw_path + "diagnosis_ref.csv"

# Step 3: Download the CSV into DBFS
dbutils.fs.cp(diagnoses_url, raw_file_path)

# Step 4: Read the CSV into a DataFrame
df_diag = spark.read.csv(raw_file_path, header=True, inferSchema=True)

# Step 5: Write the DataFrame as a managed table (Spark will manage the storage)
df_diag.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("medisure_capstone_joab.medisure_bronze.medisure_ref_diagnosis")

display(spark.table('medisure_capstone_joab.medisure_bronze.medisure_ref_diagnosis'))
