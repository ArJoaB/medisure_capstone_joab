# Databricks notebook source
from pyspark.sql.functions import col, trim

# Load bronze members table
df_raw = spark.table("medisure_capstone_joab.medisure_bronze.medisure_ref_diagnosis")

# Trim whitespace from string columns
string_cols = [f.name for f in df_raw.schema.fields if f.dataType.simpleString() == "string"]
for c in string_cols:
    df_raw = df_raw.withColumn(c, trim(col(c)))

# Deduplicate by MemberID
df_dedup = df_raw.dropDuplicates(["Code"])

# Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS medisure_capstone_joab.medisure_silver")

# Save as Delta Table for diagnosis clean
df_dedup.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/silver/diagnosis_clean")

# Create silver table for diagnosis clean
spark.sql("""
CREATE TABLE IF NOT EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_diagnosis_clean
AS SELECT * FROM delta.`dbfs:/silver/diagnosis_clean`
""")

display(spark.table("medisure_capstone_joab.medisure_silver.medisure_ref_diagnosis_clean"))