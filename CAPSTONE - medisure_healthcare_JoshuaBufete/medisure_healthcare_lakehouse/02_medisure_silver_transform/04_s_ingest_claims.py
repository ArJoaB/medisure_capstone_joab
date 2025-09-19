# Databricks notebook source
from pyspark.sql.functions import col, trim, split, explode

# Load bronze claims table
df_raw = spark.table("medisure_capstone_joab.medisure_bronze.medisure_fact_claims")

# Trim whitespace from string columns
string_cols = [f.name for f in df_raw.schema.fields if f.dataType.simpleString() == "string"]
for c in string_cols:
    df_raw = df_raw.withColumn(c, trim(col(c)))

# Split and explode ICD10Codes and CPTCodes columns by ";"
df_split = df_raw.withColumn("ICD10Codes", split(col("ICD10Codes"), ";")) \
                 .withColumn("CPTCodes", split(col("CPTCodes"), ";"))
df_exploded = df_split.withColumn("ICD10Codes", explode(col("ICD10Codes"))) \
                      .withColumn("CPTCodes", explode(col("CPTCodes")))

# Select only the required columns (excluding ServiceDate, SubmissionChannel, Notes, IngestTimeStamp)
selected_cols = [
    "ClaimID",
    "MemberID",
    "ProviderID",
    "ClaimDate",
    "Status",
    "Amount",
    "ICD10Codes",
    "CPTCodes"
]
df_final = df_exploded.select(*selected_cols)

# Deduplicate by ClaimID and ICD10Codes
df_dedup = df_final.dropDuplicates(["ClaimID", "ICD10Codes"])

# Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS medisure_capstone_joab.medisure_silver")

# Save as Delta Table
df_dedup.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/silver/claims_clean")

# Create silver table
spark.sql("""
CREATE TABLE IF NOT EXISTS medisure_capstone_joab.medisure_silver.medisure_fact_claims_clean
AS SELECT * FROM delta.`dbfs:/silver/claims_clean`
""")

display(spark.table("medisure_capstone_joab.medisure_silver.medisure_fact_claims_clean"))