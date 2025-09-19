# Databricks notebook source
from pyspark.sql.functions import ( 
    col, trim, regexp_replace, split, explode_outer, 
    monotonically_increasing_id
)

# Define source/target
BRONZE_TABLE = "medisure_capstone_joab.medisure_bronze.medisure_ref_providers"
SILVER_SCHEMA = "medisure_capstone_joab.medisure_silver"

# Create Silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# Load Bronze table
df_raw = spark.table(BRONZE_TABLE)

# Trim string columns
string_cols = [f.name for f in df_raw.schema.fields if f.dataType.simpleString() == "string"]
for col_name in string_cols:
    df_raw = df_raw.withColumn(col_name, trim(col(col_name)))

# Drop duplicates by ProviderID
df_clean = df_raw.dropDuplicates(["ProviderID"])

# Save temp clean table
clean_table_temp = f"{SILVER_SCHEMA}.medisure_ref_providers_clean_temp"
df_clean.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(clean_table_temp)

# --- Specialties ---
df_specialties = df_clean.select("ProviderID", "TIN", "Name", "IsActive", "Specialties") \
    .withColumn("Specialty", explode_outer(col("Specialties"))) \
    .drop("Specialties") \
    .filter(col("Specialty").isNotNull() & (col("Specialty") != ""))
specialties_table = f"{SILVER_SCHEMA}.medisure_ref_providers_specialties"
df_specialties.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(specialties_table)

# --- Locations ---
df_locations = df_clean.select("ProviderID", "TIN", "Name", "IsActive", "Locations") \
    .withColumn("Location", explode_outer(col("Locations"))) \
    .select(
        "ProviderID", "TIN", "Name", "IsActive",
        col("Location.Address").alias("Address"),
        col("Location.City").alias("City"),
        col("Location.State").alias("State")
    ) \
    .filter(col("Address").isNotNull() | col("City").isNotNull() | col("State").isNotNull())
locations_table = f"{SILVER_SCHEMA}.medisure_ref_providers_locations"
df_locations.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(locations_table)

# --- Names ---
df_names = df_clean.select("ProviderID", "TIN", "Name", "IsActive") \
    .withColumn("Name_split", split(regexp_replace(col("Name"), r"\s*and\s*|,", ","), ",")) \
    .withColumn("Name_individual", explode_outer(col("Name_split"))) \
    .withColumn("Name_individual", trim(col("Name_individual"))) \
    .filter(col("Name_individual") != "") \
    .drop("Name", "Name_split") \
    .withColumnRenamed("Name_individual", "Name")
names_table = f"{SILVER_SCHEMA}.medisure_ref_providers_names"
df_names.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(names_table)

# --- Unified table ---
df_base = df_clean.withColumn("row_id", monotonically_increasing_id())

df_names_unified = df_base.select("row_id", "ProviderID", "TIN", "Name", "IsActive") \
    .withColumn("Name_split", split(regexp_replace(col("Name"), r"\s*and\s*|,", ","), ",")) \
    .withColumn("Name_clean", explode_outer(col("Name_split"))) \
    .withColumn("Name_clean", trim(col("Name_clean"))) \
    .filter(col("Name_clean") != "") \
    .select("row_id", "ProviderID", "TIN", "IsActive", col("Name_clean").alias("Name"))

df_locations_unified = df_base.select("row_id", "Locations") \
    .withColumn("Location", explode_outer(col("Locations"))) \
    .select(
        "row_id",
        col("Location.Address").alias("Address"),
        col("Location.City").alias("City"),
        col("Location.State").alias("State")
    )

df_specialties_unified = df_base.select("row_id", "Specialties") \
    .withColumn("Specialty", explode_outer(col("Specialties"))) \
    .select("row_id", "Specialty")