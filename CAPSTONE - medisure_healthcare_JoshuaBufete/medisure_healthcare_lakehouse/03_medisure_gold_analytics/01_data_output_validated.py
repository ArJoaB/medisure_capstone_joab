# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Gold schema
# MAGIC CREATE SCHEMA IF NOT EXISTS medisure_capstone_joab.medisure_gold;
# MAGIC
# MAGIC -- Fraud detection table
# MAGIC CREATE OR REPLACE TABLE medisure_capstone_joab.medisure_gold.tbl_fraud_detection AS
# MAGIC SELECT 
# MAGIC   f.ClaimID,
# MAGIC   f.MemberID,
# MAGIC   COALESCE(m.Name, 'N/A') AS MemberName,
# MAGIC   f.ProviderID,
# MAGIC   COALESCE(p.Name, 'N/A') AS ProviderName,
# MAGIC   COALESCE(f.ICD10Codes, 'N/A') AS ICD10Codes,
# MAGIC   COALESCE(d.Description, 'N/A') AS DiagnosisDescription,
# MAGIC   COALESCE(f.Amount, 0) AS Amount,
# MAGIC   f.ClaimDate,
# MAGIC   CASE 
# MAGIC     WHEN f.Amount > 10000 THEN 'High Value Claim'
# MAGIC     WHEN COUNT(*) OVER (PARTITION BY f.MemberID, f.ClaimDate) > 1 THEN 'Duplicate Claim Same Day'
# MAGIC     ELSE 'Normal'
# MAGIC   END AS FraudFlag
# MAGIC FROM medisure_capstone_joab.medisure_silver.medisure_fact_claims_clean f
# MAGIC LEFT JOIN medisure_capstone_joab.medisure_silver.medisure_ref_members_clean m
# MAGIC   ON f.MemberID = m.MemberID
# MAGIC LEFT JOIN medisure_capstone_joab.medisure_silver.medisure_ref_providers_clean p
# MAGIC   ON f.ProviderID = p.ProviderID
# MAGIC LEFT JOIN medisure_capstone_joab.medisure_silver.medisure_ref_diagnosis_clean d
# MAGIC   ON f.ICD10Codes = d.Code;
# MAGIC
# MAGIC -- Compliance summary table
# MAGIC CREATE OR REPLACE TABLE medisure_capstone_joab.medisure_gold.tbl_compliance AS
# MAGIC SELECT 
# MAGIC   f.MemberID, 
# MAGIC   m.Name AS MemberName,
# MAGIC   COUNT(*) AS ClaimCount, 
# MAGIC   SUM(f.Amount) AS TotalAmount,
# MAGIC   AVG(f.Amount) AS AvgClaimAmount
# MAGIC FROM medisure_capstone_joab.medisure_silver.medisure_fact_claims_clean f
# MAGIC LEFT JOIN medisure_capstone_joab.medisure_silver.medisure_ref_members_clean m
# MAGIC   ON f.MemberID = m.MemberID
# MAGIC GROUP BY f.MemberID, m.Name
# MAGIC ORDER BY TotalAmount DESC;
# MAGIC
# MAGIC -- Provider KPIs table
# MAGIC CREATE OR REPLACE TABLE medisure_capstone_joab.medisure_gold.tbl_provider_kpis AS
# MAGIC SELECT 
# MAGIC   f.ProviderID, 
# MAGIC   p.Name AS ProviderName,
# MAGIC   COUNT(*) AS TotalClaims, 
# MAGIC   AVG(f.Amount) AS AvgClaim,
# MAGIC   SUM(f.Amount) AS TotalRevenue,
# MAGIC   COUNT(DISTINCT f.MemberID) AS UniqueMembers
# MAGIC FROM medisure_capstone_joab.medisure_silver.medisure_fact_claims_clean f
# MAGIC LEFT JOIN medisure_capstone_joab.medisure_silver.medisure_ref_providers_clean p
# MAGIC   ON f.ProviderID = p.ProviderID
# MAGIC GROUP BY f.ProviderID, p.Name
# MAGIC ORDER BY TotalRevenue DESC;

# COMMAND ----------

# Fraud Detection
df_fraud = spark.table("medisure_capstone_joab.medisure_gold.tbl_fraud_detection")
display(df_fraud.limit(20))

# Compliance
df_compliance = spark.table("medisure_capstone_joab.medisure_gold.tbl_compliance")
display(df_compliance.limit(20))

# Provider KPIs
df_kpis = spark.table("medisure_capstone_joab.medisure_gold.tbl_provider_kpis")
display(df_kpis.limit(20))