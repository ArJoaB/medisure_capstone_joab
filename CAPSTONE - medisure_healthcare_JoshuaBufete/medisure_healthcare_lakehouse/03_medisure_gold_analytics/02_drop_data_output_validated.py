# Databricks notebook source
# MAGIC %sql
# MAGIC -- Bronze Layer
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.claims_batch;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.claims_stream;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.medisure_fact_claims;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.medisure_ref_providers_clean;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.medisure_ref_providers_specialties;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_bronze.members;
# MAGIC
# MAGIC -- Gold Layer
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_gold.medisure_data;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_gold.medisure_data_output;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_gold.tbl_compliance;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_gold.tbl_fraud_detection;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_gold.tbl_provider_kpis;
# MAGIC
# MAGIC DROP VIEW IF EXISTS medisure_capstone_joab.medisure_gold.vw_compliance;
# MAGIC DROP VIEW IF EXISTS medisure_capstone_joab.medisure_gold.vw_provider_kpis;
# MAGIC
# MAGIC -- Silver Layer
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_flat;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_clean_temp;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_fact_claims_valid;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_fact_claims_dedup;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_fact_claims_clea;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_names;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_locations;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_unified;
# MAGIC DROP TABLE IF EXISTS medisure_capstone_joab.medisure_silver.medisure_ref_providers_specialties;