-- =============================================================================
-- 01_setup.sql  |  Snowflake setup for the G9 AIS & Port Data pipeline
-- =============================================================================
-- Run once. Safe to rerun (everything is IF NOT EXISTS).
-- Sections 1-3 run as ACCOUNTADMIN, section 4 runs as DBT_ROLE.
--
-- Prerequisite (done in AWS, not here):
--   IAM role g9-snowflake-s3-role with READ-ONLY access to s3://g9-ais-project/silver/
--   (policy g9-snowflake-silver-read). Its trust policy was updated with the
--   AWS_KEY_ID and AWS_SECRET_KEY from AWS.
--
-- Do NOT put passwords or keys in this file.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Compute and storage objects
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS AIS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS AIS;
CREATE SCHEMA IF NOT EXISTS AIS.RAW;       -- loaded copies of the silver Parquet
CREATE SCHEMA IF NOT EXISTS AIS.STAGING;   -- dbt staging models
CREATE SCHEMA IF NOT EXISTS AIS.MARTS;     -- dbt gold layer (star schema)

-- -----------------------------------------------------------------------------
-- 2. Role and grants (one shared role for the people building models)
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS DBT_ROLE;

GRANT USAGE ON WAREHOUSE AIS_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE AIS TO ROLE DBT_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIS TO ROLE DBT_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIS TO ROLE DBT_ROLE;

-- One line per person:
GRANT ROLE DBT_ROLE TO USER <snowflake_username>;

-- -----------------------------------------------------------------------------
-- 3. File format and external stage (as DBT_ROLE)
-- -----------------------------------------------------------------------------
USE ROLE DBT_ROLE;
USE WAREHOUSE AIS_WH;
USE SCHEMA AIS.RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FF TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS S3_SILVER
  URL = 's3://g9-ais-project/silver/'
  CREDENTIALS = (AWS_KEY_ID = 'AWS_ACCESS_KEY_ID' AWS_SECRET_KEY = 'AWS_SECRET_ACCESS_KEY')
  FILE_FORMAT = PARQUET_FF;

-- Sanity check: should list .parquet files (plus empty *_$folder$ markers left by Glue).
-- If this errors, the integration trust policy in AWS is wrong.
LIST @S3_SILVER;