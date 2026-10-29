-- =============================================================================
-- 02_initial_load.sql  |  Initial full load of silver (S3) into Snowflake RAW
-- =============================================================================
-- Run as DBT_ROLE after 01_setup.sql. Safe to rerun: each table is truncated and
-- reloaded, so a rerun replaces the contents instead of duplicating rows.
--
-- Sources (written by the Glue jobs):
--   s3://g9-ais-project/silver/ports_activity/ports/   (partitioned year=/month=)
--   s3://g9-ais-project/silver/ports_reference/
--
-- Notes:
--   * PATTERN skips the empty *_$folder$ marker files that Glue leaves in the bucket.
--   * year and month are folder names, not columns inside the Parquet files, so they
--     are not in the table. Derive them from date (dbt does this in stg_ports_activity).
--   * Weekly incremental loading is a separate step (staging table + delete/insert by month).
-- =============================================================================

USE ROLE DBT_ROLE;
USE WAREHOUSE AIS_WH;
USE SCHEMA AIS.RAW;

-- -----------------------------------------------------------------------------
-- Tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PORTS_ACTIVITY (
  portid                   VARCHAR,
  date                     DATE,
  portcalls_container      NUMBER,
  portcalls_dry_bulk       NUMBER,
  portcalls_general_cargo  NUMBER,
  portcalls_roro           NUMBER,
  portcalls_tanker         NUMBER,
  portcalls_cargo          NUMBER,
  portcalls_total          NUMBER,
  import_container         NUMBER,
  import_dry_bulk          NUMBER,
  import_general_cargo     NUMBER,
  import_roro              NUMBER,
  import_tanker            NUMBER,
  import_cargo             NUMBER,
  import_total             NUMBER,
  export_container         NUMBER,
  export_dry_bulk          NUMBER,
  export_general_cargo     NUMBER,
  export_roro              NUMBER,
  export_tanker            NUMBER,
  export_cargo             NUMBER,
  export_total             NUMBER
);

CREATE TABLE IF NOT EXISTS PORTS_REF (
  portid                         VARCHAR,
  portname                       VARCHAR,
  country                        VARCHAR,
  country_ascii                  VARCHAR,
  iso3                           VARCHAR,
  continent                      VARCHAR,
  fullname                       VARCHAR,
  lat                            FLOAT,
  lon                            FLOAT,
  vessel_count_total             NUMBER,
  vessel_count_container         NUMBER,
  vessel_count_dry_bulk          NUMBER,
  vessel_count_general_cargo     NUMBER,
  vessel_count_roro              NUMBER,
  vessel_count_tanker            NUMBER,
  industry_top1                  VARCHAR,
  industry_top2                  VARCHAR,
  industry_top3                  VARCHAR,
  share_country_maritime_import  FLOAT,
  share_country_maritime_export  FLOAT,
  locode                         VARCHAR
);

-- -----------------------------------------------------------------------------
-- Load
-- -----------------------------------------------------------------------------
TRUNCATE TABLE PORTS_ACTIVITY;
COPY INTO PORTS_ACTIVITY
  FROM @S3_SILVER/ports_activity/ports/
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FORCE = TRUE;

TRUNCATE TABLE PORTS_REF;
COPY INTO PORTS_REF
  FROM @S3_SILVER/ports_ref/ports/
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FORCE = TRUE;

-- -----------------------------------------------------------------------------
-- Checks (compare with the Glue QUALITY REPORT lines)
-- -----------------------------------------------------------------------------
SELECT COUNT(*)                    AS activity_rows,   -- 5,876,990 at the first load, more after weekly runs
       COUNT(DISTINCT portid)      AS activity_ports,  -- 2,065
       MIN(date)                   AS min_date,        -- 2019-01-01
       MAX(date)                   AS max_date
FROM PORTS_ACTIVITY;

SELECT COUNT(*) AS ref_rows,                           -- 2,065
       COUNT(DISTINCT portid) AS ref_distinct_portid   -- must equal ref_rows
FROM PORTS_REF;

-- Referential check: every activity port exists in ref (expect 0)
SELECT COUNT(DISTINCT a.portid) AS activity_ports_missing_in_ref
FROM PORTS_ACTIVITY a
LEFT JOIN PORTS_REF r ON a.portid = r.portid
WHERE r.portid IS NULL;

CREATE OR REPLACE EXTERNAL TABLE AIS_POSITIONS_EXT (user_id NUMBER(38, 0) AS (VALUE:"user_id"::NUMBER(38, 0)), name TEXT AS (VALUE:"name"::TEXT), latitude REAL AS (VALUE:"latitude"::REAL), longitude REAL AS (VALUE:"longitude"::REAL), sog REAL AS (VALUE:"sog"::REAL), cog REAL AS (VALUE:"cog"::REAL), navigational_status NUMBER(38, 0) AS (VALUE:"navigational_status"::NUMBER(38, 0)), event_time TIMESTAMP_NTZ AS (VALUE:"event_time"::TIMESTAMP_NTZ), cell_id TEXT AS (VALUE:"cell_id"::TEXT), hour TEXT AS (VALUE:"hour"::TEXT), wave_height_m REAL AS (VALUE:"wave_height_m"::REAL), current_velocity_kmh REAL AS (VALUE:"current_velocity_kmh"::REAL), wind_speed_kmh REAL AS (VALUE:"wind_speed_kmh"::REAL), polled_at REAL AS (VALUE:"polled_at"::REAL), year  NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'year=([0-9]+)',  1, 1, 'e', 1))), month NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'month=([0-9]+)', 1, 1, 'e', 1))), day   NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'day=([0-9]+)',   1, 1, 'e', 1)))) LOCATION = @S3_SILVER/positions/ FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FF') TABLE_FORMAT = DELTA AUTO_REFRESH = FALSE REFRESH_ON_CREATE = FALSE;

CREATE OR REPLACE EXTERNAL TABLE AIS_VESSELS_EXT (user_id NUMBER(38, 0) AS (VALUE:"user_id"::NUMBER(38, 0)), name TEXT AS (VALUE:"name"::TEXT), ship_type NUMBER(38, 0) AS (VALUE:"ship_type"::NUMBER(38, 0)), destination TEXT AS (VALUE:"destination"::TEXT), call_sign TEXT AS (VALUE:"call_sign"::TEXT), imo_number NUMBER(38, 0) AS (VALUE:"imo_number"::NUMBER(38, 0)), draught_m REAL AS (VALUE:"draught_m"::REAL), eta_month NUMBER(38, 0) AS (VALUE:"eta_month"::NUMBER(38, 0)), eta_day NUMBER(38, 0) AS (VALUE:"eta_day"::NUMBER(38, 0)), eta_hour NUMBER(38, 0) AS (VALUE:"eta_hour"::NUMBER(38, 0)), eta_minute NUMBER(38, 0) AS (VALUE:"eta_minute"::NUMBER(38, 0)), seen_at TIMESTAMP_NTZ AS (VALUE:"seen_at"::TIMESTAMP_NTZ), is_cargo_or_tanker BOOLEAN AS (VALUE:"is_cargo_or_tanker"::BOOLEAN), year  NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'year=([0-9]+)',  1, 1, 'e', 1))), month NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'month=([0-9]+)', 1, 1, 'e', 1))), day   NUMBER AS (TO_NUMBER(REGEXP_SUBSTR(metadata$filename, 'day=([0-9]+)',   1, 1, 'e', 1)))) LOCATION = @S3_SILVER/vessels/ FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FF') TABLE_FORMAT = DELTA AUTO_REFRESH = FALSE REFRESH_ON_CREATE = FALSE;

DESC TABLE AIS_POSITIONS_EXT;
DESC TABLE AIS_VESSELS_EXT;

CREATE TABLE IF NOT EXISTS AIS_POSITIONS AS
  SELECT * EXCLUDE (value) FROM AIS_POSITIONS_EXT WHERE 1=0;
CREATE TABLE IF NOT EXISTS AIS_VESSELS AS
  SELECT * EXCLUDE (value) FROM AIS_VESSELS_EXT WHERE 1=0;

CREATE OR REPLACE TRANSIENT TABLE AIS_POSITIONS_STG
  DATA_RETENTION_TIME_IN_DAYS = 0
  LIKE AIS_POSITIONS;

CREATE OR REPLACE TRANSIENT TABLE AIS_VESSELS_STG
  DATA_RETENTION_TIME_IN_DAYS = 0
  LIKE AIS_VESSELS;