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