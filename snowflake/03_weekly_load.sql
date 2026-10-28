USE ROLE DBT_ROLE; USE WAREHOUSE AIS_WH; USE SCHEMA AIS.RAW;
CREATE TRANSIENT TABLE IF NOT EXISTS PORTS_ACTIVITY_STG LIKE PORTS_ACTIVITY;

-- 1. load the months the Glue job just rewrote (current month, plus the previous one if the window crossed a month boundary)
TRUNCATE TABLE PORTS_ACTIVITY_STG;

COPY INTO PORTS_ACTIVITY_STG
  FROM @S3_SILVER/ports_activity/ports/year=2026/month=10/
  PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

-- add one COPY per extra month, e.g. .../year=2026/month=9/

-- 2. replace those months atomically, with a safety guard
EXECUTE IMMEDIATE $$
DECLARE n NUMBER;
BEGIN
  SELECT COUNT(*) INTO :n FROM PORTS_ACTIVITY_STG;
  IF (n = 0) THEN
    RETURN 'ABORT: staging is empty, nothing changed';
  END IF;
  BEGIN TRANSACTION;
  DELETE FROM PORTS_ACTIVITY
   WHERE date >= (SELECT DATE_TRUNC('month', MIN(date)) FROM PORTS_ACTIVITY_STG);
  INSERT INTO PORTS_ACTIVITY SELECT * FROM PORTS_ACTIVITY_STG;
  COMMIT;
  RETURN 'OK: ' || n || ' rows';
END;
$$;