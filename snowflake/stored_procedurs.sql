CREATE OR REPLACE PROCEDURE LOAD_PORTS_ACTIVITY(LOOKBACK_DAYS NUMBER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  m      DATE;
  end_m  DATE;
  n      NUMBER;
  cmd    STRING;
BEGIN
  -- 1. detect the months the window touches
  m     := DATE_TRUNC('month', DATEADD(day, -:LOOKBACK_DAYS, CURRENT_DATE()));
  end_m := DATE_TRUNC('month', CURRENT_DATE());

  TRUNCATE TABLE PORTS_ACTIVITY_STG;

  WHILE (m <= end_m) DO
    cmd := 'COPY INTO PORTS_ACTIVITY_STG
            FROM @S3_SILVER/ports_activity/ports/year=' || YEAR(m) || '/month=' || MONTH(m) || '/
            PATTERN = ''.*[.]parquet''
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE';
    EXECUTE IMMEDIATE :cmd;
    m := DATEADD(month, 1, m);
  END WHILE;

  -- 2. safety guard
  SELECT COUNT(*) INTO :n FROM PORTS_ACTIVITY_STG;
  IF (n = 0) THEN
    RETURN 'ABORT: staging is empty, nothing changed';
  END IF;

  -- 3. atomic replace
  BEGIN TRANSACTION;
  DELETE FROM PORTS_ACTIVITY
  WHERE date >= (SELECT DATE_TRUNC('month', MIN(date)) FROM PORTS_ACTIVITY_STG);
  INSERT INTO PORTS_ACTIVITY SELECT * FROM PORTS_ACTIVITY_STG;
  COMMIT;

  RETURN 'OK: ' || n || ' rows';
END;
$$;

CREATE OR REPLACE PROCEDURE LOAD_AIS_DATA(LOOKBACK_DAYS NUMBER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  cutoff_num NUMBER;
  n_pos      NUMBER;
  n_ves      NUMBER;
BEGIN
  cutoff_num := TO_NUMBER(TO_CHAR(DATEADD(day, -:LOOKBACK_DAYS, CURRENT_DATE()), 'YYYYMMDD'));

  ALTER EXTERNAL TABLE AIS_POSITIONS_EXT REFRESH;
  ALTER EXTERNAL TABLE AIS_VESSELS_EXT REFRESH;

  TRUNCATE TABLE AIS_POSITIONS_STG;
  INSERT INTO AIS_POSITIONS_STG
    SELECT * EXCLUDE (value) FROM AIS_POSITIONS_EXT
    WHERE year*10000 + month*100 + day >= :cutoff_num;

  TRUNCATE TABLE AIS_VESSELS_STG;
  INSERT INTO AIS_VESSELS_STG
    SELECT * EXCLUDE (value) FROM AIS_VESSELS_EXT
    WHERE year*10000 + month*100 + day >= :cutoff_num;

  SELECT COUNT(*) INTO :n_pos FROM AIS_POSITIONS_STG;
  SELECT COUNT(*) INTO :n_ves FROM AIS_VESSELS_STG;
  IF (n_pos = 0 OR n_ves = 0) THEN
    RETURN 'ABORT: staging empty (positions=' || n_pos || ', vessels=' || n_ves || '), nothing changed';
  END IF;

  BEGIN TRANSACTION;
    DELETE FROM AIS_POSITIONS WHERE year*10000 + month*100 + day >= :cutoff_num;
    INSERT INTO AIS_POSITIONS SELECT * FROM AIS_POSITIONS_STG;

    DELETE FROM AIS_VESSELS WHERE year*10000 + month*100 + day >= :cutoff_num;
    INSERT INTO AIS_VESSELS SELECT * FROM AIS_VESSELS_STG;
  COMMIT;

  RETURN 'OK: positions=' || n_pos || ', vessels=' || n_ves;
END;
$$;