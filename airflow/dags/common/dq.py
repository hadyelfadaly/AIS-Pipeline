from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_default"
LOOKBACK = 14

PORTS_WIN = f"date >= DATE_TRUNC('month', DATEADD(day, -{LOOKBACK}, CURRENT_DATE()))"   # same months the procedure reloads
AIS_DAY = "DATE_FROM_PARTS(year, month, day)"
AIS_WIN = f"{AIS_DAY} >= DATEADD(day, -{LOOKBACK}, CURRENT_DATE())"
AIS_NUM = "year*10000 + month*100 + day"

PORTS_CHECKS = {
    "window has rows": f"SELECT IFF(COUNT(*) = 0, 1, 0) FROM PORTS_ACTIVITY WHERE {PORTS_WIN}",
    "target matches staging": """
        SELECT ABS((SELECT COUNT(*) FROM PORTS_ACTIVITY_STG)
                 - (SELECT COUNT(*) FROM PORTS_ACTIVITY
                    WHERE date >= (SELECT DATE_TRUNC('month', MIN(date)) FROM PORTS_ACTIVITY_STG)))""",
    "newest date within 14 days": "SELECT IFF(MAX(date) < DATEADD(day, -14, CURRENT_DATE()), 1, 0) FROM PORTS_ACTIVITY",
    "no future dates": "SELECT COUNT(*) FROM PORTS_ACTIVITY WHERE date > CURRENT_DATE()",
    "null keys": f"SELECT COUNT(*) FROM PORTS_ACTIVITY WHERE {PORTS_WIN} AND (portid IS NULL OR date IS NULL)",
    "duplicate (portid, date)": f"""
        SELECT COUNT(*) FROM (SELECT 1 FROM PORTS_ACTIVITY WHERE {PORTS_WIN}
                              GROUP BY portid, date HAVING COUNT(*) > 1)""",
    "negative counts or volumes": f"""
        SELECT COUNT(*) FROM PORTS_ACTIVITY WHERE {PORTS_WIN}
          AND (portcalls_total < 0 OR import_total < 0 OR export_total < 0)""",
    "missing days in window": f"""
        SELECT DATEDIFF(day, MIN(date), MAX(date)) + 1 - COUNT(DISTINCT date)
        FROM PORTS_ACTIVITY WHERE {PORTS_WIN}""",
        "days with under 50% of average rows": f"""
        WITH d AS (SELECT date, COUNT(*) AS n FROM PORTS_ACTIVITY
                   WHERE {PORTS_WIN} AND date < DATEADD(day, -{LOOKBACK}, CURRENT_DATE()) GROUP BY date),
             a AS (SELECT AVG(n) AS avg_n FROM d)
        SELECT COUNT(*) FROM d, a WHERE d.n < 0.5 * a.avg_n""",
}

AIS_CHECKS = {
    "positions: window has rows": f"SELECT IFF(COUNT(*) = 0, 1, 0) FROM AIS_POSITIONS WHERE {AIS_WIN}",
    "vessels: window has rows": f"SELECT IFF(COUNT(*) = 0, 1, 0) FROM AIS_VESSELS WHERE {AIS_WIN}",
    "positions: target matches staging": f"""
        SELECT ABS((SELECT COUNT(*) FROM AIS_POSITIONS_STG)
                 - (SELECT COUNT(*) FROM AIS_POSITIONS
                    WHERE {AIS_NUM} >= (SELECT MIN({AIS_NUM}) FROM AIS_POSITIONS_STG)))""",
    "vessels: target matches staging": f"""
        SELECT ABS((SELECT COUNT(*) FROM AIS_VESSELS_STG)
                 - (SELECT COUNT(*) FROM AIS_VESSELS
                    WHERE {AIS_NUM} >= (SELECT MIN({AIS_NUM}) FROM AIS_VESSELS_STG)))""",
    "positions: stream is fresh (2 days)": f"SELECT IFF(MAX({AIS_DAY}) < DATEADD(day, -2, CURRENT_DATE()), 1, 0) FROM AIS_POSITIONS",
    "vessels: stream is fresh (2 days)": f"SELECT IFF(MAX({AIS_DAY}) < DATEADD(day, -2, CURRENT_DATE()), 1, 0) FROM AIS_VESSELS",
        "positions: days with no data (gap inside the data range)": f"""
        WITH have AS (SELECT DISTINCT {AIS_DAY} AS d FROM AIS_POSITIONS WHERE {AIS_WIN})
        SELECT DATEDIFF(day, MIN(d), MAX(d)) + 1 - COUNT(*) FROM have""",
    "positions: over 1% null keys or bad coordinates": f"""
        SELECT IFF(AVG(IFF(user_id IS NULL OR event_time IS NULL OR latitude IS NULL OR longitude IS NULL
                           OR latitude NOT BETWEEN -90 AND 90
                           OR longitude NOT BETWEEN -180 AND 180, 1, 0)) > 0.01, 1, 0)
        FROM AIS_POSITIONS WHERE {AIS_WIN}""",
    "positions: over 1% bad speed or course": f"""
        SELECT IFF(AVG(IFF(sog < 0 OR sog > 102.2 OR cog < 0 OR cog > 360, 1, 0)) > 0.01, 1, 0)
        FROM AIS_POSITIONS WHERE {AIS_WIN}""",
    "positions: event_time in the future": f"""
        SELECT COUNT(*) FROM AIS_POSITIONS
        WHERE {AIS_WIN} AND event_time > DATEADD(day, 1, CURRENT_TIMESTAMP())""",
        "positions: duplicates in Delta source (Spark dedupe broken)": f"""
        SELECT COUNT(*) FROM (SELECT 1 FROM AIS_POSITIONS_EXT
                              WHERE year*10000 + month*100 + day >= TO_NUMBER(TO_CHAR(DATEADD(day, -{LOOKBACK}, CURRENT_DATE()), 'YYYYMMDD'))
                              GROUP BY user_id, event_time HAVING COUNT(*) > 1)""",
    "vessels: null user_id": f"SELECT COUNT(*) FROM AIS_VESSELS WHERE {AIS_WIN} AND user_id IS NULL",
    "vessels: over 1% impossible draught": f"""
        SELECT IFF(AVG(IFF(draught_m < 0 OR draught_m > 25.5, 1, 0)) > 0.01, 1, 0)
        FROM AIS_VESSELS WHERE {AIS_WIN}""",
}


def run_checks(checks):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    failures = []
    for name, sql in checks.items():
        bad = hook.get_first(sql)[0]
        print(f"{'FAIL' if bad else 'ok  '} {name}: {bad}", flush=True)
        if bad:
            failures.append(f"{name} ({bad})")
    if failures:
        raise ValueError("Data quality failed: " + "; ".join(failures))