import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import silver, spark_session

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "vessel_tracking")
PG_USER = os.getenv("POSTGRES_USER", "ais_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "change_this_local_password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
CHECKPOINT_DIR = os.getenv("PG_CHECKPOINT_DIR", silver("_cp"))

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

POSITION_COLUMNS = [
    "user_id", "event_time", "latitude", "longitude", "sog", "cog",
    "navigational_status", "name", "cell_id", "hour", "polled_at",
]

VESSEL_COLUMNS = [
    "user_id", "name", "ship_type", "destination", "call_sign", "imo_number",
    "draught_m", "eta_month", "eta_day", "eta_hour", "eta_minute",
    "is_cargo_or_tanker", "seen_at",
]

LIVE_UPSERT = """
INSERT INTO live_vessels
    (user_id, name, latitude, longitude, sog, cog, navigational_status, event_time)
VALUES %s
ON CONFLICT (user_id) DO UPDATE SET
    name                = EXCLUDED.name,
    latitude            = EXCLUDED.latitude,
    longitude           = EXCLUDED.longitude,
    sog                 = EXCLUDED.sog,
    cog                 = EXCLUDED.cog,
    navigational_status = EXCLUDED.navigational_status,
    event_time          = EXCLUDED.event_time
WHERE live_vessels.event_time IS NULL
   OR EXCLUDED.event_time >= live_vessels.event_time
"""

VESSEL_UPSERT = """
INSERT INTO vessels
    (user_id, name, ship_type, destination, call_sign, imo_number, draught_m,
     eta_month, eta_day, eta_hour, eta_minute, is_cargo_or_tanker, seen_at)
VALUES %s
ON CONFLICT (user_id) DO UPDATE SET
    name               = COALESCE(EXCLUDED.name, vessels.name),
    ship_type          = COALESCE(EXCLUDED.ship_type, vessels.ship_type),
    destination        = COALESCE(EXCLUDED.destination, vessels.destination),
    call_sign          = COALESCE(EXCLUDED.call_sign, vessels.call_sign),
    imo_number         = COALESCE(EXCLUDED.imo_number, vessels.imo_number),
    draught_m          = COALESCE(EXCLUDED.draught_m, vessels.draught_m),
    eta_month          = COALESCE(EXCLUDED.eta_month, vessels.eta_month),
    eta_day            = COALESCE(EXCLUDED.eta_day, vessels.eta_day),
    eta_hour           = COALESCE(EXCLUDED.eta_hour, vessels.eta_hour),
    eta_minute         = COALESCE(EXCLUDED.eta_minute, vessels.eta_minute),
    is_cargo_or_tanker = EXCLUDED.is_cargo_or_tanker,
    seen_at            = GREATEST(vessels.seen_at, EXCLUDED.seen_at)
"""


def connection():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASS)


def upsert(sql, rows):
    if not rows:
        return 0
    with connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    return len(rows)


def align(frame, columns):
    present = set(frame.columns)
    for name in columns:
        if name not in present:
            frame = frame.withColumn(name, F.lit(None))
    return frame


def latest_per_vessel(frame, order_col):
    window = Window.partitionBy("user_id").orderBy(F.col(order_col).desc())
    return (frame
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .drop("rn"))


def positions_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    history = align(batch_df, POSITION_COLUMNS).select(*POSITION_COLUMNS)
    (history.write
     .format("jdbc")
     .option("url", JDBC_URL)
     .option("dbtable", "positions")
     .option("user", PG_USER)
     .option("password", PG_PASS)
     .option("driver", "org.postgresql.Driver")
     .option("batchsize", "2000")
     .mode("append")
     .save())

    latest = latest_per_vessel(batch_df, "event_time").select(
        "user_id", "name", "latitude", "longitude", "sog", "cog",
        "navigational_status", "event_time")
    rows = [tuple(r) for r in latest.collect()]
    moved = upsert(LIVE_UPSERT, rows)

    print(f"[pg] positions batch {batch_id}: +{len(rows)} live, "
          f"{moved} upserted", flush=True)


def vessels_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    latest = align(latest_per_vessel(batch_df, "seen_at"), VESSEL_COLUMNS)
    rows = [tuple(r) for r in latest.select(*VESSEL_COLUMNS).collect()]
    upserted = upsert(VESSEL_UPSERT, rows)

    print(f"[pg] vessels batch {batch_id}: {upserted} upserted", flush=True)


def main():
    spark = spark_session("Silver-Delta-to-Postgres", extra_conf=DELTA_CONF)

    positions = (
        spark.readStream.format("delta").load(silver("positions"))
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/pg_positions")
        .trigger(processingTime="10 seconds")
        .foreachBatch(positions_batch)
        .start()
    )

    vessels = (
        spark.readStream.format("delta").load(silver("vessels"))
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/pg_vessels")
        .trigger(processingTime="10 seconds")
        .foreachBatch(vessels_batch)
        .start()
    )

    print("postgres sink started: positions + live_vessels + vessels", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()