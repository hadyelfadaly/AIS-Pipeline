import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import to_utc_timestamp

from common import silver, spark_session

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "vessel_tracking")
PG_USER = os.getenv("POSTGRES_USER", "ais_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "change_this_local_password")

SOURCE_TZ = os.getenv("SOURCE_TZ", "Africa/Cairo")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

POSITION_COLUMNS = [
    "user_id", "event_time", "latitude", "longitude", "sog", "cog",
    "navigational_status", "name", "cell_id", "hour",
    "wave_height_m", "current_velocity_kmh", "wind_speed_kmh", "polled_at",
]

VESSEL_COLUMNS = [
    "user_id", "name", "ship_type", "destination", "call_sign", "imo_number",
    "draught_m", "eta_month", "eta_day", "eta_hour", "eta_minute",
    "is_cargo_or_tanker", "seen_at",
]

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def align(frame, columns):
    """Select the columns the Postgres table needs, nulling any that are absent."""
    present = set(frame.columns)
    for name in columns:
        if name not in present:
            frame = frame.withColumn(name, F.lit(None))
    return frame.select(*columns)


def write_table(frame, table):
    (frame.write
     .format("jdbc")
     .option("url", JDBC_URL)
     .option("dbtable", table)
     .option("user", PG_USER)
     .option("password", PG_PASS)
     .option("driver", "org.postgresql.Driver")
     .option("batchsize", "5000")
     .mode("append")
     .save())


def main():
    spark = spark_session("Silver-to-Postgres", extra_conf=DELTA_CONF)

    positions = align(spark.read.format("delta").load(silver("positions")),
                      POSITION_COLUMNS)
    positions = positions.withColumn("event_time",
                                     to_utc_timestamp("event_time", SOURCE_TZ))

    vessels = spark.read.format("delta").load(silver("vessels"))
    vessels = vessels.withColumn("seen_at", to_utc_timestamp("seen_at", SOURCE_TZ))
    latest = Window.partitionBy("user_id").orderBy(F.col("seen_at").desc())
    vessels = align(
        vessels
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn"),
        VESSEL_COLUMNS,
    )

    position_rows = positions.count()
    vessel_rows = vessels.count()
    print(f"positions rows : {position_rows}")
    print(f"vessels rows   : {vessel_rows}")

    write_table(vessels, "vessels")
    write_table(positions, "positions")

    print("load finished", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
