from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common import (
    GRID_SIZE,
    bronze,
    silver,
    spark_session,
)

# AIS ship-type codes kept in silver: 70-79 cargo, 80-89 tanker.
# Vessels whose type is unknown (no static message, or code 0) are excluded.
ALLOWED_SHIP_TYPE_MIN = 70
ALLOWED_SHIP_TYPE_MAX = 89

BRONZE_SCHEMA = StructType([
    StructField("raw_json", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", LongType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", StringType()),
    StructField("processed_at", StringType()),
])

POSITION_SCHEMA = StructType([
    StructField("MetaData", StructType([
        StructField("MMSI", LongType()),
        StructField("ShipName", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("time_utc", StringType()),
    ])),
    StructField("Message", StructType([
        StructField("PositionReport", StructType([
            StructField("Latitude", DoubleType()),
            StructField("Longitude", DoubleType()),
            StructField("Sog", DoubleType()),
            StructField("Cog", DoubleType()),
            StructField("TrueHeading", LongType()),
            StructField("NavigationalStatus", LongType()),
        ])),
    ])),
])

STATIC_SCHEMA = StructType([
    StructField("MetaData", StructType([StructField("MMSI", LongType())])),
    StructField("Message", StructType([
        StructField("ShipStaticData", StructType([
            StructField("Name", StringType()),
            StructField("Type", LongType()),
            StructField("Destination", StringType()),
            StructField("Eta", StructType([
                StructField("Month", LongType()),
                StructField("Day", LongType()),
                StructField("Hour", LongType()),
                StructField("Minute", LongType()),
            ])),
        ])),
    ])),
])

WEATHER_SCHEMA = StructType([
    StructField("cell_id", StringType()),
    StructField("cell_lat", DoubleType()),
    StructField("cell_lon", DoubleType()),
    StructField("hour", StringType()),
    StructField("wave_height_m", DoubleType()),
    StructField("current_velocity_kmh", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("polled_at", LongType()),
])


def cell_id_expression(lat, lon):
    grid_lat = F.floor(lat / GRID_SIZE) * GRID_SIZE + GRID_SIZE / 2
    grid_lon = F.floor(lon / GRID_SIZE) * GRID_SIZE + GRID_SIZE / 2
    return F.concat(
        F.format_number(grid_lat, 2),
        F.lit("_"),
        F.format_number(grid_lon, 2),
    )


def hour_expression(ts):
    return F.concat(
        F.date_format(ts, "yyyy-MM-dd"),
        F.lit("T"),
        F.date_format(ts, "HH"),
        F.lit(":00"),
    )


def parse_positions(batch_df):
    parsed = (
        batch_df
        .select(F.from_json(F.col("raw_json"), POSITION_SCHEMA).alias("m"))
        .select(
            F.col("m.MetaData.MMSI").cast("long").alias("user_id"),
            F.col("m.MetaData.ShipName").alias("ais_name"),
            F.col("m.Message.PositionReport.Latitude").alias("latitude"),
            F.col("m.Message.PositionReport.Longitude").alias("longitude"),
            F.col("m.Message.PositionReport.Sog").alias("sog"),
            F.col("m.Message.PositionReport.Cog").alias("cog"),
            F.col("m.Message.PositionReport.NavigationalStatus")
                .alias("navigational_status"),
            F.to_timestamp(
                F.substring(F.col("m.MetaData.time_utc"), 1, 19),
                "yyyy-MM-dd HH:mm:ss",
            ).alias("time_utc"),
        )
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("time_utc").isNotNull())
        .filter(F.col("latitude").between(-90, 90))
        .filter(F.col("longitude").between(-180, 180))
        .filter(F.col("latitude") != 91)
        .filter(F.col("longitude") != 181)
    )

    return parsed.dropDuplicates(["user_id", "time_utc"]).withColumn(
        "sog",
        F.when(F.col("sog").between(-5, 60), F.col("sog")).otherwise(F.lit(None)),
    ).withColumn(
        "cog",
        F.when(F.col("cog").between(0, 359.9), F.col("cog")).otherwise(F.lit(None)),
    )


def load_static(spark):
    try:
        raw = spark.read.schema(BRONZE_SCHEMA).json(f"{bronze('ship_static_data')}/part-*.json")
    except Exception:
        return None
    if raw.rdd.isEmpty():
        return None

    parsed = (
        raw.select(
            F.from_json(F.col("raw_json"), STATIC_SCHEMA).alias("m"),
            F.col("kafka_timestamp"),
        )
        .select(
            F.col("m.MetaData.MMSI").cast("long").alias("user_id"),
            F.trim(F.col("m.Message.ShipStaticData.Name")).alias("name"),
            F.col("m.Message.ShipStaticData.Type").alias("ship_type"),
            F.trim(F.col("m.Message.ShipStaticData.Destination")).alias("destination"),
            F.col("m.Message.ShipStaticData.Eta.Month").alias("eta_month"),
            F.col("m.Message.ShipStaticData.Eta.Day").alias("eta_day"),
            F.col("m.Message.ShipStaticData.Eta.Hour").alias("eta_hour"),
            F.col("m.Message.ShipStaticData.Eta.Minute").alias("eta_minute"),
            F.col("kafka_timestamp"),
        )
        .filter(F.col("user_id").isNotNull())
    )

    latest = Window.partitionBy("user_id").orderBy(F.col("kafka_timestamp").desc())
    return (
        parsed.withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn", "kafka_timestamp")
    )


def load_weather(spark):
    try:
        raw = spark.read.schema(BRONZE_SCHEMA).json(f"{bronze('weather')}/part-*.json")
    except Exception:
        return None
    if raw.rdd.isEmpty():
        return None

    parsed = (
        raw.select(F.from_json(F.col("raw_json"), WEATHER_SCHEMA).alias("w"))
        .select("w.*")
        .filter(F.col("cell_id").isNotNull())
        .filter(F.col("hour").isNotNull())
    )

    latest = Window.partitionBy("cell_id", "hour").orderBy(F.col("polled_at").desc())
    return (
        parsed.withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "cell_id",
            "hour",
            "wave_height_m",
            "current_velocity_kmh",
            "wind_speed_kmh",
            "polled_at",
        )
    )


def build_silver(spark, batch_df):
    positions = parse_positions(batch_df)

    positions = (
        positions
        .withColumn("cell_id", cell_id_expression(F.col("latitude"), F.col("longitude")))
        .withColumn("hour", hour_expression(F.col("time_utc")))
    )

    static = load_static(spark)
    if static is not None:
        positions = positions.join(static, "user_id", "left")
    else:
        for name in ("name", "ship_type", "destination",
                     "eta_month", "eta_day", "eta_hour", "eta_minute"):
            positions = positions.withColumn(name, F.lit(None))

    weather = load_weather(spark)
    if weather is not None:
        positions = positions.join(weather, ["cell_id", "hour"], "left")
    else:
        for name in ("wave_height_m", "current_velocity_kmh",
                     "wind_speed_kmh", "polled_at"):
            positions = positions.withColumn(name, F.lit(None))

    return (
        positions
        .withColumn("name", F.coalesce(F.col("name"), F.col("ais_name")))
        .select(
            "user_id",
            F.col("time_utc").alias("event_time"),
            "latitude",
            "longitude",
            "sog",
            "cog",
            "navigational_status",
            "name",
            "ship_type",
            "destination",
            "eta_month",
            "eta_day",
            "eta_hour",
            "eta_minute",
            "cell_id",
            "hour",
            "wave_height_m",
            "current_velocity_kmh",
            "wind_speed_kmh",
            "polled_at",
        )
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
        .filter(F.col("ship_type").between(ALLOWED_SHIP_TYPE_MIN, ALLOWED_SHIP_TYPE_MAX))
    )


def main():
    spark = spark_session("AIS-Silver-Builder")

    bronze_df = spark.read.schema(BRONZE_SCHEMA).json(f"{bronze('position_report')}/part-*.json")
    bronze_rows = bronze_df.count()

    result = build_silver(spark, bronze_df).persist()
    silver_rows = result.count()

    (
        result.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(silver("live_telemetry"))
    )

    print(f"bronze position rows : {bronze_rows}")
    print(f"silver rows written  : {silver_rows}")
    print(f"silver location      : {silver('live_telemetry')}")

    result.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
