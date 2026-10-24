import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

from collectors import config

LAKE = config.LAKE_ROOT
BRONZE = f"{LAKE}/bronze"
WEATHER_TABLE = f"{LAKE}/weather_hourly"
SILVER = f"{LAKE}/silver/live_telemetry"
CHECKPOINTS = f"{LAKE}/checkpoints"

STATIC_CACHE = {}
STATIC_LIMIT = 50000

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
            StructField("Eta", StringType()),
        ])),
        StructField("StaticDataReport", StructType([
            StructField("PartA", StructType([StructField("Name", StringType())])),
            StructField("PartB", StructType([
                StructField("ShipType", LongType()),
                StructField("Destination", StringType()),
            ])),
        ])),
    ])),
])

WEATHER_SCHEMA = StructType([
    StructField("cell_id", StringType()),
    StructField("hour", StringType()),
    StructField("wave_height_m", DoubleType()),
    StructField("current_velocity_kmh", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("polled_at", LongType()),
])

NAVIGATION_STATUS = {
    0: "under way using engine", 1: "at anchor", 2: "not under command",
    3: "restricted manoeuvrability", 4: "constrained by draught", 5: "moored",
    6: "aground", 7: "fishing", 8: "under way sailing", 11: "towing astern",
    12: "pushing ahead", 14: "AIS-SART", 15: "undefined",
}


def write_bronze(batch_df, name):
    (batch_df
     .selectExpr("topic", "CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value",
                 "timestamp AS kafka_timestamp")
     .withColumn("ingest_date", F.to_date("kafka_timestamp"))
     .write.mode("append")
     .partitionBy("ingest_date")
     .parquet(f"{BRONZE}/{name}"))


def update_static(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    write_bronze(batch_df, "ais_static")

    rows = (batch_df
            .select(F.from_json(F.col("value").cast("string"), STATIC_SCHEMA).alias("m"))
            .select(
                F.col("m.MetaData.MMSI").cast("string").alias("mmsi"),
                F.coalesce(
                    F.col("m.Message.ShipStaticData.Name"),
                    F.col("m.Message.StaticDataReport.PartA.Name"),
                ).alias("name"),
                F.coalesce(
                    F.col("m.Message.ShipStaticData.Type"),
                    F.col("m.Message.StaticDataReport.PartB.ShipType"),
                ).alias("ship_type"),
                F.coalesce(
                    F.col("m.Message.ShipStaticData.Destination"),
                    F.col("m.Message.StaticDataReport.PartB.Destination"),
                ).alias("destination"),
                F.col("m.Message.ShipStaticData.Eta").alias("eta"),
            )
            .filter(F.col("mmsi").isNotNull())
            .collect())

    for row in rows:
        if len(STATIC_CACHE) >= STATIC_LIMIT:
            break
        STATIC_CACHE[row.mmsi] = {
            "name": (row.name or "").strip() or None,
            "ship_type": row.ship_type,
            "destination": (row.destination or "").strip() or None,
            "eta": row.eta,
        }

    print(f"[static] cache now holds {len(STATIC_CACHE)} vessels", flush=True)


def write_weather(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    write_bronze(batch_df, "weather")

    (batch_df
     .select(F.from_json(F.col("value").cast("string"), WEATHER_SCHEMA).alias("w"))
     .select("w.*")
     .filter(F.col("cell_id").isNotNull())
     .write.mode("append")
     .parquet(WEATHER_TABLE))


def load_weather(spark):
    try:
        frame = spark.read.parquet(WEATHER_TABLE)
    except Exception:
        return None

    if frame.rdd.isEmpty():
        return None

    window = Window.partitionBy("cell_id", "hour").orderBy(F.col("polled_at").desc())
    return (frame
            .withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            .select("cell_id", "hour",
                    F.col("wind_speed_kmh").alias("wind_speed"),
                    F.col("wave_height_m").alias("wave_height_meters"),
                    F.col("current_velocity_kmh").alias("ocean_current_velocity")))


def build_silver(spark, batch_df):
    nav_map = F.create_map(*[
        item for code, label in NAVIGATION_STATUS.items()
        for item in (F.lit(code), F.lit(label))
    ])

    parsed = (batch_df
              .select(F.from_json(F.col("value").cast("string"), POSITION_SCHEMA).alias("m"))
              .select(
                  F.col("m.MetaData.MMSI").cast("string").alias("mmsi"),
                  F.trim(F.col("m.MetaData.ShipName")).alias("ais_name"),
                  F.col("m.Message.PositionReport.Latitude").alias("latitude"),
                  F.col("m.Message.PositionReport.Longitude").alias("longitude"),
                  F.col("m.Message.PositionReport.Sog").alias("speed_over_ground_knots"),
                  F.col("m.Message.PositionReport.Cog").alias("course_over_ground"),
                  nav_map[F.col("m.Message.PositionReport.NavigationalStatus").cast("int")]
                      .alias("navigational_status"),
                  F.to_timestamp(
                      F.substring(F.col("m.MetaData.time_utc"), 1, 19),
                      "yyyy-MM-dd HH:mm:ss",
                  ).alias("event_ts"),
              )
              .filter(F.col("mmsi").isNotNull() & F.col("event_ts").isNotNull()))

    if STATIC_CACHE:
        static = spark.createDataFrame([
            (mmsi, data["name"], data["ship_type"], data["destination"], data["eta"])
            for mmsi, data in STATIC_CACHE.items()
        ], "mmsi string, static_name string, ship_type long, destination_port string, eta_raw string")
        parsed = parsed.join(static, "mmsi", "left")
    else:
        parsed = (parsed
                  .withColumn("static_name", F.lit(None).cast("string"))
                  .withColumn("ship_type", F.lit(None).cast("long"))
                  .withColumn("destination_port", F.lit(None).cast("string"))
                  .withColumn("eta_raw", F.lit(None).cast("string")))

    enriched = parsed.select(
        "mmsi",
        F.coalesce("static_name", "ais_name").alias("ship_name"),
        "ship_type",
        "navigational_status",
        "latitude",
        "longitude",
        "speed_over_ground_knots",
        "course_over_ground",
        "destination_port",
        "eta_raw",
        F.col("event_ts").alias("timestamp"),
    )

    enriched = (enriched
                .withColumn("cell_id", F.concat(
                    F.floor(F.col("latitude") / config.GRID_DEG).cast("int").cast("string"),
                    F.lit(":"),
                    F.floor(F.col("longitude") / config.GRID_DEG).cast("int").cast("string")))
                .withColumn("hour", F.date_format("timestamp", "yyyy-MM-dd HH:00:00")))

    weather = load_weather(spark)
    if weather is not None:
        enriched = (enriched.join(weather, ["cell_id", "hour"], "left")
                    .drop("cell_id", "hour"))
    else:
        enriched = (enriched
                    .withColumn("wind_speed", F.lit(None).cast("double"))
                    .withColumn("wave_height_meters", F.lit(None).cast("double"))
                    .withColumn("ocean_current_velocity", F.lit(None).cast("double"))
                    .drop("cell_id", "hour"))

    return (enriched
            .withColumn("year", F.year("timestamp"))
            .withColumn("month", F.month("timestamp"))
            .withColumn("day", F.dayofmonth("timestamp")))


def write_silver(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    write_bronze(batch_df, "ais_positions")

    spark = batch_df.sparkSession
    silver = build_silver(spark, batch_df)

    (silver.write.mode("append")
     .partitionBy("year", "month", "day")
     .parquet(SILVER))

    print(f"[silver] batch {batch_id}: {silver.count()} rows written", flush=True)


def main():
    spark = (SparkSession.builder
             .appName("maritime-lake")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def stream(topics):
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
                .option("subscribe", topics)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load())

    queries = [
        stream(config.TOPIC_STATIC).writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/static")
        .trigger(processingTime="10 seconds")
        .foreachBatch(update_static).start(),

        stream(config.TOPIC_WEATHER).writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/weather")
        .trigger(processingTime="10 seconds")
        .foreachBatch(write_weather).start(),

        stream(config.TOPIC_AIS).writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/silver")
        .trigger(processingTime="10 seconds")
        .foreachBatch(write_silver).start(),
    ]

    print(f"streaming queries started: {len(queries)}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
