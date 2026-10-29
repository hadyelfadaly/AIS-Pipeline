"""Recovery path: rebuild silver/positions and silver/vessels from bronze.

The streaming job (silver_stream.py) is the normal path. This batch job exists
because the Kafka topics keep only one hour of data; if the stream is down
longer than that, bronze is the only place the messages still exist.

It reuses the same cleaning functions as the stream, so the output shape is
identical. Run it after an outage, or to reprocess history.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from streaming.spark.common import bronze, silver, spark_session
from streaming.spark.silver_stream import clean_positions, clean_static, clean_weather

RAW_SCHEMA = StructType([
    StructField("raw_json", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", LongType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", StringType()),
    StructField("processed_at", StringType()),
])


def read_bronze(spark, name):
    return spark.read.schema(RAW_SCHEMA).json(f"{bronze(name)}/part-*.json")


def rebuild_positions(spark):
    positions = (clean_positions(read_bronze(spark, "position_report"))
                 .dropDuplicates(["user_id", "event_time"]))

    weather = clean_weather(read_bronze(spark, "weather"))
    if not weather.rdd.isEmpty():
        # bronze keeps every poll, so the same (cell, hour) appears many times.
        # Keep the newest row per key or the join multiplies positions.
        latest = Window.partitionBy("cell_id", "hour").orderBy(F.col("polled_at").desc())
        weather = (weather
                   .withColumn("_rn", F.row_number().over(latest))
                   .filter(F.col("_rn") == 1)
                   .drop("_rn"))
    if not weather.rdd.isEmpty():
        positions = positions.join(weather, ["cell_id", "hour"], "left")
    else:
        for name in ("wave_height_m", "current_velocity_kmh",
                     "wind_speed_kmh", "polled_at"):
            positions = positions.withColumn(name, F.lit(None).cast("double"))

    (positions
     .withColumn("year", F.year("event_time"))
     .withColumn("month", F.month("event_time"))
     .withColumn("day", F.dayofmonth("event_time"))
     .write.mode("overwrite")
     .partitionBy("year", "month", "day")
     .parquet(silver("positions")))

    print(f"positions rebuilt: {positions.count()} rows", flush=True)


def rebuild_vessels(spark):
    vessels = clean_static(read_bronze(spark, "ship_static_data"))

    (vessels
     .withColumn("year", F.year("seen_at"))
     .withColumn("month", F.month("seen_at"))
     .withColumn("day", F.dayofmonth("seen_at"))
     .write.mode("overwrite")
     .partitionBy("year", "month", "day")
     .parquet(silver("vessels")))

    print(f"vessels rebuilt: {vessels.count()} rows", flush=True)


def main():
    spark = spark_session("AIS-Silver-Rebuild")
    rebuild_vessels(spark)
    rebuild_positions(spark)
    spark.stop()


if __name__ == "__main__":
    main()
