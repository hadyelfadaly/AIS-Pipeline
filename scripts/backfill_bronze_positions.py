from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

import sys, os
sys.path.insert(0, "/home/ahmed/AIS-Pipeline/spark")
from common import KAFKA_BOOTSTRAP, TOPIC_POSITION, bronze, spark_session

BRONZE_SCHEMA = StructType([
    StructField("raw_json", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", LongType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", StringType()),
    StructField("processed_at", StringType()),
])

spark = spark_session("AIS-Bronze-Position-Backfill")

existing = (spark.read.schema(BRONZE_SCHEMA).json(f"{bronze(TOPIC_POSITION)}/part-*.json")
            .select("kafka_partition", "kafka_offset").distinct())

topic = (spark.read.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("subscribe", TOPIC_POSITION)
         .option("startingOffsets", "earliest")
         .option("endingOffsets", "latest")
         .load()
         .select(col := F.col("partition").alias("kafka_partition"),
                 F.col("offset").alias("kafka_offset"),
                 F.col("topic").alias("kafka_topic"),
                 F.col("value").cast("string").alias("raw_json"),
                 F.col("timestamp").cast("string").alias("kafka_timestamp"))
         .withColumn("processed_at", F.current_timestamp().cast("string")))

missing = topic.join(existing, ["kafka_partition", "kafka_offset"], "left_anti")

print(f"already in bronze : {existing.count()}")
print(f"in topic          : {topic.count()}")
print(f"missing           : {missing.count()}")

(missing.write.mode("append").json(f"{bronze(TOPIC_POSITION)}"))
print("backfill written")
spark.stop()
