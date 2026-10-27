from pyspark.sql.functions import col, current_timestamp

from common import (
    KAFKA_BOOTSTRAP,
    TOPIC_STATIC,
    bronze,
    bronze_checkpoint,
    spark_session,
)

spark = spark_session("AIS-Bronze-Static-Stream")

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_STATIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 2000)
    .load()
)

bronze_stream = stream.select(
    col("value").cast("string").alias("raw_json"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("processed_at"),
)

query = (
    bronze_stream.writeStream
    .format("json")
    .outputMode("append")
    .option("path", bronze("ship_static_data"))
    .option("checkpointLocation", bronze_checkpoint("ship_static_data"))
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"static bronze stream started -> {bronze('ship_static_data')}")
query.awaitTermination()
