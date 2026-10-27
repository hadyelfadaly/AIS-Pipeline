from pyspark.sql.functions import col, current_timestamp

from common import (
    KAFKA_BOOTSTRAP,
    TOPIC_WEATHER,
    bronze,
    bronze_checkpoint,
    spark_session,
)

spark = spark_session("AIS-Bronze-Weather-Stream")

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_WEATHER)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
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
    .option("path", bronze("weather"))
    .option("checkpointLocation", bronze_checkpoint("weather"))
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"weather bronze stream started -> {bronze('weather')}")
query.awaitTermination()
