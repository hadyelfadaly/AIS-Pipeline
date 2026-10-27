
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# Configuration
bucket = os.getenv("S3_BUCKET", "g9-ais-project")
kafka_bootstrap_servers = "kafka:29092"
topic = "position_report"

output_path = (
    f"s3a://{bucket}/bronze/position_report/data/"
)

checkpoint_path = (
    f"s3a://{bucket}/bronze/position_report/checkpoint/"
)


# Create Spark Session
spark = (
    SparkSession.builder
    .appName("AIS-Bronze-Position-Stream")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "s3.eu-north-1.amazonaws.com"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read messages from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)


# Preserve the original Kafka message and its metadata
bronze_stream = (
    kafka_stream.select(
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processed_at")
    )
    .coalesce(1)
)


# Write raw messages to S3 Bronze
query = (
    bronze_stream.writeStream
    .format("json")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark Streaming started")
print("Kafka topic:", topic)
print("S3 output:", output_path)
print("Checkpoint:", checkpoint_path)

query.awaitTermination()
