
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("AIS-S3-Connection-Test")
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

data = [
    (1, "AIS-Pipeline", "S3 connection test"),
    (2, "Spark", "Writing data to AWS S3"),
]

df = spark.createDataFrame(
    data,
    ["id", "project", "message"]
).withColumn(
    "created_at",
    current_timestamp()
)

bucket = os.getenv("S3_BUCKET", "g9-ais-project")
output_path = f"s3a://{bucket}/bronze/test/"

print("Writing test data to:", output_path)

try:
    df.write.mode("append").json(output_path)
    print("S3 write test completed successfully!")
finally:
    spark.stop()
