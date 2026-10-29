import os
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUCKET = os.getenv("S3_BUCKET", "g9-ais-project")
REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

TOPIC_POSITION = "position_report"
TOPIC_STATIC = "ship_static_data"
TOPIC_WEATHER = "weather_data"

GRID_SIZE = float(os.getenv("WEATHER_GRID_SIZE", "1.0"))


def s3(path):
    return f"s3a://{BUCKET}/{path.strip('/')}"


def bronze(source):
    return s3(f"bronze/{source}/data")


def bronze_checkpoint(source):
    return s3(f"bronze/{source}/checkpoint")


def silver(name):
    return s3(f"silver/{name}")


def spark_session(app_name, extra_conf=None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")       
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    if S3_ENDPOINT:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{REGION}.amazonaws.com",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
