import os
from uuid import uuid4

import pytest


@pytest.mark.integration
def test_s3_write_read_round_trip():
    if os.getenv("RUN_S3_INTEGRATION") != "1":
        pytest.skip("Set RUN_S3_INTEGRATION=1 to run the S3 integration test")

    bucket = os.environ["S3_BUCKET"]

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("AIS-S3-Pytest")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{os.getenv('AWS_REGION', 'eu-north-1')}.amazonaws.com",
        )
        .getOrCreate()
    )
    output_path = (
        f"s3a://{bucket}/bronze/test/pytest-{uuid4().hex}"
    )

    try:
        expected = [(1, "pytest S3 connection test")]
        frame = spark.createDataFrame(expected, ["id", "message"])
        frame.write.mode("errorifexists").json(output_path)

        assert spark.read.json(output_path).collect()[0]["message"] == expected[0][1]
    finally:
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_path)
        filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        filesystem.delete(path, True)
        spark.stop()
