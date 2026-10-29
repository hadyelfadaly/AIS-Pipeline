import os

import requests
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import task, task_group

AWS_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"


@task(retries=0)
def check_env(required):

    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")


@task(retries=0)
def check_aws_identity():

    sts = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="sts").get_conn()
    identity = sts.get_caller_identity()

    print(f"AWS credentials OK, connected as {identity['Arn']}")


@task(retries=0)
def check_s3_roundtrip():

    bucket = os.getenv("BUCKET_NAME")
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    if not hook.check_for_bucket(bucket):
        raise RuntimeError(f"Bucket '{bucket}' not found or not accessible")

    key = "_preflight/healthcheck.txt"

    hook.load_string("ok", key=key, bucket_name=bucket, replace=True)

    if not hook.check_for_key(key, bucket_name=bucket):
        raise RuntimeError("Wrote a test object but could not read it back")
    
    hook.delete_objects(bucket=bucket, keys=[key])


@task(retries=2)
def check_source(service_url):

    r = requests.get(
        f"{service_url}/query",
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=30,
    )

    r.raise_for_status()

    body = r.json()

    if body.get("count", 0) <= 0:
        raise RuntimeError(f"Source returned no rows: {service_url}")


@task(retries=0)
def check_snowflake_identity():

    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
    )
    user, role, warehouse, database, schema = cursor.fetchone()

    print(
        f"Snowflake credentials OK, connected as {user} (Role: {role}, "
        f"Warehouse: {warehouse}, DB: {database}, Schema: {schema})"
    )

    cursor.close()
    conn.close()

@task_group(group_id="preflight")
def preflight(required_env, source_urls):
    
    check_env(required_env)
    check_aws_identity()
    check_s3_roundtrip()
    check_source.expand(service_url=source_urls)
    check_snowflake_identity()